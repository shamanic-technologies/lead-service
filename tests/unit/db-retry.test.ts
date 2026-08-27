import { describe, it, expect, vi } from "vitest";
import { isTransientConnectError, withConnectRetry, CONNECT_BACKOFF_MS } from "../../src/lib/db-retry.js";

/** Never actually sleep — the ladder is ~65s of real time. */
const noWait = () => Promise.resolve();

describe("isTransientConnectError", () => {
  it("recognises the Node-20 happy-eyeballs AggregateError a cold Neon compute produces", () => {
    const sub = Object.assign(new Error("connect ETIMEDOUT 10.0.0.1:5432"), { code: "ETIMEDOUT" });
    const agg = new AggregateError([sub], "");

    expect(isTransientConnectError(agg)).toBe(true);
  });

  it("recognises postgres.js CONNECT_TIMEOUT", () => {
    expect(isTransientConnectError(Object.assign(new Error("write CONNECT_TIMEOUT"), { code: "CONNECT_TIMEOUT" }))).toBe(true);
  });

  it("recognises a refusal while the compute is still resuming", () => {
    expect(isTransientConnectError(Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" }))).toBe(true);
  });

  it("recognises Neon's 57P03 cannot_connect_now", () => {
    expect(isTransientConnectError(Object.assign(new Error("the database system is starting up"), { code: "57P03" }))).toBe(true);
  });

  it("recognises the pool-acquire message that carries no code", () => {
    expect(isTransientConnectError(new Error("timeout exceeded when trying to connect"))).toBe(true);
  });

  it("walks a nested cause funnel", () => {
    const inner = Object.assign(new Error("socket"), { code: "ECONNRESET" });
    expect(isTransientConnectError(new Error("boom", { cause: new Error("mid", { cause: inner }) }))).toBe(true);
  });

  it("does NOT swallow a real migration error", () => {
    expect(isTransientConnectError(Object.assign(new Error('syntax error at or near "CREAT"'), { code: "42601" }))).toBe(false);
    expect(isTransientConnectError(new Error('relation "leads" already exists'))).toBe(false);
  });

  it("terminates on a self-referencing cause", () => {
    const err = new Error("loop") as Error & { cause?: unknown };
    err.cause = err;
    expect(isTransientConnectError(err)).toBe(false);
  });
});

describe("withConnectRetry", () => {
  it("returns the result when the first attempt succeeds", async () => {
    const fn = vi.fn().mockResolvedValue("migrated");

    await expect(withConnectRetry(fn, { wait: noWait })).resolves.toBe("migrated");
    expect(fn).toHaveBeenCalledOnce();
  });

  it("retries a cold-compute connection failure and succeeds once the compute wakes", async () => {
    const cold = Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" });
    const fn = vi.fn().mockRejectedValueOnce(cold).mockRejectedValueOnce(cold).mockResolvedValue("migrated");
    const onRetry = vi.fn();

    await expect(withConnectRetry(fn, { wait: noWait, onRetry })).resolves.toBe("migrated");
    expect(fn).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry.mock.calls[0][1]).toBe(CONNECT_BACKOFF_MS[0]);
  });

  it("does NOT retry a real migration failure — it surfaces on the first throw", async () => {
    const bad = Object.assign(new Error('syntax error at or near "CREAT"'), { code: "42601" });
    const fn = vi.fn().mockRejectedValue(bad);

    await expect(withConnectRetry(fn, { wait: noWait })).rejects.toBe(bad);
    expect(fn).toHaveBeenCalledOnce();
  });

  it("gives up after the budget and rejects with the original error", async () => {
    const cold = Object.assign(new Error("connect ETIMEDOUT"), { code: "ETIMEDOUT" });
    const fn = vi.fn().mockRejectedValue(cold);
    const backoffMs = [1, 1, 1];

    await expect(withConnectRetry(fn, { wait: noWait, backoffMs })).rejects.toBe(cold);
    expect(fn).toHaveBeenCalledTimes(backoffMs.length + 1);
  });

  it("waits the configured backoff between attempts", async () => {
    const cold = Object.assign(new Error("connect ETIMEDOUT"), { code: "ETIMEDOUT" });
    const fn = vi.fn().mockRejectedValueOnce(cold).mockResolvedValue("migrated");
    const waited: number[] = [];

    await withConnectRetry(fn, {
      wait: async (ms) => {
        waited.push(ms);
      },
    });

    expect(waited).toEqual([CONNECT_BACKOFF_MS[0]]);
  });
});
