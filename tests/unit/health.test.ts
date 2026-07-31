import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";

const dbExecute = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: {
    execute: (...args: unknown[]) => dbExecute(...args),
  },
  sql: {},
}));

/**
 * `vi.resetModules()` gives each case a fresh module registry, so the boot-state
 * singleton must be imported through the SAME registry as the route under test —
 * a static top-level import would bind to a different instance and silently stop
 * driving it.
 */
async function makeApp() {
  const bootState = await import("../../src/lib/boot-state.js");
  const { default: healthRoutes } = await import("../../src/routes/health.js");
  const app = express();
  app.use(healthRoutes);
  return { app, bootState };
}

describe("GET /health", () => {
  beforeEach(() => {
    dbExecute.mockReset();
    vi.resetModules();
  });

  it("returns 200 without touching the DB while migrations are still running", async () => {
    const { app } = await makeApp();

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "starting", service: "lead-service", migrations: "pending" });
    // Decisive: a cold Neon compute must not be able to fail the deploy healthcheck.
    expect(dbExecute).not.toHaveBeenCalled();
  });

  it("returns 503 when migrations failed, so the release is rejected loudly", async () => {
    const { app, bootState } = await makeApp();
    bootState.markBootFailed(new Error('relation "leads" already exists'));

    const res = await request(app).get("/health");

    expect(res.status).toBe(503);
    expect(res.body).toMatchObject({
      status: "unavailable",
      service: "lead-service",
      migrations: "failed",
      detail: 'relation "leads" already exists',
    });
    expect(dbExecute).not.toHaveBeenCalled();
  });

  it("returns 200 when ready and the DB ping succeeds", async () => {
    const { app, bootState } = await makeApp();
    bootState.markBootReady();
    dbExecute.mockResolvedValueOnce([{ "?column?": 1 }]);

    const res = await request(app).get("/health");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ status: "ok", service: "lead-service" });
    expect(dbExecute).toHaveBeenCalledOnce();
  });

  it("returns 503 when ready and the DB ping rejects", async () => {
    const { app, bootState } = await makeApp();
    bootState.markBootReady();
    dbExecute.mockRejectedValueOnce(new Error("connection refused"));

    const res = await request(app).get("/health");

    expect(res.status).toBe(503);
    expect(res.body).toEqual({ status: "unavailable", service: "lead-service" });
  });
});
