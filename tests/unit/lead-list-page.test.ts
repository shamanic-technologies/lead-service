import { describe, it, expect } from "vitest";

const {
  parseLeadLimit,
  parseLeadOffset,
  parseLeadListPage,
  encodeLeadCursor,
  decodeLeadCursor,
  UNBOUNDED_LEAD_PAGE,
} = await import("../../src/lib/lead-list-query.js");

describe("parseLeadLimit", () => {
  it("is UNBOUNDED when the caller names no limit — the read every caller had before bounds existed", () => {
    expect(parseLeadLimit(undefined)).toBeNull();
    expect(UNBOUNDED_LEAD_PAGE).toEqual({ limit: null, cursor: null, offset: null });
  });

  it("takes a positive integer", () => {
    expect(parseLeadLimit("50")).toBe(50);
    expect(parseLeadLimit(" 1 ")).toBe(1);
    expect(parseLeadLimit("57622")).toBe(57622);
  });

  it("imposes no ceiling — a staff console legitimately reads the whole population", () => {
    expect(parseLeadLimit("1000000")).toBe(1000000);
  });

  it("fails loud on anything that is not a positive integer — a bound is never silently dropped", () => {
    expect(() => parseLeadLimit("0")).toThrow(/positive integer/);
    expect(() => parseLeadLimit("-1")).toThrow(/positive integer/);
    expect(() => parseLeadLimit("abc")).toThrow(/positive integer/);
    expect(() => parseLeadLimit("1.5")).toThrow(/positive integer/);
    expect(() => parseLeadLimit("")).toThrow(/positive integer/);
    // A repeated query param arrives as an array — reject rather than guess which one wins.
    expect(() => parseLeadLimit(["1", "2"])).toThrow(/single positive integer/);
  });
});

describe("parseLeadOffset", () => {
  it("is absent by default and accepts zero", () => {
    expect(parseLeadOffset(undefined)).toBeNull();
    expect(parseLeadOffset("0")).toBe(0);
    expect(parseLeadOffset("500")).toBe(500);
  });

  it("fails loud on a negative or non-integer value", () => {
    expect(() => parseLeadOffset("-1")).toThrow(/non-negative integer/);
    expect(() => parseLeadOffset("x")).toThrow(/non-negative integer/);
    expect(() => parseLeadOffset(["1"])).toThrow(/single non-negative integer/);
  });
});

describe("lead list cursor", () => {
  it("round-trips a walk position", () => {
    const position = { createdAt: "2026-01-02 03:04:05.678901+00", id: "lc-42" };
    const encoded = encodeLeadCursor(position);
    expect(decodeLeadCursor(encoded)).toEqual(position);
  });

  it("keeps MICROseconds — a position round-tripped through a Date repeats rows", () => {
    // timestamptz holds microseconds, a JS Date holds milliseconds. Flooring the position makes
    // the next page re-read every row inside the dropped microseconds: a real walk of one 57k-row
    // brand came back with 115 repeats before the cursor carried text.
    const position = { createdAt: "2026-01-02 03:04:05.678901+00", id: "lc-42" };
    const decoded = decodeLeadCursor(encodeLeadCursor(position))!;
    expect(decoded.createdAt).toBe("2026-01-02 03:04:05.678901+00");
    expect(new Date(decoded.createdAt).toISOString()).not.toBe(decoded.createdAt);
  });

  it("is opaque — no caller can read a position out of it by hand", () => {
    const encoded = encodeLeadCursor({ createdAt: "2026-01-02 03:04:05.678901+00", id: "lc-42" });
    expect(encoded).not.toContain("lc-42");
    expect(encoded).not.toContain("2026");
  });

  it("fails loud on a position that is not a timestamp at all", () => {
    expect(() => encodeLeadCursor({ createdAt: "not-a-timestamp", id: "lc-1" })).toThrow(
      /invalid cursor created_at/,
    );
  });

  it("is absent when the caller names none", () => {
    expect(decodeLeadCursor(undefined)).toBeNull();
  });

  it("fails loud on a cursor this endpoint did not issue", () => {
    expect(() => decodeLeadCursor("not-a-cursor")).toThrow(/not a cursor/);
    expect(() => decodeLeadCursor("")).toThrow(/non-empty/);
    expect(() => decodeLeadCursor(Buffer.from('{"t":"nope","i":"x"}').toString("base64url"))).toThrow(
      /not a cursor/,
    );
    expect(() => decodeLeadCursor(Buffer.from('{"i":"x"}').toString("base64url"))).toThrow(/not a cursor/);
  });
});

describe("parseLeadListPage", () => {
  it("reads limit, cursor and offset together", () => {
    const cursor = encodeLeadCursor({ createdAt: "2026-01-01 00:00:00+00", id: "lc-1" });
    expect(parseLeadListPage({ limit: "50", cursor })).toEqual({
      limit: 50,
      cursor: { createdAt: "2026-01-01 00:00:00+00", id: "lc-1" },
      offset: null,
    });
    expect(parseLeadListPage({ limit: "50", offset: "100" })).toEqual({
      limit: 50,
      cursor: null,
      offset: 100,
    });
    expect(parseLeadListPage({})).toEqual(UNBOUNDED_LEAD_PAGE);
  });

  it("refuses two start positions at once rather than silently picking one", () => {
    const cursor = encodeLeadCursor({ createdAt: "2026-01-01 00:00:00+00", id: "lc-1" });
    expect(() => parseLeadListPage({ cursor, offset: "10" })).toThrow(/one, not both/);
  });
});
