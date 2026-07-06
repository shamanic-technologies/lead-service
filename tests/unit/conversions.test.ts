import { describe, it, expect, vi, beforeEach } from "vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

const execute = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: { execute: (...args: unknown[]) => execute(...args) },
}));

const {
  normalizeEmail,
  normalizePhone,
  deriveDomain,
  computeDedupeSignature,
  resolveAttributionStatus,
  isConversionEvent,
  generateConversionToken,
  matchConversion,
} = await import("../../src/lib/conversions.js");

const dialect = new PgDialect();
function compile(call: unknown): { sql: string; params: unknown[] } {
  return dialect.sqlToQuery(call as SQL);
}

describe("normalizeEmail", () => {
  it("lowercases and trims", () => {
    expect(normalizeEmail("  Jane@Acme.COM ")).toBe("jane@acme.com");
  });
  it("returns null for empty/nullish", () => {
    expect(normalizeEmail(undefined)).toBeNull();
    expect(normalizeEmail("   ")).toBeNull();
  });
});

describe("normalizePhone", () => {
  it("strips all non-digits", () => {
    expect(normalizePhone("+1 (415) 555-0142")).toBe("14155550142");
  });
  it("returns null when no digits", () => {
    expect(normalizePhone("n/a")).toBeNull();
    expect(normalizePhone(null)).toBeNull();
  });
});

describe("deriveDomain", () => {
  it("strips protocol, path, and www", () => {
    expect(deriveDomain("https://www.Acme.com/careers?x=1")).toBe("acme.com");
  });
  it("handles bare domains without protocol", () => {
    expect(deriveDomain("acme.com")).toBe("acme.com");
  });
  it("returns null for junk", () => {
    expect(deriveDomain("")).toBeNull();
    expect(deriveDomain(undefined)).toBeNull();
  });
});

describe("computeDedupeSignature", () => {
  const now = new Date("2026-07-05T12:34:56.000Z");
  it("uses dedupeKey verbatim when provided", () => {
    expect(computeDedupeSignature({ dedupeKey: "abc", event: "signup", now })).toBe("k:abc");
  });
  it("falls back to (event, email, day) when no dedupeKey", () => {
    expect(
      computeDedupeSignature({ event: "signup", email: "Jane@Acme.com", now }),
    ).toBe("a:signup:jane@acme.com:2026-07-05");
  });
  it("uses normalized phone when no email", () => {
    expect(
      computeDedupeSignature({ event: "meeting_booked", phone: "+1 415 555 0142", now }),
    ).toBe("a:meeting_booked:14155550142:2026-07-05");
  });
  it("returns null when there is no dedupe basis", () => {
    expect(computeDedupeSignature({ event: "signup", now })).toBeNull();
  });
});

describe("resolveAttributionStatus", () => {
  it("deterministic always attributes (even with ties)", () => {
    expect(resolveAttributionStatus("deterministic", 1)).toBe("attributed");
    expect(resolveAttributionStatus("deterministic", 3)).toBe("attributed");
  });
  it("strong attributes only when single candidate", () => {
    expect(resolveAttributionStatus("strong", 1)).toBe("attributed");
    expect(resolveAttributionStatus("strong", 2)).toBe("needs_review");
  });
  it("probabilistic auto-attributes to the top candidate (name is enough)", () => {
    expect(resolveAttributionStatus("probabilistic", 1)).toBe("attributed");
    expect(resolveAttributionStatus("probabilistic", 9)).toBe("attributed");
  });
  it("unmatched stays unmatched", () => {
    expect(resolveAttributionStatus("unmatched", 0)).toBe("unmatched");
  });
});

describe("isConversionEvent", () => {
  it("accepts the two valid events", () => {
    expect(isConversionEvent("signup")).toBe(true);
    expect(isConversionEvent("meeting_booked")).toBe(true);
  });
  it("rejects everything else", () => {
    expect(isConversionEvent("purchase")).toBe(false);
    expect(isConversionEvent(undefined)).toBe(false);
  });
});

describe("generateConversionToken", () => {
  it("produces a publishable pk_conv_ key, unique per call", () => {
    const a = generateConversionToken();
    const b = generateConversionToken();
    expect(a).toMatch(/^pk_conv_[A-Za-z0-9_-]+$/);
    expect(a).not.toBe(b);
  });
});

describe("matchConversion waterfall", () => {
  beforeEach(() => execute.mockReset());

  it("email exact → deterministic/attributed, candidate set is served-for-brand", async () => {
    execute.mockResolvedValueOnce([{ lead_id: "lead-1" }]);
    const result = await matchConversion({ brandId: "brand-1", email: "Jane@Acme.com" });
    expect(result).toEqual({
      matchedLeadId: "lead-1",
      matchMethod: "email",
      matchConfidence: "deterministic",
      attributionStatus: "attributed",
      candidateCount: 1,
    });
    expect(execute).toHaveBeenCalledOnce();
    const { sql, params } = compile(execute.mock.calls[0][0]);
    const lower = sql.toLowerCase();
    expect(lower).toContain("lc.status = 'served'");
    expect(lower).toContain("channel = 'email'");
    expect(params).toContain("jane@acme.com");
    expect(params).toContain("brand-1");
  });

  it("last_name only → probabilistic/attributed to top candidate", async () => {
    // no email/phone/domain enabled → first enabled tier is full_name (needs first+last),
    // here only lastName given so full_name is disabled; last_name tier runs.
    execute.mockResolvedValueOnce([{ lead_id: "lead-2" }, { lead_id: "lead-3" }]);
    const result = await matchConversion({ brandId: "brand-1", lastName: "Doe" });
    expect(result.matchMethod).toBe("last_name");
    expect(result.matchConfidence).toBe("probabilistic");
    expect(result.attributionStatus).toBe("attributed");
    expect(result.matchedLeadId).toBe("lead-2"); // top = most-engaged candidate
    expect(result.candidateCount).toBe(2);
    expect(execute).toHaveBeenCalledOnce();
  });

  it("stops at the first non-empty tier (phone beats name)", async () => {
    // email disabled (none), phone enabled → first call returns a hit; name tiers never run.
    execute.mockResolvedValueOnce([{ lead_id: "lead-9" }]);
    const result = await matchConversion({
      brandId: "b",
      phone: "+1 415 555 0142",
      firstName: "Jane",
      lastName: "Doe",
    });
    expect(result.matchMethod).toBe("phone");
    expect(result.matchConfidence).toBe("deterministic");
    expect(execute).toHaveBeenCalledOnce();
  });

  it("domain+lastName → strong/attributed on single candidate, needs_review on ties", async () => {
    execute.mockResolvedValueOnce([{ lead_id: "lead-1" }]);
    const single = await matchConversion({
      brandId: "b",
      companyUrl: "https://acme.com",
      lastName: "Doe",
    });
    expect(single.matchMethod).toBe("domain_name");
    expect(single.matchConfidence).toBe("strong");
    expect(single.attributionStatus).toBe("attributed");

    execute.mockResolvedValueOnce([{ lead_id: "lead-1" }, { lead_id: "lead-2" }]);
    const tie = await matchConversion({
      brandId: "b",
      companyUrl: "https://acme.com",
      lastName: "Doe",
    });
    expect(tie.attributionStatus).toBe("needs_review");
    expect(tie.candidateCount).toBe(2);
  });

  it("no identity yields → unmatched, no DB call", async () => {
    const result = await matchConversion({ brandId: "b" });
    expect(result).toEqual({
      matchedLeadId: null,
      matchMethod: null,
      matchConfidence: "unmatched",
      attributionStatus: "unmatched",
      candidateCount: 0,
    });
    expect(execute).not.toHaveBeenCalled();
  });

  it("falls through empty tiers to unmatched", async () => {
    // email + full name given: tier1 email empty, then full_name empty, then last_name empty.
    execute.mockResolvedValue([]);
    const result = await matchConversion({
      brandId: "b",
      email: "no@one.com",
      firstName: "No",
      lastName: "One",
    });
    expect(result.matchConfidence).toBe("unmatched");
    // email(1) + full_name(1) + last_name(1) = 3 tiers evaluated.
    expect(execute).toHaveBeenCalledTimes(3);
  });
});
