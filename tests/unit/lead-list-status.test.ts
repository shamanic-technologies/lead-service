import { describe, it, expect, vi } from "vitest";

// Same fragment-capturing mock as lead-list-dedup.test.ts: assert WHICH statuses the list
// queries filter on, and where the predicate lands, without a live DB.
let mockSqlCalls: Array<{ strings: readonly string[]; values: unknown[] }> = [];

vi.mock("../../src/db/index.js", () => ({
  sql: (strings: readonly string[], ...values: unknown[]) => {
    mockSqlCalls.push({ strings, values });
    return { __fragment: true };
  },
}));

const {
  leadCampaignBaseRelation,
  leadStatusScope,
  parseLeadStatusFilter,
  LEAD_LIFECYCLE_STATUSES,
  DEFAULT_LEAD_LIST_STATUSES,
} = await import("../../src/lib/lead-list-query.js");

const ORG = "30000000-0000-0000-0000-000000000001";
const BRAND = "20000000-0000-0000-0000-000000000001";

function textOf(): string {
  return mockSqlCalls.map((c) => c.strings.join(" ")).join(" ");
}

describe("parseLeadStatusFilter", () => {
  it("answers the ACTIONABLE population when the caller names no status — skipped excluded", () => {
    expect(parseLeadStatusFilter(undefined)).toEqual(["buffered", "claimed", "served"]);
    expect(DEFAULT_LEAD_LIST_STATUSES).not.toContain("skipped");
  });

  it("`all` means every lifecycle status", () => {
    expect(parseLeadStatusFilter("all")).toEqual(LEAD_LIFECYCLE_STATUSES);
    expect(parseLeadStatusFilter(" all ")).toEqual(LEAD_LIFECYCLE_STATUSES);
    expect(parseLeadStatusFilter("all")).toContain("skipped");
  });

  it("takes an explicit comma-separated list, trimmed and deduped", () => {
    expect(parseLeadStatusFilter("served")).toEqual(["served"]);
    expect(parseLeadStatusFilter("skipped")).toEqual(["skipped"]);
    expect(parseLeadStatusFilter(" served , buffered ")).toEqual(["served", "buffered"]);
    expect(parseLeadStatusFilter("served,served")).toEqual(["served"]);
  });

  it("fails loud on an unknown, empty or non-string value — never a silent fallback", () => {
    expect(() => parseLeadStatusFilter("bogus")).toThrow(/Unknown status value/);
    expect(() => parseLeadStatusFilter("served,bogus")).toThrow(/bogus/);
    expect(() => parseLeadStatusFilter("")).toThrow(/at least one/);
    expect(() => parseLeadStatusFilter(",")).toThrow(/at least one/);
    // A repeated query param arrives as an array — reject rather than guess which one wins.
    expect(() => parseLeadStatusFilter(["served", "skipped"])).toThrow(/single comma-separated/);
  });
});

describe("leadStatusScope", () => {
  it("filters on the actionable statuses by default", () => {
    expect(leadStatusScope({ orgId: ORG })).toEqual(["buffered", "claimed", "served"]);
  });

  it("is null — no predicate at all — when the scope names every status", () => {
    expect(leadStatusScope({ orgId: ORG, statuses: LEAD_LIFECYCLE_STATUSES })).toBeNull();
  });

  it("passes an explicit narrower list through", () => {
    expect(leadStatusScope({ orgId: ORG, statuses: ["served"] })).toEqual(["served"]);
    expect(leadStatusScope({ orgId: ORG, statuses: ["skipped"] })).toEqual(["skipped"]);
  });
});

describe("leadCampaignBaseRelation status predicate", () => {
  it("filters INSIDE the dedup subquery, so the winning membership is chosen among the asked-for statuses", () => {
    mockSqlCalls = [];
    leadCampaignBaseRelation({ orgId: ORG, brandId: BRAND });
    expect(textOf()).toContain("lc0.status = ANY(");
    expect(mockSqlCalls.flatMap((c) => c.values)).toContainEqual(["buffered", "claimed", "served"]);
  });

  it("emits no status predicate when every status was asked for", () => {
    mockSqlCalls = [];
    leadCampaignBaseRelation({ orgId: ORG, brandId: BRAND, statuses: LEAD_LIFECYCLE_STATUSES });
    expect(textOf()).not.toContain("lc0.status = ANY(");
  });
});
