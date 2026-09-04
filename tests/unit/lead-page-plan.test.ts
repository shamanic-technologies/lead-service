import { describe, it, expect } from "vitest";
import { parseLeadSort, planLeadPage } from "../../src/lib/lead-page-plan.js";
import { decodeLeadCursor, type LeadListPage } from "../../src/lib/lead-list-query.js";
import type { EnrichedLeadIndexRow } from "../../src/lib/lead-engagement.js";
import type { LeadBucket } from "../../src/lib/lead-buckets.js";

function row(i: number, buckets: LeadBucket[] = [], activityAt?: string): EnrichedLeadIndexRow {
  return {
    id: `lc-${String(i).padStart(3, "0")}`,
    leadId: `lead-${i}`,
    campaignId: "camp",
    brandIds: ["brand"],
    status: "served",
    email: `p${i}@example.test`,
    servedAt: null,
    // Postgres's own spelling, exactly as the index reads it back.
    createdAtText: `2026-01-01 00:00:00.${String(i).padStart(6, "0")}+00`,
    buckets: new Set(buckets),
    activityAt: activityAt ?? `2026-01-01T00:00:${String(i).padStart(2, "0")}.000Z`,
  };
}

const page = (over: Partial<LeadListPage> = {}): LeadListPage => ({
  limit: null,
  cursor: null,
  offset: null,
  ...over,
});

describe("the order a caller names", () => {
  it("defaults to the order this endpoint has always answered in", () => {
    expect(parseLeadSort(undefined)).toBe("created");
    expect(parseLeadSort("activity")).toBe("activity");
    expect(() => parseLeadSort("relevance")).toThrow(/Unknown sort/);
  });
});

describe("which rows a filtered read returns", () => {
  const rows = [
    row(0, ["contacted"]),
    row(1, ["contacted", "website_visit"]),
    row(2, []),
    row(3, ["contacted", "sale"]),
  ];

  it("totals what MATCHES the filter, not what the brand holds", () => {
    expect(planLeadPage(rows, "contacted", "created", page()).total).toBe(3);
    expect(planLeadPage(rows, "sale", "created", page()).total).toBe(1);
    expect(planLeadPage(rows, null, "created", page()).total).toBe(4);
  });

  it("returns only the bucket's own rows", () => {
    expect(planLeadPage(rows, "website_visit", "created", page()).ids).toEqual(["lc-001"]);
  });

  it("returns everything and no cursor when the caller names no bound", () => {
    const plan = planLeadPage(rows, null, "created", page());
    expect(plan.ids).toHaveLength(4);
    expect(plan.nextCursor).toBeNull();
  });

  it("orders newest-first on the timestamp that dates each lead when asked to", () => {
    const plan = planLeadPage(rows, null, "activity", page());
    expect(plan.ids).toEqual(["lc-003", "lc-002", "lc-001", "lc-000"]);
  });
});

describe("walking a filtered, re-ordered population", () => {
  const rows = Array.from({ length: 25 }, (_, i) => row(i, ["contacted"], `2026-01-01T00:00:${String(i).padStart(2, "0")}.000Z`));

  for (const sort of ["created", "activity"] as const) {
    it(`visits every row exactly once, no gaps and no repeats (${sort})`, () => {
      const seen: string[] = [];
      let cursor: string | null = null;
      for (let guard = 0; guard < 20; guard += 1) {
        const plan = planLeadPage(rows, "contacted", sort, page({
          limit: 7,
          cursor: cursor ? decodeLeadCursor(cursor) : null,
        }));
        expect(plan.total).toBe(25);
        seen.push(...plan.ids);
        cursor = plan.nextCursor;
        if (cursor === null) break;
      }
      expect(seen).toHaveLength(25);
      expect(new Set(seen).size).toBe(25);
    });
  }

  it("says the walk is over on the page that reaches the end", () => {
    const plan = planLeadPage(rows, null, "created", page({ limit: 100 }));
    expect(plan.ids).toHaveLength(25);
    expect(plan.nextCursor).toBeNull();
  });

  it("honours offset as the positional form of the same window", () => {
    const plan = planLeadPage(rows, null, "created", page({ limit: 3, offset: 10 }));
    expect(plan.ids).toEqual(["lc-010", "lc-011", "lc-012"]);
  });

  it("breaks a tie on the row id, so an order over equal timestamps is still total", () => {
    const tied = [
      row(1, [], "2026-05-05T00:00:00.000Z"),
      row(2, [], "2026-05-05T00:00:00.000Z"),
      row(3, [], "2026-05-05T00:00:00.000Z"),
    ];
    const first = planLeadPage(tied, null, "activity", page({ limit: 2 }));
    expect(first.ids).toEqual(["lc-003", "lc-002"]);
    const second = planLeadPage(tied, null, "activity", page({
      limit: 2,
      cursor: decodeLeadCursor(first.nextCursor!),
    }));
    expect(second.ids).toEqual(["lc-001"]);
  });
});
