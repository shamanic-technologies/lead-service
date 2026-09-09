import { describe, it, expect, vi, beforeEach } from "vitest";
import { organizations, leadsOrganizations } from "../../src/db/schema.js";

/**
 * A lead ingested from apollo-service (through human-service's people gateway)
 * must land with the organization facts and the FULL career history that
 * enrichment already paid for — not a slim org row and a single employment row.
 *
 * The mock is drizzle-faithful in the one way that matters here: `.where()` is
 * itself awaitable (the full-history read has no `.limit()`), so a handler that
 * forgets a terminal call cannot pass.
 */

const insertCalls: { table: unknown; values: unknown }[] = [];
const updateCalls: { table: unknown; set: unknown }[] = [];
const selectResults = new Map<unknown, unknown[]>();
const findFirstOrg = vi.fn();

function makeInsert(table: unknown) {
  return {
    values: (obj: unknown) => {
      insertCalls.push({ table, values: obj });
      const returningVal =
        table === organizations
          ? [{ id: `org-inserted-${insertCalls.length}` }]
          : [{ id: `link-inserted-${insertCalls.length}` }];
      const p = Promise.resolve(returningVal) as Promise<unknown[]> & {
        onConflictDoNothing: () => Promise<unknown[]>;
        returning: () => Promise<unknown[]>;
      };
      p.onConflictDoNothing = () => Promise.resolve(returningVal);
      p.returning = () => Promise.resolve(returningVal);
      return p;
    },
  };
}

function makeUpdate(table: unknown) {
  return {
    set: (obj: unknown) => {
      updateCalls.push({ table, set: obj });
      return { where: () => Promise.resolve(undefined) };
    },
  };
}

function makeSelect() {
  return {
    from: (table: unknown) => ({
      where: () => {
        const rows = selectResults.get(table) ?? [];
        const p = Promise.resolve(rows) as Promise<unknown[]> & {
          limit: () => Promise<unknown[]>;
        };
        p.limit = () => Promise.resolve(rows);
        return p;
      },
    }),
  };
}

vi.mock("../../src/db/index.js", () => ({
  db: {
    insert: (table: unknown) => makeInsert(table),
    update: (table: unknown) => makeUpdate(table),
    select: () => makeSelect(),
    query: {
      organizations: { findFirst: (...a: unknown[]) => findFirstOrg(...a) },
    },
  },
}));

const linkInserts = () =>
  insertCalls
    .filter((c) => c.table === leadsOrganizations)
    .map((c) => c.values as Record<string, unknown>);
const linkUpdates = () =>
  updateCalls
    .filter((c) => c.table === leadsOrganizations)
    .map((c) => c.set as Record<string, unknown>);
const orgUpdates = () =>
  updateCalls.filter((c) => c.table === organizations).map((c) => c.set as Record<string, unknown>);
const orgInserts = () =>
  insertCalls.filter((c) => c.table === organizations).map((c) => c.values as Record<string, unknown>);

beforeEach(() => {
  insertCalls.length = 0;
  updateCalls.length = 0;
  selectResults.clear();
  findFirstOrg.mockReset();
});

/** What apollo-service holds and human-service now carries, for one real person. */
const richPerson = {
  providerPersonId: "person-1",
  title: "Founder",
  organization: {
    name: "Casco Bay",
    domain: "cascobay.com",
    websiteUrl: "https://cascobay.com",
    industry: "marketing",
    estimatedNumEmployees: 12,
    annualRevenue: 1_500_000,
    linkedinUrl: "https://linkedin.com/company/cascobay",
    logoUrl: null,
    city: "Portland",
    state: "Maine",
    country: "United States",
    providerOrganizationId: "apollo-org-1",
    shortDescription: "Boutique digital marketing agency in Portland, ME.",
    seoDescription: "Casco Bay helps brands grow.",
    keywords: ["marketing", "branding"],
    technologyNames: ["GA4", "HubSpot"],
    industries: ["marketing & advertising"],
    secondaryIndustries: ["design"],
    latestFundingStage: "Series A",
    latestFundingRoundDate: "2024-03-01",
    totalFunding: 5_000_000,
    totalFundingPrinted: "$5M",
    fundingEvents: [{ id: "fe-1", type: "Series A", amount: 5_000_000 }],
    foundedYear: 2018,
    twitterUrl: "https://x.com/cascobay",
    streetAddress: "1 Main St",
    postalCode: "04101",
    primaryPhone: "+12075550100",
    numSuborganizations: 0,
    alexaRanking: 812_345,
  },
  employmentHistory: [
    {
      organizationName: "Casco Bay",
      title: "Founder",
      startDate: "2018-04-01",
      endDate: null,
      current: true,
      description: "Runs the shop.",
    },
    {
      organizationName: "Old Agency",
      title: "Head of Growth",
      startDate: "2014-01-01",
      endDate: "2018-03-01",
      current: false,
      description: null,
    },
  ],
};

describe("pickOrgFields — the organization facts apollo-service already paid for", () => {
  it("carries every widened field onto the organization row", async () => {
    const { pickOrgFields } = await import("../../src/lib/leads-registry.js");
    const fields = pickOrgFields(richPerson.organization as never);

    expect(fields).toMatchObject({
      name: "Casco Bay",
      primaryDomain: "cascobay.com",
      apolloOrganizationId: "apollo-org-1",
      shortDescription: "Boutique digital marketing agency in Portland, ME.",
      seoDescription: "Casco Bay helps brands grow.",
      keywords: ["marketing", "branding"],
      technologyNames: ["GA4", "HubSpot"],
      industries: ["marketing & advertising"],
      secondaryIndustries: ["design"],
      latestFundingStage: "Series A",
      latestFundingRoundDate: "2024-03-01",
      totalFunding: "5000000",
      totalFundingPrinted: "$5M",
      foundedYear: 2018,
      annualRevenue: "1500000",
      estimatedNumEmployees: 12,
      twitterUrl: "https://x.com/cascobay",
      streetAddress: "1 Main St",
      postalCode: "04101",
      primaryPhone: "+12075550100",
      alexaRanking: 812345,
    });
    expect(fields.fundingEvents).toEqual([{ id: "fe-1", type: "Series A", amount: 5_000_000 }]);
    // A stated zero is a value, not an absence.
    expect(fields.numSuborganizations).toBe(0);
  });

  it("writes nothing for a field the producer did not send", async () => {
    const { pickOrgFields } = await import("../../src/lib/leads-registry.js");
    const fields = pickOrgFields({
      name: "Slim Co",
      domain: "slim.co",
      websiteUrl: null,
      industry: null,
      estimatedNumEmployees: null,
      annualRevenue: null,
      linkedinUrl: null,
      logoUrl: null,
      city: null,
      state: null,
      country: null,
    } as never);

    expect(Object.keys(fields).sort()).toEqual(["name", "primaryDomain"]);
    expect("shortDescription" in fields).toBe(false);
    expect("keywords" in fields).toBe(false);
  });

  it("does not treat an empty array as a stated value", async () => {
    const { pickOrgFields } = await import("../../src/lib/leads-registry.js");
    const fields = pickOrgFields({
      name: "Empty Co",
      domain: "empty.co",
      websiteUrl: null,
      industry: null,
      estimatedNumEmployees: null,
      annualRevenue: null,
      linkedinUrl: null,
      logoUrl: null,
      city: null,
      state: null,
      country: null,
      keywords: [],
      technologyNames: [],
      fundingEvents: [],
    } as never);

    expect("keywords" in fields).toBe(false);
    expect("technologyNames" in fields).toBe(false);
    expect("fundingEvents" in fields).toBe(false);
  });
});

describe("recordEmploymentHistory — the person's whole career, not just today's job", () => {
  it("writes one row per role, current flagged exactly once", async () => {
    // Top-level org resolves by domain; the past employer is not known yet.
    findFirstOrg.mockImplementation(async () => undefined);
    selectResults.set(leadsOrganizations, []);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({ leadId: "lead-1", person: richPerson as never });

    const rows = linkInserts();
    expect(rows).toHaveLength(2);
    expect(rows.filter((r) => r.current === true)).toHaveLength(1);
    expect(rows).toContainEqual(
      expect.objectContaining({
        title: "Founder",
        startDate: "2018-04-01",
        endDate: null,
        current: true,
        description: "Runs the shop.",
      }),
    );
    expect(rows).toContainEqual(
      expect.objectContaining({
        title: "Head of Growth",
        startDate: "2014-01-01",
        endDate: "2018-03-01",
        current: false,
      }),
    );
  });

  it("mints an organization for a past employer known by name only", async () => {
    findFirstOrg.mockImplementation(async () => undefined);
    selectResults.set(leadsOrganizations, []);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({ leadId: "lead-1", person: richPerson as never });

    expect(orgInserts()).toContainEqual(expect.objectContaining({ name: "Old Agency" }));
  });

  it("adopts the legacy dateless row for the current employer instead of duplicating it", async () => {
    // The org row already exists by domain, and this lead already carries the
    // single dateless employment row the pre-history write path produced.
    findFirstOrg.mockResolvedValue({ id: "org-top" });
    selectResults.set(leadsOrganizations, [
      { id: "link-legacy", organizationId: "org-top", startDate: null },
    ]);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({ leadId: "lead-1", person: richPerson as never });

    // The legacy row is updated in place with the real dates…
    expect(linkUpdates()).toContainEqual(
      expect.objectContaining({ current: true, startDate: "2018-04-01", title: "Founder" }),
    );
    // …and only the PAST role is inserted.
    expect(linkInserts()).toHaveLength(1);
    expect(linkInserts()[0]).toMatchObject({ title: "Head of Growth", current: false });
  });

  it("re-running is idempotent — the same history writes no new rows", async () => {
    // Both employers already resolve to a known organization row, and this lead
    // already carries a row for each role at its real start date.
    findFirstOrg.mockResolvedValue({ id: "org-top" });
    selectResults.set(leadsOrganizations, [
      { id: "link-current", organizationId: "org-top", startDate: "2018-04-01" },
      { id: "link-past", organizationId: "org-top", startDate: "2014-01-01" },
    ]);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({ leadId: "lead-1", person: richPerson as never });

    expect(linkInserts()).toHaveLength(0);
  });

  it("a producer that serves no history still records exactly the current employer", async () => {
    findFirstOrg.mockResolvedValue({ id: "org-top" });
    selectResults.set(leadsOrganizations, []);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({
      leadId: "lead-1",
      person: { ...richPerson, employmentHistory: undefined } as never,
    });

    expect(linkInserts()).toHaveLength(1);
    expect(linkInserts()[0]).toMatchObject({ organizationId: "org-top", current: true });
  });

  it("records a role whose date the provider mangled, without a date", async () => {
    findFirstOrg.mockImplementation(async () => undefined);
    selectResults.set(leadsOrganizations, []);
    const { recordEmploymentHistory } = await import("../../src/lib/leads-registry.js");
    await recordEmploymentHistory({
      leadId: "lead-1",
      person: {
        ...richPerson,
        employmentHistory: [
          { organizationName: "Old Agency", title: "Analyst", startDate: "circa 2011", current: false },
        ],
      } as never,
    });

    const past = linkInserts().find((r) => r.title === "Analyst");
    expect(past).toBeDefined();
    expect(past?.startDate).toBeNull();
  });
});

describe("normalizeEmploymentDate", () => {
  it("keeps a calendar date and refuses anything else", async () => {
    const { normalizeEmploymentDate } = await import("../../src/lib/leads-registry.js");
    expect(normalizeEmploymentDate("2018-04-01")).toBe("2018-04-01");
    expect(normalizeEmploymentDate("2018-04-01T00:00:00Z")).toBe("2018-04-01");
    expect(normalizeEmploymentDate("circa 2011")).toBeNull();
    expect(normalizeEmploymentDate("")).toBeNull();
    expect(normalizeEmploymentDate(null)).toBeNull();
    expect(normalizeEmploymentDate(undefined)).toBeNull();
  });
});
