import { describe, it, expect, vi, beforeEach } from "vitest";

// pgSql mock — must be hoisted since vi.mock factory runs before variable declarations
const { pgSqlMock } = vi.hoisted(() => ({
  pgSqlMock: vi.fn().mockResolvedValue([]),
}));

// Mock db
vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
      leadBuffer: {
        findFirst: vi.fn(),
      },
      enrichments: {
        findFirst: vi.fn(),
      },
      cursors: {
        findFirst: vi.fn(),
      },
    },
    insert: vi.fn(),
    update: vi.fn(),
  },
  sql: pgSqlMock,
}));

// Mock apollo-client
vi.mock("../../src/lib/apollo-client.js", () => ({
  apolloSearchNext: vi.fn(),
  apolloSearchParams: vi.fn(),
  apolloEnrich: vi.fn(),
  apolloMatch: vi.fn(),
}));

// Mock outlet-client
vi.mock("../../src/lib/outlet-client.js", () => ({
  fetchOutletsByCampaign: vi.fn().mockResolvedValue(null),
  fetchNextOutlet: vi.fn().mockResolvedValue({ found: false }),
}));

// Mock journalist-client
vi.mock("../../src/lib/journalist-client.js", () => ({
  fetchNextJournalist: vi.fn().mockResolvedValue({ found: false }),
}));

// Mock campaign-client
vi.mock("../../src/lib/campaign-client.js", () => ({
  fetchCampaign: vi.fn().mockResolvedValue(null),
}));

// Mock brand-client
vi.mock("../../src/lib/brand-client.js", () => ({
  extractBrandFields: vi.fn().mockResolvedValue(null),
}));

// Mock email-gateway-client
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: vi.fn().mockResolvedValue({ results: [] }),
  isContacted: vi.fn().mockReturnValue(false),
}));

// Mock leads-registry
vi.mock("../../src/lib/leads-registry.js", () => ({
  resolveOrCreateLead: vi.fn().mockResolvedValue({ leadId: "lead-uuid-1", isNew: true }),
  findLeadByApolloPersonId: vi.fn().mockResolvedValue(null),
  findLeadByEmail: vi.fn().mockResolvedValue(null),
}));

import { db } from "../../src/db/index.js";
import { pullNext } from "../../src/lib/buffer.js";
import { apolloSearchNext, apolloSearchParams, apolloEnrich, apolloMatch } from "../../src/lib/apollo-client.js";
import { checkDeliveryStatus } from "../../src/lib/email-gateway-client.js";
import { resolveOrCreateLead } from "../../src/lib/leads-registry.js";
import { fetchOutletsByCampaign, fetchNextOutlet } from "../../src/lib/outlet-client.js";
import { fetchCampaign } from "../../src/lib/campaign-client.js";
import { extractBrandFields } from "../../src/lib/brand-client.js";
import { fetchNextJournalist } from "../../src/lib/journalist-client.js";

/** Helper: convert camelCase buffer row to snake_case raw SQL row (as returned by pgSql) */
function toClaimedRow(row: {
  id: string;
  namespace: string;
  campaignId: string;
  email: string;
  externalId: string | null;
  data: unknown;
  status?: string;
  pushRunId?: string | null;
  brandId: string;
  orgId: string;
  userId: string | null;
  createdAt?: Date;
}) {
  return {
    id: row.id,
    namespace: row.namespace,
    campaign_id: row.campaignId,
    email: row.email,
    external_id: row.externalId,
    data: row.data,
    status: "claimed",
    push_run_id: row.pushRunId ?? null,
    brand_id: row.brandId,
    org_id: row.orgId,
    user_id: row.userId,
    created_at: row.createdAt ?? new Date(),
  };
}

describe("buffer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset default mocks
    pgSqlMock.mockResolvedValue([]);
    vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });
    vi.mocked(resolveOrCreateLead).mockResolvedValue({ leadId: "lead-uuid-1", isNew: true });
  });

  describe("pullNext", () => {
    it("returns found: false when buffer is empty and Apollo search returns nothing", async () => {
      pgSqlMock.mockResolvedValue([]);

      // fillBufferFromSearch will call apolloSearchParams then apolloSearchNext
      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["CEO"] },
        totalResults: 0,
        attempts: 1,
      });
      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [],
        done: true,
        totalEntries: 0,
      });

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(false);
      // Should have attempted to fill even without searchParams
      expect(apolloSearchParams).toHaveBeenCalled();
      expect(apolloSearchNext).toHaveBeenCalled();
    });

    it("fills buffer from Apollo search even when searchParams is omitted (regression)", async () => {
      // First call: buffer empty → triggers fill
      // Second call: buffer has the lead
      pgSqlMock
        .mockResolvedValueOnce([])  // 1st pullNext: buffer empty
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "hire@example.com",
          externalId: "apollo-1",
          data: { firstName: "Jane", email: "hire@example.com" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })]);

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["Software Engineer"] },
        totalResults: 10,
        attempts: 1,
      });
      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-1", email: "hire@example.com", firstName: "Jane" }],
        done: false,
        totalEntries: 10,
      });

      // isInBuffer → false
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue(undefined as never);

      // db.insert for buffer row
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: vi.fn().mockReturnValue({ returning: vi.fn().mockResolvedValue([{ id: "served-1" }]) }) });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // checkDeliveryStatus for batch dedup
      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      // resolveOrCreateLead
      vi.mocked(resolveOrCreateLead).mockResolvedValue({ leadId: "lead-hire-1", isNew: true });

      // db.update for status changes
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      // Call WITHOUT searchParams — this should still trigger Apollo fill
      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        // No searchParams!
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("hire@example.com");
      expect(apolloSearchParams).toHaveBeenCalled();
    });

    it("returns a lead with leadId and marks it served", async () => {
      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        externalId: "e-1",
        data: { name: "Alice" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      // email-gateway: not delivered
      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      // resolveOrCreateLead returns leadId
      vi.mocked(resolveOrCreateLead).mockResolvedValue({ leadId: "lead-abc", isNew: true });

      // markServed insert
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "child-run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.leadId).toBe("lead-abc");
      expect(result.lead?.email).toBe("alice@acme.com");
      expect(result.lead?.externalId).toBe("e-1");
      expect(result.lead?.data).toEqual({ name: "Alice", email: "alice@acme.com" });
    });

    it("passes lead data through as-is without modification", async () => {
      const apolloData = {
        firstName: "Svitlana",
        organizationName: "HashtagWeb3",
        organizationDomain: "hashtagweb3.com",
        organizationIndustry: "information technology & services",
      };

      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "svitlana@hashtagweb3.com",
        externalId: "e-1",
        data: apolloData,
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      // data includes all original fields PLUS email is always synced
      expect(result.lead?.data).toEqual({ ...apolloData, email: "svitlana@hashtagweb3.com" });
    });

    it("skips already-delivered buffer rows and tries next", async () => {
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "alice@acme.com",
          externalId: "e-1",
          data: { name: "Alice" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-2",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "bob@acme.com",
          externalId: "e-2",
          data: { name: "Bob" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })]);

      // First lead: delivered, second lead: not delivered
      vi.mocked(checkDeliveryStatus)
        .mockResolvedValueOnce({
          results: [{ email: "alice@acme.com", broadcast: {
            campaign: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
            },
            global: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
            },
          }}],
        })
        .mockResolvedValueOnce({ results: [] });

      const { isContacted } = await import("../../src/lib/email-gateway-client.js");
      vi.mocked(isContacted)
        .mockReturnValueOnce(true)   // alice: delivered
        .mockReturnValueOnce(false); // bob: not delivered (no results)

      vi.mocked(resolveOrCreateLead)
        .mockResolvedValueOnce({ leadId: "lead-alice", isNew: false })
        .mockResolvedValueOnce({ leadId: "lead-bob", isNew: true });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-2" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("bob@acme.com");
      expect(result.lead?.leadId).toBe("lead-bob");
    });

    it("fills buffer from apolloSearchNext when buffer empty and searchParams provided", async () => {
      const newLeadRow = toClaimedRow({
        id: "buf-new",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "new-lead@example.com",
        externalId: "apollo-1",
        data: { firstName: "New" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])          // pullNext: buffer empty (claim returns nothing)
        .mockResolvedValueOnce([newLeadRow]); // pullNext retry: claimed new lead

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined);   // isInBuffer → not in buffer

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-1", email: "new-lead@example.com", firstName: "New" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({
        onConflictDoNothing: onConflictMock,
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",

      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("new-lead@example.com");
      expect(result.lead?.leadId).toBeDefined();
      expect(vi.mocked(apolloSearchParams)).toHaveBeenCalledOnce();
    });

    it("injects campaign featureInputs from campaign-service into LLM context", async () => {
      const newLeadRow = toClaimedRow({
        id: "buf-ctx-search",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "ctx-lead@example.com",
        externalId: "apollo-ctx",
        data: { firstName: "Ctx" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([newLeadRow]);

      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValueOnce(undefined);

      // Campaign-service returns featureInputs
      vi.mocked(fetchCampaign).mockResolvedValueOnce({
        id: "campaign-1",
        name: "PR Outreach",
        targetAudience: null,
        targetOutcome: null,
        valueForTarget: null,
        featureInputs: {
          companyContext: "AI-powered PR distribution",
          prAngle: "Launch of new journalist outreach feature",
          targetOutlets: ["TechCrunch", "The Verge"],
        },
      });

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["Head of PR"] }, totalResults: 50, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-ctx", email: "ctx-lead@example.com", firstName: "Ctx" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-ctx" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({ where: vi.fn().mockResolvedValue(undefined) });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);

      // Verify the LLM context includes featureInputs fetched from campaign-service
      const contextArg = vi.mocked(apolloSearchParams).mock.calls[0][0].context;
      expect(contextArg).toContain("AI-powered PR distribution");
      expect(contextArg).toContain("Launch of new journalist outreach feature");
      expect(contextArg).toContain("TechCrunch");
    });

    it("fetches campaign featureInputs and brand extract-fields for LLM context", async () => {
      const newLeadRow = toClaimedRow({
        id: "buf-conv",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "conv@example.com",
        externalId: "apollo-conv",
        data: { firstName: "Conv" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([newLeadRow]);

      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValueOnce(undefined);

      // Campaign returns featureInputs
      vi.mocked(fetchCampaign).mockResolvedValueOnce({
        id: "campaign-1",
        name: "Sustainability Launch",
        targetAudience: "Tech journalists",
        targetOutcome: "Press coverage",
        valueForTarget: "Exclusive story",
        featureInputs: {
          editorialAngle: "sustainability in AI",
          targetRegion: "North America",
        },
      });

      // Brand extract-fields returns extracted values
      vi.mocked(extractBrandFields).mockResolvedValueOnce([
        { key: "brand_name", value: "GreenTech Co", cached: true, extractedAt: "2026-01-01", expiresAt: null, sourceUrls: null },
        { key: "industry", value: "Clean Technology", cached: true, extractedAt: "2026-01-01", expiresAt: null, sourceUrls: null },
        { key: "target_job_titles", value: ["Editor", "Reporter"], cached: false, extractedAt: "2026-01-01", expiresAt: null, sourceUrls: null },
      ]);

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["Editor"] }, totalResults: 50, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-conv", email: "conv@example.com", firstName: "Conv" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-conv" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({ where: vi.fn().mockResolvedValue(undefined) });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);

      // Verify campaign featureInputs injected
      const contextArg = vi.mocked(apolloSearchParams).mock.calls[0][0].context;
      expect(contextArg).toContain("sustainability in AI");
      expect(contextArg).toContain("North America");
      expect(contextArg).toContain("Campaign context:");

      // Verify brand extract-fields injected
      expect(contextArg).toContain("GreenTech Co");
      expect(contextArg).toContain("Clean Technology");
      expect(contextArg).toContain("Editor");

      // Verify campaign fields still included
      expect(contextArg).toContain("Tech journalists");
      expect(contextArg).toContain("Press coverage");

      // Verify extractBrandFields was called with the right field descriptors
      expect(vi.mocked(extractBrandFields)).toHaveBeenCalledWith(
        "brand-1",
        expect.arrayContaining([
          expect.objectContaining({ key: "industry" }),
          expect.objectContaining({ key: "target_job_titles" }),
        ]),
        "org-1",
        expect.any(Object),
      );
    });

    it("merges enrichment cache data into buffer when filling from search", async () => {
      // Scenario: Apollo search returns sparse person data (no lastName).
      // Enrichment cache has full data (with lastName). fillBufferFromSearch should
      // merge the enriched data so pullNext returns complete lead.data.
      const enrichedLeadRow = toClaimedRow({
        id: "buf-enriched",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "briannah@example.com",
        externalId: "apollo-enr-1",
        data: {
          id: "apollo-enr-1",
          firstName: "Briannah",
          lastName: "Drew",
          email: "briannah@example.com",
          organizationName: "Braven",
          title: "Managing Director",
        },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])              // pullNext: buffer empty
        .mockResolvedValueOnce([enrichedLeadRow]); // pullNext retry: merged lead

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined);     // isInBuffer → not found

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["Director"] }, totalResults: 50, attempts: 1 });

      // Apollo search returns sparse data — no lastName
      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-enr-1", email: null, firstName: "Briannah", title: "Managing Director", organizationName: "Braven" }],
        done: true,
        totalEntries: 1,
      });

      // Enrichment cache has full data with lastName
      vi.mocked(db.query.enrichments.findFirst).mockResolvedValueOnce({
        id: "e-1", email: "briannah@example.com", apolloPersonId: "apollo-enr-1",
        firstName: "Briannah", lastName: "Drew", title: "Managing Director",
        linkedinUrl: null, organizationName: "Braven", organizationDomain: "bebraven.org",
        organizationIndustry: "Education", organizationSize: "51-200",
        responseRaw: {
          id: "apollo-enr-1", email: "briannah@example.com",
          firstName: "Briannah", lastName: "Drew", title: "Managing Director",
          organizationName: "Braven", organizationDomain: "bebraven.org",
        },
        enrichedAt: new Date(),
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const insertCalls: unknown[] = [];
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockImplementation((vals: unknown) => {
        insertCalls.push(vals);
        return { onConflictDoNothing: onConflictMock };
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",

      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("briannah@example.com");

      // Verify the buffer insert included merged data with lastName
      const bufferInsert = insertCalls.find(
        (c: any) => c.email === "briannah@example.com" && c.status === "buffered"
      ) as any;
      expect(bufferInsert).toBeDefined();
      expect(bufferInsert.data.lastName).toBe("Drew");
      expect(bufferInsert.data.organizationDomain).toBe("bebraven.org");
    });

    it("returns found: false when Apollo returns done: true with 0 people", async () => {
      pgSqlMock.mockResolvedValue([]);

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [],
        done: true,
        totalEntries: 0,
      });

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",

      });

      expect(result.found).toBe(false);
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalled();
    });

    it("uses cached enrichment instead of calling apolloEnrich", async () => {
      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "apollo-person-1",
        data: { firstName: "Ray" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      vi.mocked(db.query.enrichments.findFirst).mockResolvedValue({
        id: "enrich-1",
        email: "ray@provaliant.com",
        apolloPersonId: "apollo-person-1",
        firstName: "Ray",
        lastName: "Smith",
        title: "Program Director",
        linkedinUrl: "http://linkedin.com/in/ray-smith",
        organizationName: "Provaliant",
        organizationDomain: "provaliant.com",
        organizationIndustry: "IT",
        organizationSize: "27",
        responseRaw: { firstName: "Ray", lastName: "Smith", email: "ray@provaliant.com", title: "Program Director" },
        enrichedAt: new Date(),
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("ray@provaliant.com");
      expect(result.lead?.leadId).toBeDefined();
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("skips enrichment when cache has no-email entry for person", async () => {
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "known-no-email",
          data: { firstName: "Ghost" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-2",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "bob@acme.com",
          externalId: "e-2",
          data: { name: "Bob" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })]);

      vi.mocked(db.query.enrichments.findFirst).mockResolvedValueOnce({
        id: "e-1", email: null, apolloPersonId: "known-no-email",
        firstName: "Ghost", lastName: null, title: null, linkedinUrl: null,
        organizationName: null, organizationDomain: null,
        organizationIndustry: null, organizationSize: null,
        responseRaw: null, enrichedAt: new Date(),
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("bob@acme.com");
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("passes workflowSlug to apolloSearchParams, apolloSearchNext, and apolloEnrich", async () => {
      const newLeadRow = toClaimedRow({
        id: "buf-wf",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "apollo-wf-1",
        data: { firstName: "Workflow" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])          // pullNext: buffer empty
        .mockResolvedValueOnce([newLeadRow]); // pullNext retry: claimed new lead

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined);    // isInBuffer → not in buffer

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-wf-1", firstName: "Workflow" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(db.query.enrichments.findFirst).mockResolvedValue(undefined);

      vi.mocked(apolloEnrich).mockResolvedValue({
        person: { id: "apollo-wf-1", email: "wf@acme.com", firstName: "Workflow" },
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",

        workflowSlug: "cold-email-outreach",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("wf@acme.com");

      expect(vi.mocked(apolloSearchParams)).toHaveBeenCalledWith(
        expect.objectContaining({ workflowSlug: "cold-email-outreach" })
      );
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalledWith(
        expect.objectContaining({ workflowSlug: "cold-email-outreach" })
      );
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledWith(
        "apollo-wf-1",
        expect.objectContaining({ workflowSlug: "cold-email-outreach" })
      );
    });

    it("always includes email in data even when data.email is null (DAG reads data.email)", async () => {
      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "torian@theorion.com",
        externalId: "e-1",
        data: { firstName: "Torian", email: null, title: "Director" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("torian@theorion.com");
      // Critical: data.email must match lead.email, never null
      const data = result.lead?.data as Record<string, unknown>;
      expect(data.email).toBe("torian@theorion.com");
      expect(data.firstName).toBe("Torian");
      expect(data.title).toBe("Director");
    });

    it("skips buffer rows with no email and no externalId (never returns found: true with empty email)", async () => {
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-no-email",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: null,
          data: { name: "Ghost" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([]); // no more rows

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(false);
      // Verify it was marked as skipped
      expect(db.update).toHaveBeenCalled();
    });

    it("skips lead with no email after failed enrichment and serves next lead", async () => {
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-no-email",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "apollo-no-email",
          data: { name: "NoEmail" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-good",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "good@acme.com",
          externalId: "e-good",
          data: { name: "Good" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })]);

      // Enrichment returns no email
      vi.mocked(db.query.enrichments.findFirst).mockResolvedValueOnce(undefined);
      vi.mocked(apolloEnrich).mockResolvedValueOnce({
        person: { id: "apollo-no-email", firstName: "NoEmail", email: null },
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("good@acme.com");
    });

    it("continues when email-gateway is unreachable (fallback)", async () => {
      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        externalId: "e-1",
        data: { name: "Alice" },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      // email-gateway is unreachable
      vi.mocked(checkDeliveryStatus).mockResolvedValue(null);

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      // Should still serve the lead (fallback: not delivered)
      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("alice@acme.com");
    });

    it("fills buffer from journalists when sourceType=journalist and buffer empty", async () => {
      // First pullNext: buffer empty → fills from journalists
      // Second pullNext: serves the buffered journalist lead
      const journalistRow = toClaimedRow({
        id: "buf-j1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "jane@techcrunch.com",
        externalId: "journalist:j-uuid-1",
        data: {
          firstName: "Jane",
          lastName: "Reporter",
          organizationDomain: "techcrunch.com",
          organizationName: "TechCrunch",
          sourceType: "journalist",
        },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])             // pullNext: buffer empty
        .mockResolvedValueOnce([journalistRow]); // pullNext retry: claimed journalist lead

      // Cursor: no existing cursor
      vi.mocked(db.query.cursors.findFirst).mockResolvedValueOnce(undefined);

      // isInBuffer → not in buffer
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValueOnce(undefined);

      // Outlet service returns outlets
      vi.mocked(fetchOutletsByCampaign).mockResolvedValueOnce([
        { id: "outlet-1", outletName: "TechCrunch", outletUrl: "https://techcrunch.com", outletDomain: "techcrunch.com", relevanceScore: 85, outletStatus: "open", campaignId: "campaign-1" },
      ]);

      // Journalist service returns journalist via buffer/next, then exhausted
      vi.mocked(fetchNextJournalist)
        .mockResolvedValueOnce({
          found: true,
          journalist: {
            id: "j-uuid-1",
            journalistName: "Jane Reporter",
            firstName: "Jane",
            lastName: "Reporter",
            entityType: "individual" as const,
            relevanceScore: 0.85,
            whyRelevant: "Covers tech",
            whyNotRelevant: "",
            emails: [{ email: "jane@techcrunch.com", isValid: true, confidence: 0.95 }],
          },
        })
        .mockResolvedValueOnce({ found: false });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        sourceType: "journalist",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("jane@techcrunch.com");
      expect(vi.mocked(fetchOutletsByCampaign)).toHaveBeenCalledWith(
        "campaign-1", "org-1", expect.objectContaining({ campaignId: "campaign-1" })
      );
      expect(vi.mocked(fetchNextJournalist)).toHaveBeenCalledWith(
        "outlet-1", expect.objectContaining({ campaignId: "campaign-1" })
      );
    });

    it("uses apolloMatch for journalist leads without email", async () => {
      pgSqlMock.mockResolvedValue([toClaimedRow({
        id: "buf-j-noemail",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "journalist:j-uuid-2",
        data: {
          firstName: "John",
          lastName: "Writer",
          organizationDomain: "theverge.com",
          organizationName: "The Verge",
          sourceType: "journalist",
        },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      })]);

      vi.mocked(apolloMatch).mockResolvedValueOnce({
        enrichmentId: "enr-1",
        person: {
          id: "apollo-matched-1",
          email: "john.writer@theverge.com",
          firstName: "John",
          lastName: "Writer",
          organizationName: "The Verge",
          organizationDomain: "theverge.com",
        },
        cached: false,
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("john.writer@theverge.com");
      expect(vi.mocked(apolloMatch)).toHaveBeenCalledWith(
        { firstName: "John", lastName: "Writer", organizationDomain: "theverge.com" },
        expect.objectContaining({ orgId: "org-1" })
      );
      // Should NOT call apolloEnrich (journalist path uses apolloMatch)
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("skips journalist lead when apolloMatch returns no email", async () => {
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-j-fail",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "journalist:j-uuid-3",
          data: {
            firstName: "Ghost",
            lastName: "Journalist",
            organizationDomain: "unknown.com",
            sourceType: "journalist",
          },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([]); // no more rows

      vi.mocked(apolloMatch).mockResolvedValueOnce({
        enrichmentId: null,
        person: null,
        cached: false,
      });

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(false);
      expect(vi.mocked(apolloMatch)).toHaveBeenCalled();
    });

    it("returns found: false when no outlets exist and discovery finds none", async () => {
      pgSqlMock.mockResolvedValue([]);

      vi.mocked(db.query.cursors.findFirst).mockResolvedValueOnce(undefined);
      vi.mocked(fetchOutletsByCampaign).mockResolvedValueOnce([]);

      // buffer/next triggers auto-discovery but finds nothing
      vi.mocked(fetchNextOutlet).mockResolvedValueOnce({ found: false });

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        sourceType: "journalist",
        featureInput: { companyContext: "A test brand" },
      });

      expect(result.found).toBe(false);
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledOnce();
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledWith(
        expect.objectContaining({
          campaignId: "campaign-1",
          brandId: "brand-1",
          orgId: "org-1",
        }),
      );
    });

    it("discovers outlets and fills buffer — passes featureInput through as-is", async () => {
      const journalistRow = toClaimedRow({
        id: "buf-j-discover",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "discovered@outlet.com",
        externalId: "journalist:j-discovered",
        data: {
          firstName: "Jane",
          lastName: "Reporter",
          organizationName: "Discovered Outlet",
          sourceType: "journalist",
        },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])              // pullNext: buffer empty
        .mockResolvedValueOnce([journalistRow]); // pullNext retry: claimed journalist lead

      vi.mocked(db.query.cursors.findFirst).mockResolvedValueOnce(undefined);

      vi.mocked(fetchOutletsByCampaign)
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([{
          id: "outlet-discovered",
          outletName: "Discovered Outlet",
          outletUrl: "https://discovered-outlet.com",
          outletDomain: "discovered-outlet.com",
          relevanceScore: 0.9,
          outletStatus: "active",
          campaignId: "campaign-1",
        }]);

      // buffer/next triggers auto-discovery and finds an outlet
      vi.mocked(fetchNextOutlet).mockResolvedValueOnce({ found: true, outlet: {
        outletId: "outlet-discovered",
        outletName: "Discovered Outlet",
        outletUrl: "https://discovered-outlet.com",
        outletDomain: "discovered-outlet.com",
        campaignId: "campaign-1",
        brandId: "brand-1",
        relevanceScore: 0.9,
        whyRelevant: "Relevant outlet",
        whyNotRelevant: "",
        overallRelevance: null,
      }});

      vi.mocked(fetchNextJournalist)
        .mockResolvedValueOnce({
          found: true,
          journalist: {
            id: "j-discovered",
            journalistName: "Jane Reporter",
            firstName: "Jane",
            lastName: "Reporter",
            entityType: "individual" as const,
            relevanceScore: 0.8,
            whyRelevant: "Covers tech",
            whyNotRelevant: "",
            emails: [{ email: "discovered@outlet.com", isValid: true, confidence: 0.95 }],
          },
        })
        .mockResolvedValueOnce({ found: false });

      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValueOnce(undefined);

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({
        onConflictDoNothing: onConflictMock,
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const featureInput = {
        companyContext: "AI-powered PR distribution platform",
        prAngle: "Launch of the Stripe for Distribution",
        targetOutlets: "Top-tier tech blogs, SaaS publications",
      };

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        sourceType: "journalist",
        featureInput,
      });

      // fetchNextOutlet triggers auto-discovery on outlets-service
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledOnce();
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledWith(
        expect.objectContaining({
          campaignId: "campaign-1",
          brandId: "brand-1",
          orgId: "org-1",
        }),
      );
      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("discovered@outlet.com");
    });

    it("calls fetchNextOutlet even without featureInput", async () => {
      pgSqlMock.mockResolvedValue([]);

      vi.mocked(db.query.cursors.findFirst).mockResolvedValueOnce(undefined);
      vi.mocked(fetchOutletsByCampaign).mockResolvedValueOnce([]);

      // buffer/next triggers auto-discovery but finds nothing
      vi.mocked(fetchNextOutlet).mockResolvedValueOnce({ found: false });

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        sourceType: "journalist",
      });

      expect(result.found).toBe(false);
      // fetchNextOutlet is always called — outlets-service handles discovery internally
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledOnce();
      expect(vi.mocked(fetchNextOutlet)).toHaveBeenCalledWith(
        expect.objectContaining({
          campaignId: "campaign-1",
          brandId: "brand-1",
        }),
      );
    });

    it("uses apolloMatch proactively when resolve returns journalists without emails", async () => {
      const journalistRow = toClaimedRow({
        id: "buf-j-noemail",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "found@techcrunch.com",
        externalId: "journalist:j-noemail",
        data: {
          firstName: "Bob",
          lastName: "Writer",
          organizationDomain: "techcrunch.com",
          organizationName: "TechCrunch",
          sourceType: "journalist",
        },
        brandId: "brand-1",
        orgId: "org-1",
        userId: null,
      });

      pgSqlMock
        .mockResolvedValueOnce([])              // pullNext: buffer empty
        .mockResolvedValueOnce([journalistRow]); // pullNext retry: claimed lead

      vi.mocked(db.query.cursors.findFirst).mockResolvedValueOnce(undefined);
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValueOnce(undefined);

      // Outlets exist
      vi.mocked(fetchOutletsByCampaign).mockResolvedValueOnce([
        { id: "outlet-1", outletName: "TechCrunch", outletUrl: "https://techcrunch.com", outletDomain: "techcrunch.com", relevanceScore: 85, outletStatus: "open", campaignId: "campaign-1" },
      ]);

      // buffer/next returns journalist WITHOUT emails, then exhausted
      vi.mocked(fetchNextJournalist)
        .mockResolvedValueOnce({
          found: true,
          journalist: {
            id: "j-noemail",
            journalistName: "Bob Writer",
            firstName: "Bob",
            lastName: "Writer",
            entityType: "individual" as const,
            relevanceScore: 0.8,
            whyRelevant: "Covers tech",
            whyNotRelevant: "",
            emails: [],
          },
        })
        .mockResolvedValueOnce({ found: false });

      // apolloMatch finds the email
      vi.mocked(apolloMatch).mockResolvedValueOnce({
        person: {
          id: "apollo-bob",
          email: "found@techcrunch.com",
          firstName: "Bob",
          lastName: "Writer",
          organizationName: "TechCrunch",
          organizationDomain: "techcrunch.com",
        },
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        sourceType: "journalist",
      });

      // apolloMatch should have been called during fillBufferFromJournalists
      expect(vi.mocked(apolloMatch)).toHaveBeenCalledWith(
        { firstName: "Bob", lastName: "Writer", organizationDomain: "techcrunch.com" },
        expect.objectContaining({ campaignId: "campaign-1", brandId: "brand-1" }),
      );
      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("found@techcrunch.com");
    });

    it("skips lead when markServed returns inserted: false (race condition dedup)", async () => {
      // Regression test: two concurrent pullNext calls claim different rows
      // but both have the same email. The second call's markServed returns
      // inserted: false due to the unique index — it should skip and serve the next lead.
      pgSqlMock
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-dup",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "jserra@elkisconstruction.com",
          externalId: "e-dup",
          data: { name: "J Serra" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })])
        .mockResolvedValueOnce([toClaimedRow({
          id: "buf-next",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "unique@other.com",
          externalId: "e-next",
          data: { name: "Unique" },
          brandId: "brand-1",
          orgId: "org-1",
          userId: null,
        })]);

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      vi.mocked(resolveOrCreateLead)
        .mockResolvedValueOnce({ leadId: "lead-dup", isNew: false })
        .mockResolvedValueOnce({ leadId: "lead-next", isNew: true });

      // First markServed: conflict (another request already served this email)
      // Second markServed: success
      const returningMockEmpty = vi.fn().mockResolvedValueOnce([]);
      const onConflictMockEmpty = vi.fn().mockReturnValue({ returning: returningMockEmpty });

      const returningMockSuccess = vi.fn().mockResolvedValueOnce([{ id: "served-next" }]);
      const onConflictMockSuccess = vi.fn().mockReturnValue({ returning: returningMockSuccess });

      const valuesMock = vi.fn()
        .mockReturnValueOnce({ onConflictDoNothing: onConflictMockEmpty })     // markServed → conflict
        .mockReturnValueOnce({ onConflictDoNothing: onConflictMockSuccess });  // markServed → success

      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        orgId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      // Should skip the duplicate and serve the next lead
      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("unique@other.com");
      expect(result.lead?.leadId).toBe("lead-next");
    });
  });
});
