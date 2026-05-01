import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import request from "supertest";
import app from "../../src/index.js";
import {
  cleanupTestData,
  closeDb,
  getAuthHeaders,
  seedBuffer,
  TEST_API_KEY,
} from "./setup.js";
import { createRun } from "../../src/lib/runs-client.js";
import { checkDeliveryStatus } from "../../src/lib/email-gateway-client.js";
import { checkEmailStatus } from "../../src/lib/email-gateway-client.js";

vi.mock("../../src/lib/runs-client.js", () => ({
  createRun: vi.fn().mockResolvedValue({ id: "mock-run-id" }),
  updateRun: vi.fn().mockResolvedValue(undefined),
  addCosts: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: vi.fn().mockResolvedValue({ results: [] }),
  isContacted: vi.fn().mockReturnValue(false),
  checkEmailStatus: vi.fn().mockReturnValue({ contacted: false, bounced: false, unsubscribed: false }),
}));

vi.mock("../../src/lib/apollo-client.js", () => ({
  apolloSearch: vi.fn().mockResolvedValue({ people: [], pagination: {} }),
  apolloSearchNext: vi.fn().mockResolvedValue({ people: [], pagination: {} }),
  apolloSearchParams: vi.fn().mockResolvedValue({ searchParams: {} }),
  fetchApolloStats: vi.fn().mockResolvedValue(null),
  apolloMatch: vi.fn().mockResolvedValue(null),
  apolloEnrich: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../src/lib/campaign-client.js", () => ({
  fetchCampaign: vi.fn().mockResolvedValue(null),
}));

vi.mock("../../src/lib/brand-client.js", () => ({
  fetchBrand: vi.fn().mockResolvedValue(null),
  extractBrandFields: vi.fn().mockResolvedValue(null),
  fetchExtractedFields: vi.fn().mockResolvedValue(null),
}));

let runCounter = 0;
function uniqueRunId(): string {
  return `test-run-${Date.now()}-${++runCounter}`;
}

describe("API Integration Tests", { timeout: 30000 }, () => {
  beforeAll(async () => {
    await cleanupTestData();
  });

  afterAll(async () => {
    await cleanupTestData();
    await closeDb();
  });

  describe("Health check", () => {
    it("GET /health returns 200 with service status", async () => {
      const res = await request(app).get("/health");
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ status: "ok", service: "lead-service" });
    });
  });

  describe("Authentication", () => {
    it("rejects requests without API key", async () => {
      const res = await request(app)
        .post("/orgs/buffer/next")
        .send({});

      expect(res.status).toBe(401);
    });

    it("rejects requests with invalid API key", async () => {
      const res = await request(app)
        .post("/orgs/buffer/next")
        .set("x-api-key", "wrong-key")
        .set("x-org-id", "test")
        .send({});

      expect(res.status).toBe(401);
    });

    it("rejects requests without x-org-id header", async () => {
      const res = await request(app)
        .post("/orgs/buffer/next")
        .set("x-api-key", TEST_API_KEY)
        .send({});

      expect(res.status).toBe(400);
    });
  });

  describe("POST /orgs/buffer/next", () => {
    it("pulls next lead from buffer", async () => {
      await seedBuffer({
        campaignId: "campaign-b",
        brandId: "brand-b",
        leads: [{ email: "charlie@example.com", data: { name: "Charlie" } }],
      });

      const res = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-b", brandId: "brand-b", runId: uniqueRunId() }))
        .send({});

      expect(res.status).toBe(200);
      expect(res.body.found).toBe(true);
      expect(res.body.lead.email).toBe("charlie@example.com");
      expect(res.body.lead.leadId).toBeDefined();
    });

    it("returns found: false when buffer empty", async () => {
      const res = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-empty", brandId: "brand-empty", runId: uniqueRunId() }))
        .send({});

      expect(res.status).toBe(200);
      expect(res.body.found).toBe(false);
    });

    it("returns 400 when x-brand-id header is missing", async () => {
      const res = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-x", runId: uniqueRunId() }))
        .send({});

      expect(res.status).toBe(400);
      expect(res.body.error).toContain("x-campaign-id and x-brand-id");
    });

    it("passes workflowSlug to createRun", async () => {
      await seedBuffer({
        campaignId: "campaign-wf-next",
        brandId: "brand-wf-next",
        leads: [{ email: "wf-next@example.com" }],
      });

      vi.mocked(createRun).mockClear();

      const headers = getAuthHeaders({ campaignId: "campaign-wf-next", brandId: "brand-wf-next", runId: uniqueRunId() });
      headers["x-workflow-slug"] = "cold-email-outreach";

      await request(app)
        .post("/orgs/buffer/next")
        .set(headers)
        .send({});

      expect(createRun).toHaveBeenCalledWith(
        expect.objectContaining({ workflowSlug: "cold-email-outreach" })
      );
    });

    it("deduplicates — skips leads that email-gateway reports as contacted", async () => {
      await seedBuffer({
        campaignId: "campaign-c",
        brandId: "brand-c",
        leads: [
          { email: "dedup@example.com" },
          { email: "fresh@example.com" },
        ],
      });

      // First call: checkEmailStatus reports dedup@example.com as contacted
      // Second call: fresh@example.com not contacted
      vi.mocked(checkDeliveryStatus)
        .mockResolvedValueOnce({
          results: [{
            email: "dedup@example.com",
            broadcast: {
              campaign: { contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              brand: { contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              global: {
                email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              },
            },
          }],
        })
        .mockResolvedValue({ results: [] });

      vi.mocked(checkEmailStatus)
        .mockReturnValueOnce({ contacted: true, bounced: false, unsubscribed: false })
        .mockReturnValue({ contacted: false, bounced: false, unsubscribed: false });

      const res = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-c", brandId: "brand-c", runId: uniqueRunId() }))
        .send({});

      expect(res.body.found).toBe(true);
      expect(res.body.lead.email).toBe("fresh@example.com");

      // Reset mocks for subsequent tests
      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });
      vi.mocked(checkEmailStatus).mockReturnValue({ contacted: false, bounced: false, unsubscribed: false });
    }, 10000);
  });

  describe("POST /orgs/buffer/next idempotency", () => {
    it("returns same lead on retry with same x-run-id", async () => {
      await seedBuffer({
        campaignId: "campaign-idem-1",
        brandId: "brand-idem-1",
        leads: [{ email: "idem@example.com", data: { name: "Idem" } }],
      });

      const runId = uniqueRunId();

      const first = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-1", brandId: "brand-idem-1", runId }))
        .send({});

      expect(first.status).toBe(200);
      expect(first.body.found).toBe(true);
      expect(first.body.lead.email).toBe("idem@example.com");
      expect(first.body.lead.leadId).toBeDefined();

      // Retry with same runId — should return cached result
      const retry = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-1", brandId: "brand-idem-1", runId }))
        .send({});

      expect(retry.status).toBe(200);
      expect(retry.body.found).toBe(true);
      expect(retry.body.lead.email).toBe("idem@example.com");
    });

    it("does not consume extra leads on retry", async () => {
      await seedBuffer({
        campaignId: "campaign-idem-2",
        brandId: "brand-idem-2",
        leads: [
          { email: "first@example.com", data: { name: "First" } },
          { email: "second@example.com", data: { name: "Second" } },
        ],
      });

      const runId = uniqueRunId();

      const first = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-2", brandId: "brand-idem-2", runId }))
        .send({});

      expect(first.body.found).toBe(true);
      const firstEmail = first.body.lead.email;

      // Retry with same key — should NOT consume second lead
      await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-2", brandId: "brand-idem-2", runId }))
        .send({});

      // Pull with different key — should get the other lead
      const second = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-2", brandId: "brand-idem-2", runId: uniqueRunId() }))
        .send({});

      expect(second.body.found).toBe(true);
      expect(second.body.lead.email).not.toBe(firstEmail);
    });

    it("caches found: false responses too", async () => {
      const runId = uniqueRunId();

      const first = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-empty", brandId: "brand-idem-empty", runId }))
        .send({});

      expect(first.body.found).toBe(false);

      // Now seed a lead to that campaign
      await seedBuffer({
        campaignId: "campaign-idem-empty",
        brandId: "brand-idem-empty",
        leads: [{ email: "late@example.com" }],
      });

      // Retry with same key — should still return cached found: false
      const retry = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-empty", brandId: "brand-idem-empty", runId }))
        .send({});

      expect(retry.body.found).toBe(false);
    });

    it("works without idempotencyKey (backwards compatible)", async () => {
      await seedBuffer({
        campaignId: "campaign-idem-compat",
        brandId: "brand-idem-compat",
        leads: [{ email: "compat@example.com" }],
      });

      const res = await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-idem-compat", brandId: "brand-idem-compat", runId: uniqueRunId() }))
        .send({});

      expect(res.status).toBe(200);
      expect(res.body.found).toBe(true);
      expect(res.body.lead.email).toBe("compat@example.com");
    });
  }, 15000);

  describe("GET /orgs/leads", () => {
    it("returns served leads with full enrichment data (no filtering)", async () => {
      const richData = {
        firstName: "Diana",
        lastName: "Prince",
        email: "diana@example.com",
        title: "CEO",
        linkedinUrl: "https://linkedin.com/in/diana",
        organizationName: "Themyscira Inc",
        organizationDomain: "themyscira.com",
        organizationIndustry: "Defense",
        organizationSize: "501-1000",
        headline: "CEO & Founder at Themyscira Inc",
        city: "Gateway City",
        state: "CA",
        country: "United States",
        organizationShortDescription: "Leading defense tech company",
        organizationFoundedYear: 2010,
        organizationRevenueUsd: "50000000",
        seniority: "founder",
        departments: ["executive"],
        photoUrl: "https://example.com/diana.jpg",
      };

      await seedBuffer({
        campaignId: "campaign-leads",
        brandId: "brand-leads",
        leads: [{ email: "diana@example.com", apolloPersonId: "apollo-1", data: richData }],
      });

      // Pull it to move to served_leads
      await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-leads", brandId: "brand-leads", runId: uniqueRunId() }))
        .send({});

      // Query GET /orgs/leads
      const res = await request(app)
        .get("/orgs/leads?brandId=brand-leads&campaignId=campaign-leads")
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      expect(res.body.leads.length).toBeGreaterThanOrEqual(1);

      const lead = res.body.leads.find((l: { email: string }) => l.email === "diana@example.com");
      expect(lead).toBeDefined();
      expect(lead.enrichment).not.toBeNull();
      expect(lead.status).toBe("served");

      expect(lead.enrichment.firstName).toBe("Diana");
      expect(lead.enrichment.lastName).toBe("Prince");
      expect(lead.enrichment.title).toBe("CEO");
      expect(lead.enrichment.organizationName).toBe("Themyscira Inc");
      expect(lead.enrichment.headline).toBe("CEO & Founder at Themyscira Inc");
      expect(lead.enrichment.city).toBe("Gateway City");
      expect(lead.enrichment.country).toBe("United States");
      expect(lead.enrichment.organizationShortDescription).toBe("Leading defense tech company");
      expect(lead.enrichment.organizationFoundedYear).toBe(2010);
      expect(lead.enrichment.organizationRevenueUsd).toBe("50000000");
      expect(lead.enrichment.seniority).toBe("founder");
      expect(lead.enrichment.departments).toEqual(["executive"]);
      expect(lead.enrichment.photoUrl).toBe("https://example.com/diana.jpg");
    });

    it("returns buffer entries with status field", async () => {
      const bufferCampaign = `campaign-buffer-status-${Date.now()}`;
      await seedBuffer({
        campaignId: bufferCampaign,
        brandId: "brand-buffer-status",
        leads: [
          { email: "buffered1@example.com", data: { firstName: "Buffered", lastName: "Lead", email: "buffered1@example.com", organizationName: "Buffer Corp" } },
          { email: "buffered2@example.com" },
        ],
      });

      const res = await request(app)
        .get(`/orgs/leads?campaignId=${bufferCampaign}`)
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      expect(res.body.leads.length).toBe(2);

      const lead1 = res.body.leads.find((l: { email: string }) => l.email === "buffered1@example.com");
      expect(lead1).toBeDefined();
      expect(lead1.status).toBe("buffered");
      expect(lead1.enrichment).not.toBeNull();
      expect(lead1.enrichment.firstName).toBe("Buffered");
      expect(lead1.enrichment.organizationName).toBe("Buffer Corp");
      // Delivery booleans default to false
      expect(lead1.contacted).toBe(false);
      expect(lead1.sent).toBe(false);
      expect(lead1.delivered).toBe(false);
      expect(lead1.servedAt).toBeNull();

      const lead2 = res.body.leads.find((l: { email: string }) => l.email === "buffered2@example.com");
      expect(lead2).toBeDefined();
      expect(lead2.status).toBe("buffered");
      expect(lead2.enrichment).toBeNull();
    });

    it("returns both served and buffered leads with correct statuses", async () => {
      const mixedCampaign = `campaign-mixed-${Date.now()}`;

      // Seed two leads, pull one to make it served
      await seedBuffer({
        campaignId: mixedCampaign,
        brandId: "brand-mixed",
        leads: [
          { email: "will-serve@example.com", data: { firstName: "Served", lastName: "One", organizationName: "Served Corp" } },
          { email: "stays-buffered@example.com", data: { firstName: "Buffered", lastName: "Two", organizationName: "Buffer Corp" } },
        ],
      });

      // Pull one to served_leads
      await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: mixedCampaign, brandId: "brand-mixed", runId: uniqueRunId() }))
        .send({});

      const res = await request(app)
        .get(`/orgs/leads?campaignId=${mixedCampaign}`)
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      // At least 2: one served + one still buffered (the pulled one changes status to "claimed" in buffer, then moved to served)
      const served = res.body.leads.filter((l: { status: string }) => l.status === "served");
      const buffered = res.body.leads.filter((l: { status: string }) => l.status === "buffered" || l.status === "claimed" || l.status === "skipped");

      expect(served.length).toBeGreaterThanOrEqual(1);
      expect(buffered.length).toBeGreaterThanOrEqual(1);
    });

    it("brandId filter works on buffer entries", async () => {
      const brandFilterCampaign = `campaign-brand-filter-${Date.now()}`;
      await seedBuffer({
        campaignId: brandFilterCampaign,
        brandId: "brand-filter-match",
        leads: [{ email: "match@example.com" }],
      });
      await seedBuffer({
        campaignId: brandFilterCampaign,
        brandId: "brand-filter-nomatch",
        leads: [{ email: "nomatch@example.com" }],
      });

      const res = await request(app)
        .get(`/orgs/leads?campaignId=${brandFilterCampaign}&brandId=brand-filter-match`)
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      const emails = res.body.leads.map((l: { email: string }) => l.email);
      expect(emails).toContain("match@example.com");
      expect(emails).not.toContain("nomatch@example.com");
    });

    it("returns null enrichment when metadata is empty", async () => {
      await seedBuffer({
        campaignId: "campaign-leads-empty",
        brandId: "brand-leads-empty",
        leads: [{ email: "empty@example.com" }],
      });

      await request(app)
        .post("/orgs/buffer/next")
        .set(getAuthHeaders({ campaignId: "campaign-leads-empty", brandId: "brand-leads-empty", runId: uniqueRunId() }))
        .send({});

      const res = await request(app)
        .get("/orgs/leads?brandId=brand-leads-empty&campaignId=campaign-leads-empty")
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      const lead = res.body.leads.find((l: { email: string }) => l.email === "empty@example.com");
      expect(lead).toBeDefined();
      expect(lead.enrichment).toBeNull();
    });
  });

  describe("Cursor endpoints", () => {
    it("GET returns null for non-existent cursor", async () => {
      const res = await request(app)
        .get("/orgs/cursor/new-namespace")
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      expect(res.body.state).toBeNull();
    });

    it("PUT creates cursor, GET retrieves it", async () => {
      const state = { page: 5, lastId: "abc123" };

      const putRes = await request(app)
        .put("/orgs/cursor/my-namespace")
        .set(getAuthHeaders())
        .send({ state });

      expect(putRes.status).toBe(200);
      expect(putRes.body.ok).toBe(true);

      const getRes = await request(app)
        .get("/orgs/cursor/my-namespace")
        .set(getAuthHeaders());

      expect(getRes.status).toBe(200);
      expect(getRes.body.state).toEqual(state);
    });

    it("PUT updates existing cursor", async () => {
      await request(app)
        .put("/orgs/cursor/update-test")
        .set(getAuthHeaders())
        .send({ state: { v: 1 } });

      await request(app)
        .put("/orgs/cursor/update-test")
        .set(getAuthHeaders())
        .send({ state: { v: 2 } });

      const res = await request(app)
        .get("/orgs/cursor/update-test")
        .set(getAuthHeaders());

      expect(res.body.state).toEqual({ v: 2 });
    });
  });
});
