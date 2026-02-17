import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from "vitest";
import request from "supertest";
import app from "../../src/index.js";
import {
  setupTestOrg,
  cleanupTestData,
  closeDb,
  getAuthHeaders,
  TEST_API_KEY,
} from "./setup.js";

vi.mock("../../src/lib/runs-client.js", () => ({
  ensureOrganization: vi.fn().mockResolvedValue("mock-org-id"),
  createRun: vi.fn().mockResolvedValue({ id: "mock-run-id" }),
  updateRun: vi.fn().mockResolvedValue(undefined),
  addCosts: vi.fn().mockResolvedValue(undefined),
}));

describe("API Integration Tests", () => {
  beforeAll(async () => {
    process.env.LEAD_SERVICE_API_KEY = TEST_API_KEY;
    await setupTestOrg();
  });

  afterAll(async () => {
    await cleanupTestData();
    await closeDb();
  });

  describe("Health check", () => {
    it("GET / returns 200 with service status", async () => {
      const res = await request(app).get("/");
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ status: "ok", service: "lead-service" });
    });

    it("GET /health returns 200 with service status", async () => {
      const res = await request(app).get("/health");
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ status: "ok", service: "lead-service" });
    });
  });

  describe("Authentication", () => {
    it("rejects requests without API key", async () => {
      const res = await request(app)
        .post("/buffer/push")
        .send({ campaignId: "c1", brandId: "b1", leads: [] });

      expect(res.status).toBe(401);
    });

    it("rejects requests with invalid API key", async () => {
      const res = await request(app)
        .post("/buffer/push")
        .set("x-api-key", "wrong-key")
        .set("x-app-id", "test")
        .set("x-org-id", "test")
        .send({ campaignId: "c1", brandId: "b1", leads: [] });

      expect(res.status).toBe(401);
    });
  });

  describe("POST /buffer/push", () => {
    it("buffers new leads", async () => {
      const res = await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-a",
          brandId: "brand-a",
          parentRunId: "test-run-push-a",
          leads: [
            { email: "alice@example.com", externalId: "e1", data: { name: "Alice" } },
            { email: "bob@example.com", externalId: "e2", data: { name: "Bob" } },
          ],
        });

      expect(res.status).toBe(200);
      expect(res.body.buffered).toBe(2);
      expect(res.body.skippedAlreadyServed).toBe(0);
    });

    it("returns 400 when campaignId or brandId missing", async () => {
      const res = await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({ leads: [] });

      expect(res.status).toBe(400);
    });

    it("returns 400 when brandId is empty string", async () => {
      const res = await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-x",
          brandId: "",
          parentRunId: "test-run",
          leads: [{ email: "test@example.com" }],
        });

      expect(res.status).toBe(400);
      expect(res.body.details.fieldErrors.brandId).toBeDefined();
    });
  });

  describe("POST /buffer/next", () => {
    it("pulls next lead from buffer", async () => {
      // First push a lead
      await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-b",
          brandId: "brand-b",
          parentRunId: "test-run-push-b",
          leads: [{ email: "charlie@example.com", data: { name: "Charlie" } }],
        });

      // Then pull it
      const res = await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-b", brandId: "brand-b", parentRunId: "test-run-next-b" });

      expect(res.status).toBe(200);
      expect(res.body.found).toBe(true);
      expect(res.body.lead.email).toBe("charlie@example.com");
    });

    it("returns found: false when buffer empty", async () => {
      const res = await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-empty", brandId: "brand-empty", parentRunId: "test-run-next-empty" });

      expect(res.status).toBe(200);
      expect(res.body.found).toBe(false);
    });

    it("returns 400 when brandId is empty string", async () => {
      const res = await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-x", brandId: "", parentRunId: "test-run" });

      expect(res.status).toBe(400);
      expect(res.body.details.fieldErrors.brandId).toBeDefined();
    });

    it("deduplicates — same lead not served twice", async () => {
      // Push same lead twice to a new campaign
      await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-c",
          brandId: "brand-c",
          parentRunId: "test-run-push-c1",
          leads: [{ email: "dedup@example.com" }],
        });

      // Pull it once
      const first = await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-c", brandId: "brand-c", parentRunId: "test-run-next-c1" });

      expect(first.body.found).toBe(true);

      // Push again
      const pushAgain = await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-c",
          brandId: "brand-c",
          parentRunId: "test-run-push-c2",
          leads: [{ email: "dedup@example.com" }],
        });

      expect(pushAgain.body.skippedAlreadyServed).toBe(1);

      // Pull again — should find nothing
      const second = await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-c", brandId: "brand-c", parentRunId: "test-run-next-c2" });

      expect(second.body.found).toBe(false);
    }, 10000); // Increased timeout for CI database latency
  });

  describe("GET /leads", () => {
    it("returns served leads with full enrichment data (no filtering)", async () => {
      // Push a lead with rich metadata that simulates Apollo data
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
        // Extra fields that were previously filtered out:
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

      await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-leads",
          brandId: "brand-leads",
          parentRunId: "test-run-push-leads",
          leads: [{ email: "diana@example.com", externalId: "apollo-1", data: richData }],
        });

      // Pull it to move to served_leads
      await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-leads", brandId: "brand-leads", parentRunId: "test-run-next-leads" });

      // Query GET /leads
      const res = await request(app)
        .get("/leads?brandId=brand-leads&campaignId=campaign-leads")
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      expect(res.body.leads.length).toBeGreaterThanOrEqual(1);

      const lead = res.body.leads.find((l: { email: string }) => l.email === "diana@example.com");
      expect(lead).toBeDefined();
      expect(lead.enrichment).not.toBeNull();

      // Verify ALL fields pass through — not just the old 8
      expect(lead.enrichment.firstName).toBe("Diana");
      expect(lead.enrichment.lastName).toBe("Prince");
      expect(lead.enrichment.title).toBe("CEO");
      expect(lead.enrichment.organizationName).toBe("Themyscira Inc");

      // These would have been stripped by the old extractEnrichment
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

    it("returns null enrichment when metadata is empty", async () => {
      // Push a lead with no enrichment data
      await request(app)
        .post("/buffer/push")
        .set(getAuthHeaders())
        .send({
          campaignId: "campaign-leads-empty",
          brandId: "brand-leads-empty",
          parentRunId: "test-run-push-leads-empty",
          leads: [{ email: "empty@example.com" }],
        });

      await request(app)
        .post("/buffer/next")
        .set(getAuthHeaders())
        .send({ campaignId: "campaign-leads-empty", brandId: "brand-leads-empty", parentRunId: "test-run-next-leads-empty" });

      const res = await request(app)
        .get("/leads?brandId=brand-leads-empty&campaignId=campaign-leads-empty")
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
        .get("/cursor/new-namespace")
        .set(getAuthHeaders());

      expect(res.status).toBe(200);
      expect(res.body.state).toBeNull();
    });

    it("PUT creates cursor, GET retrieves it", async () => {
      const state = { page: 5, lastId: "abc123" };

      const putRes = await request(app)
        .put("/cursor/my-namespace")
        .set(getAuthHeaders())
        .send({ state });

      expect(putRes.status).toBe(200);
      expect(putRes.body.ok).toBe(true);

      const getRes = await request(app)
        .get("/cursor/my-namespace")
        .set(getAuthHeaders());

      expect(getRes.status).toBe(200);
      expect(getRes.body.state).toEqual(state);
    });

    it("PUT updates existing cursor", async () => {
      await request(app)
        .put("/cursor/update-test")
        .set(getAuthHeaders())
        .send({ state: { v: 1 } });

      await request(app)
        .put("/cursor/update-test")
        .set(getAuthHeaders())
        .send({ state: { v: 2 } });

      const res = await request(app)
        .get("/cursor/update-test")
        .set(getAuthHeaders());

      expect(res.body.state).toEqual({ v: 2 });
    });
  });
});
