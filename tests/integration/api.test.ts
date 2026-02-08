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
