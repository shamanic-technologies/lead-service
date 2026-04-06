import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock db
const mockFrom = vi.fn();
const mockWhere = vi.fn();
const mockSelect = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: {
    select: (...args: unknown[]) => {
      mockSelect(...args);
      return {
        from: (...fArgs: unknown[]) => {
          mockFrom(...fArgs);
          return {
            where: (...wArgs: unknown[]) => mockWhere(...wArgs),
          };
        },
      };
    },
  },
}));

const mockCheckDeliveryStatus = vi.fn();
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (...args: unknown[]) => mockCheckDeliveryStatus(...args),
  isContacted: () => false,
}));

vi.mock("../../src/middleware/auth.js", () => ({
  authenticate: (_req: unknown, _res: unknown, next: () => void) => next(),
  getServiceContext: (req: any) => ({
    orgId: req.orgId,
    userId: req.userId,
    runId: req.runId,
    campaignId: req.campaignId,
    brandId: req.brandId,
    workflowSlug: req.workflowSlug,
    featureSlug: req.featureSlug,
  }),
}));

import request from "supertest";
import express from "express";
import leadStatusRouter from "../../src/routes/lead-status.js";
import { flattenCampaignStatus, flattenBrandStatus } from "../../src/routes/lead-status.js";

function createApp() {
  const app = express();
  app.use((req: any, _res, next) => {
    req.orgId = "org-1";
    req.userId = "user-1";
    req.runId = "run-1";
    next();
  });
  app.use(leadStatusRouter);
  return app;
}

function makeBroadcastStatus(overrides: {
  campaign?: Record<string, unknown>;
  brand?: Record<string, unknown>;
}) {
  const defaultScoped = {
    contacted: false, delivered: false, opened: false, replied: false,
    replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null,
  };
  return {
    campaign: { ...defaultScoped, ...overrides.campaign },
    brand: { ...defaultScoped, ...overrides.brand },
    global: { email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null } },
  };
}

describe("GET /leads/status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockWhere.mockResolvedValue([]);
    mockCheckDeliveryStatus.mockResolvedValue(null);
  });

  it("returns 400 when neither campaignId nor brandId is provided", async () => {
    const app = createApp();
    const res = await request(app).get("/leads/status");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("campaignId or brandId");
  });

  it("returns empty statuses when no served leads exist", async () => {
    mockWhere.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ statuses: [] });
    expect(mockCheckDeliveryStatus).not.toHaveBeenCalled();
  });

  // --- Campaign-scoped mode ---

  it("returns per-lead delivery status with campaignId (campaign-scoped)", async () => {
    const servedRows = [
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
      { leadId: "lead-2", email: "bob@acme.com", brandIds: ["b1"], metadata: null },
    ];
    mockWhere.mockResolvedValue(servedRows);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadIds: ["lead-1"],
          email: "alice@acme.com",
          broadcast: makeBroadcastStatus({
            campaign: { contacted: true, delivered: true, lastDeliveredAt: "2026-03-29T10:00:00Z" },
          }),
        },
        {
          leadIds: ["lead-2"],
          email: "bob@acme.com",
          broadcast: makeBroadcastStatus({
            campaign: { contacted: true, bounced: true },
          }),
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(2);

    const alice = res.body.statuses.find((s: any) => s.email === "alice@acme.com");
    expect(alice.contacted).toBe(true);
    expect(alice.delivered).toBe(true);
    expect(alice.lastDeliveredAt).toBe("2026-03-29T10:00:00Z");
    const bob = res.body.statuses.find((s: any) => s.email === "bob@acme.com");
    expect(bob.contacted).toBe(true);
    expect(bob.bounced).toBe(true);

    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1",
      "c1",
      expect.any(Array),
      expect.objectContaining({ orgId: "org-1" }),
    );
  });

  it("passes campaignId to email-gateway in campaign mode", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });

    const app = createApp();
    await request(app).get("/leads/status?campaignId=c1");

    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1", "c1", expect.any(Array), expect.any(Object),
    );
  });

  // --- Brand-scoped mode (cross-campaign) ---

  it("returns cross-campaign status with brandId only", async () => {
    const servedRows = [
      {
        leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"],
        metadata: null,
      },
    ];
    mockWhere.mockResolvedValue(servedRows);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadIds: ["lead-1"],
          email: "alice@acme.com",
          broadcast: makeBroadcastStatus({
            brand: { contacted: true, delivered: true, replied: true, lastDeliveredAt: "2026-03-29T10:00:00Z" },
          }),
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(1);

    const alice = res.body.statuses[0];
    expect(alice).toEqual({
      leadId: "lead-1",
      email: "alice@acme.com",
      contacted: true,
      delivered: true,
      bounced: false,
      replied: true,
      replyClassification: null,
      lastDeliveredAt: "2026-03-29T10:00:00Z",
    });
  });

  it("passes undefined campaignId to email-gateway in brand-only mode", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });

    const app = createApp();
    await request(app).get("/leads/status?brandId=b1");

    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1", undefined, expect.any(Array), expect.any(Object),
    );
  });

  it("deduplicates leads by email in cross-campaign mode", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });

    const app = createApp();
    const res = await request(app).get("/leads/status?brandId=b1");
    expect(res.body.statuses).toHaveLength(1);
  });

  // --- Edge cases ---

  it("returns all-false status when email-gateway is unreachable", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue(null);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body.statuses[0]).toMatchObject({
      contacted: false,
      delivered: false,
      bounced: false,
      replied: false,
      replyClassification: null,
    });
  });

  it("skips rows with null leadId", async () => {
    mockWhere.mockResolvedValue([
      { leadId: null, email: "orphan@acme.com", brandIds: ["b1"], metadata: null },
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue(null);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.body.statuses).toHaveLength(1);
    expect(res.body.statuses[0].leadId).toBe("lead-1");
  });

  it("groups email-gateway calls by first brandId", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
      { leadId: "lead-2", email: "bob@other.com", brandIds: ["b2"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });

    const app = createApp();
    await request(app).get("/leads/status?campaignId=c1");

    expect(mockCheckDeliveryStatus).toHaveBeenCalledTimes(2);
    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith("b1", "c1", expect.any(Array), expect.any(Object));
    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith("b2", "c1", expect.any(Array), expect.any(Object));
  });

  it("accepts both campaignId and brandId together", async () => {
    mockWhere.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1&brandId=b1");
    expect(res.status).toBe(200);
  });

  it("surfaces replyClassification from email-gateway (campaign-scoped)", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadIds: ["lead-1"],
          email: "alice@acme.com",
          broadcast: makeBroadcastStatus({
            campaign: { contacted: true, delivered: true, replied: true, replyClassification: "positive", lastDeliveredAt: "2026-04-01T12:00:00Z" },
          }),
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.body.statuses[0].replyClassification).toBe("positive");
    expect(res.body.statuses[0].replied).toBe(true);
  });

  it("surfaces replyClassification from email-gateway (brand-scoped)", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadIds: ["lead-1"],
          email: "alice@acme.com",
          broadcast: makeBroadcastStatus({
            brand: { contacted: true, delivered: true, replied: true, replyClassification: "negative", lastDeliveredAt: "2026-04-01T12:00:00Z" },
          }),
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?brandId=b1");
    expect(res.body.statuses[0].replyClassification).toBe("negative");
  });

  it("returns null replyClassification when no reply", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadIds: ["lead-1"],
          email: "alice@acme.com",
          broadcast: makeBroadcastStatus({
            campaign: { contacted: true, delivered: true, lastDeliveredAt: "2026-04-01T12:00:00Z" },
          }),
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.body.statuses[0].replyClassification).toBeNull();
    expect(res.body.statuses[0].replied).toBe(false);
  });

});

describe("flattenCampaignStatus", () => {
  it("detects replied and replyClassification from broadcast campaign", () => {
    const result = flattenCampaignStatus({
      leadIds: ["l1"],
      email: "a@b.com",
      broadcast: makeBroadcastStatus({
        campaign: { contacted: true, delivered: true, replied: true, replyClassification: "positive", lastDeliveredAt: "2026-03-29T10:00:00Z" },
      }),
    });

    expect(result.replied).toBe(true);
    expect(result.replyClassification).toBe("positive");
    expect(result.delivered).toBe(true);
    expect(result.contacted).toBe(true);
    expect(result.lastDeliveredAt).toBe("2026-03-29T10:00:00Z");
  });

  it("detects transactional delivery", () => {
    const defaultScoped = {
      contacted: false, delivered: false, opened: false, replied: false,
      replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null,
    };
    const result = flattenCampaignStatus({
      leadIds: ["l1"],
      email: "a@b.com",
      transactional: {
        campaign: { ...defaultScoped, contacted: true, delivered: true, lastDeliveredAt: "2026-03-28T08:00:00Z" },
        brand: defaultScoped,
        global: {
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
      },
    });

    expect(result.delivered).toBe(true);
    expect(result.contacted).toBe(true);
    expect(result.lastDeliveredAt).toBe("2026-03-28T08:00:00Z");
  });

  it("returns all false when no providers present", () => {
    const result = flattenCampaignStatus({ leadIds: ["l1"], email: "a@b.com" });
    expect(result).toEqual({
      contacted: false, delivered: false, bounced: false, replied: false, replyClassification: null, lastDeliveredAt: null,
    });
  });
});

describe("flattenBrandStatus", () => {
  it("uses brand scope for cross-campaign status", () => {
    const result = flattenBrandStatus({
      leadIds: ["l1"],
      email: "a@b.com",
      broadcast: makeBroadcastStatus({
        brand: { contacted: true, delivered: true, replied: true, lastDeliveredAt: "2026-03-29T10:00:00Z" },
      }),
    });

    expect(result.contacted).toBe(true);
    expect(result.delivered).toBe(true);
    expect(result.replied).toBe(true);
    expect(result.lastDeliveredAt).toBe("2026-03-29T10:00:00Z");
  });

  it("returns all false when no providers present", () => {
    const result = flattenBrandStatus({ leadIds: ["l1"], email: "a@b.com" });
    expect(result).toEqual({
      contacted: false, delivered: false, bounced: false, replied: false, replyClassification: null, lastDeliveredAt: null,
    });
  });
});
