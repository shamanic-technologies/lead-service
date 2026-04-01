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
}));

const mockFetchQualificationsByOrg = vi.fn();
vi.mock("../../src/lib/reply-qualification-client.js", () => ({
  fetchQualificationsByOrg: (...args: unknown[]) => mockFetchQualificationsByOrg(...args),
  classifyReply: (classification: string) => {
    if (classification === "willing_to_meet" || classification === "interested") return "positive";
    if (classification === "not_interested") return "negative";
    return "other";
  },
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
import outreachStatusRouter from "../../src/routes/outreach-status.js";
import { flattenBrandStatus } from "../../src/routes/outreach-status.js";

function createApp() {
  const app = express();
  app.use((req: any, _res, next) => {
    req.orgId = "org-1";
    req.userId = "user-1";
    req.runId = "run-1";
    next();
  });
  app.use(outreachStatusRouter);
  return app;
}

describe("GET /leads/outreach-status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockWhere.mockResolvedValue([]);
    mockCheckDeliveryStatus.mockResolvedValue(null);
    mockFetchQualificationsByOrg.mockResolvedValue(new Map());
  });

  it("returns 400 when brandId is missing", async () => {
    const app = createApp();
    const res = await request(app).get("/leads/outreach-status");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("brandId");
  });

  it("returns empty statuses when no served leads exist", async () => {
    mockWhere.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ statuses: [] });
    expect(mockCheckDeliveryStatus).not.toHaveBeenCalled();
    expect(mockFetchQualificationsByOrg).not.toHaveBeenCalled();
  });

  it("returns cross-campaign outreach status with reply classification", async () => {
    const servedRows = [
      {
        leadId: "lead-1",
        email: "alice@acme.com",
        brandIds: ["b1"],
        metadata: { journalistId: "j1", outletId: "o1", sourceType: "journalist" },
      },
      {
        leadId: "lead-2",
        email: "bob@acme.com",
        brandIds: ["b1"],
        metadata: null,
      },
    ];
    mockWhere.mockResolvedValue(servedRows);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadId: "lead-1",
          email: "alice@acme.com",
          broadcast: {
            campaign: {
              lead: { contacted: true, delivered: true, replied: true, lastDeliveredAt: null },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-29T10:00:00Z" },
            },
            brand: {
              lead: { contacted: true, delivered: true, replied: true, lastDeliveredAt: null },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-29T10:00:00Z" },
            },
            global: {
              email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
            },
          },
        },
        {
          leadId: "lead-2",
          email: "bob@acme.com",
          broadcast: {
            campaign: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: null },
              email: { contacted: true, delivered: false, bounced: true, unsubscribed: false, lastDeliveredAt: null },
            },
            brand: {
              lead: { contacted: true, delivered: false, replied: false, lastDeliveredAt: null },
              email: { contacted: true, delivered: false, bounced: true, unsubscribed: false, lastDeliveredAt: null },
            },
            global: {
              email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
            },
          },
        },
      ],
    });

    mockFetchQualificationsByOrg.mockResolvedValue(
      new Map([
        [
          "alice@acme.com",
          { id: "q1", fromEmail: "alice@acme.com", classification: "interested", confidence: 0.95, createdAt: "2026-03-30T10:00:00Z" },
        ],
      ]),
    );

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(2);

    const alice = res.body.statuses.find((s: any) => s.email === "alice@acme.com");
    expect(alice).toEqual({
      leadId: "lead-1",
      email: "alice@acme.com",
      journalistId: "j1",
      outletId: "o1",
      contacted: true,
      delivered: true,
      bounced: false,
      replied: true,
      replyClassification: "positive",
      lastDeliveredAt: "2026-03-29T10:00:00Z",
    });

    const bob = res.body.statuses.find((s: any) => s.email === "bob@acme.com");
    expect(bob).toEqual({
      leadId: "lead-2",
      email: "bob@acme.com",
      journalistId: null,
      outletId: null,
      contacted: true,
      delivered: false,
      bounced: true,
      replied: false,
      replyClassification: null,
      lastDeliveredAt: null,
    });
  });

  it("calls email-gateway without campaignId (brand-scoped)", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });
    mockFetchQualificationsByOrg.mockResolvedValue(new Map());

    const app = createApp();
    await request(app).get("/leads/outreach-status?brandId=b1");

    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1",
      undefined,
      [{ leadId: "lead-1", email: "alice@acme.com" }],
      expect.any(Object),
    );
  });

  it("deduplicates leads by email across campaigns", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: { journalistId: "j1", outletId: "o1" } },
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: { journalistId: "j1", outletId: "o1" } },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });
    mockFetchQualificationsByOrg.mockResolvedValue(new Map());

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(1);
  });

  it("skips rows with null leadId", async () => {
    mockWhere.mockResolvedValue([
      { leadId: null, email: "orphan@acme.com", brandIds: ["b1"], metadata: null },
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });
    mockFetchQualificationsByOrg.mockResolvedValue(new Map());

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(1);
    expect(res.body.statuses[0].leadId).toBe("lead-1");
  });

  it("handles email-gateway unreachable gracefully", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue(null);
    mockFetchQualificationsByOrg.mockResolvedValue(new Map());

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.status).toBe(200);
    expect(res.body.statuses[0]).toEqual({
      leadId: "lead-1",
      email: "alice@acme.com",
      journalistId: null,
      outletId: null,
      contacted: false,
      delivered: false,
      bounced: false,
      replied: false,
      replyClassification: null,
      lastDeliveredAt: null,
    });
  });

  it("maps negative reply classification correctly", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });
    mockFetchQualificationsByOrg.mockResolvedValue(
      new Map([
        [
          "alice@acme.com",
          { id: "q1", fromEmail: "alice@acme.com", classification: "not_interested", confidence: 0.9, createdAt: "2026-03-30T10:00:00Z" },
        ],
      ]),
    );

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.body.statuses[0].replyClassification).toBe("negative");
  });

  it("maps 'other' reply classifications correctly", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], metadata: null },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });
    mockFetchQualificationsByOrg.mockResolvedValue(
      new Map([
        [
          "alice@acme.com",
          { id: "q1", fromEmail: "alice@acme.com", classification: "out_of_office", confidence: 0.85, createdAt: "2026-03-30T10:00:00Z" },
        ],
      ]),
    );

    const app = createApp();
    const res = await request(app).get("/leads/outreach-status?brandId=b1");
    expect(res.body.statuses[0].replyClassification).toBe("other");
  });
});

describe("flattenBrandStatus", () => {
  it("uses brand scope (not campaign) for cross-campaign status", () => {
    const result = flattenBrandStatus({
      leadId: "l1",
      email: "a@b.com",
      broadcast: {
        campaign: {
          lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
        brand: {
          lead: { contacted: true, delivered: true, replied: true, lastDeliveredAt: null },
          email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-29T10:00:00Z" },
        },
        global: {
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
      },
    });

    expect(result.contacted).toBe(true);
    expect(result.delivered).toBe(true);
    expect(result.replied).toBe(true);
    expect(result.lastDeliveredAt).toBe("2026-03-29T10:00:00Z");
  });

  it("returns all false when no providers present", () => {
    const result = flattenBrandStatus({
      leadId: "l1",
      email: "a@b.com",
    });
    expect(result).toEqual({
      contacted: false,
      delivered: false,
      bounced: false,
      replied: false,
      lastDeliveredAt: null,
    });
  });
});
