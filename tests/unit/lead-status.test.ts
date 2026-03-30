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
import { flattenStatus } from "../../src/routes/lead-status.js";

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

describe("GET /leads/status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockWhere.mockResolvedValue([]);
    mockCheckDeliveryStatus.mockResolvedValue(null);
  });

  it("returns 400 when campaignId is missing", async () => {
    const app = createApp();
    const res = await request(app).get("/leads/status");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("campaignId");
  });

  it("returns empty statuses when no served leads exist", async () => {
    mockWhere.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ statuses: [] });
    expect(mockCheckDeliveryStatus).not.toHaveBeenCalled();
  });

  it("returns per-lead delivery status from email-gateway", async () => {
    const servedRows = [
      { leadId: "lead-1", email: "alice@acme.com", brandId: "b1" },
      { leadId: "lead-2", email: "bob@acme.com", brandId: "b1" },
    ];
    mockWhere.mockResolvedValue(servedRows);

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadId: "lead-1",
          email: "alice@acme.com",
          broadcast: {
            campaign: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: null },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-29T10:00:00Z" },
            },
            brand: {
              lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
              email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
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
              lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
              email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
            },
            global: {
              email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
            },
          },
        },
      ],
    });

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(2);

    const alice = res.body.statuses.find((s: any) => s.email === "alice@acme.com");
    expect(alice).toEqual({
      leadId: "lead-1",
      email: "alice@acme.com",
      contacted: true,
      delivered: true,
      bounced: false,
      replied: false,
      lastDeliveredAt: "2026-03-29T10:00:00Z",
    });

    const bob = res.body.statuses.find((s: any) => s.email === "bob@acme.com");
    expect(bob).toEqual({
      leadId: "lead-2",
      email: "bob@acme.com",
      contacted: true,
      delivered: false,
      bounced: true,
      replied: false,
      lastDeliveredAt: null,
    });

    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1",
      "c1",
      [
        { leadId: "lead-1", email: "alice@acme.com" },
        { leadId: "lead-2", email: "bob@acme.com" },
      ],
      expect.objectContaining({ orgId: "org-1" }),
    );
  });

  it("returns all-false status when email-gateway is unreachable", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandId: "b1" },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue(null);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toEqual([
      {
        leadId: "lead-1",
        email: "alice@acme.com",
        contacted: false,
        delivered: false,
        bounced: false,
        replied: false,
        lastDeliveredAt: null,
      },
    ]);
  });

  it("skips rows with null leadId", async () => {
    mockWhere.mockResolvedValue([
      { leadId: null, email: "orphan@acme.com", brandId: "b1" },
      { leadId: "lead-1", email: "alice@acme.com", brandId: "b1" },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue(null);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1");
    expect(res.status).toBe(200);
    expect(res.body.statuses).toHaveLength(1);
    expect(res.body.statuses[0].leadId).toBe("lead-1");
  });

  it("groups email-gateway calls by brandId", async () => {
    mockWhere.mockResolvedValue([
      { leadId: "lead-1", email: "alice@acme.com", brandId: "b1" },
      { leadId: "lead-2", email: "bob@other.com", brandId: "b2" },
    ]);
    mockCheckDeliveryStatus.mockResolvedValue({ results: [] });

    const app = createApp();
    await request(app).get("/leads/status?campaignId=c1");

    expect(mockCheckDeliveryStatus).toHaveBeenCalledTimes(2);
    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1", "c1",
      [{ leadId: "lead-1", email: "alice@acme.com" }],
      expect.any(Object),
    );
    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b2", "c1",
      [{ leadId: "lead-2", email: "bob@other.com" }],
      expect.any(Object),
    );
  });

  it("applies brandId filter when provided", async () => {
    mockWhere.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/leads/status?campaignId=c1&brandId=b1");
    expect(res.status).toBe(200);
    // Verify where was called (conditions include brandId filter)
    expect(mockWhere).toHaveBeenCalled();
  });
});

describe("flattenStatus", () => {
  it("detects replied from broadcast campaign lead", () => {
    const result = flattenStatus({
      leadId: "l1",
      email: "a@b.com",
      broadcast: {
        campaign: {
          lead: { contacted: true, delivered: true, replied: true, lastDeliveredAt: null },
          email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-29T10:00:00Z" },
        },
        brand: {
          lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
        global: {
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
      },
    });

    expect(result.replied).toBe(true);
    expect(result.delivered).toBe(true);
    expect(result.contacted).toBe(true);
    expect(result.lastDeliveredAt).toBe("2026-03-29T10:00:00Z");
  });

  it("detects transactional delivery", () => {
    const result = flattenStatus({
      leadId: "l1",
      email: "a@b.com",
      transactional: {
        campaign: {
          lead: { contacted: true, delivered: false, replied: false, lastDeliveredAt: null },
          email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-28T08:00:00Z" },
        },
        brand: {
          lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
          email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
        },
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
    const result = flattenStatus({
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
