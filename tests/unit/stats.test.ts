import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock db — where() returns whatever mockWhere returns
const mockFrom = vi.fn();
const mockWhere = vi.fn();
const mockGroupBy = vi.fn();
const mockSelect = vi.fn();
const mockExecute = vi.fn();

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
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

const mockFetchEmailGatewayStats = vi.fn();
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  fetchEmailGatewayStats: (...args: unknown[]) => mockFetchEmailGatewayStats(...args),
}));

const mockResolveFeatureDynastySlugs = vi.fn();
const mockResolveWorkflowDynastySlugs = vi.fn();
const mockFetchFeatureDynastyMap = vi.fn();
const mockFetchWorkflowDynastyMap = vi.fn();

vi.mock("../../src/lib/dynasty-client.js", () => ({
  resolveFeatureDynastySlugs: (...args: unknown[]) => mockResolveFeatureDynastySlugs(...args),
  resolveWorkflowDynastySlugs: (...args: unknown[]) => mockResolveWorkflowDynastySlugs(...args),
  fetchFeatureDynastyMap: (...args: unknown[]) => mockFetchFeatureDynastyMap(...args),
  fetchWorkflowDynastyMap: (...args: unknown[]) => mockFetchWorkflowDynastyMap(...args),
}));

vi.mock("../../src/middleware/auth.js", () => ({
  apiKeyAuth: (_req: unknown, _res: unknown, next: () => void) => next(),
  requireOrgId: (_req: unknown, _res: unknown, next: () => void) => next(),
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
import statsRouter from "../../src/routes/stats.js";

function createApp() {
  const app = express();
  app.use((req: any, _res, next) => {
    req.orgId = "org-1";
    req.userId = "user-1";
    req.runId = "run-1";
    next();
  });
  app.use(statsRouter);
  return app;
}

/** Helper: create a thenable that also has .groupBy() */
function queryResult(data: unknown[]) {
  return {
    then: (fn: (rows: unknown[]) => unknown) => Promise.resolve(fn(data)),
    groupBy: () => Promise.resolve([]),
  };
}

const ZERO_RECIPIENT_STATS = {
  contacted: 0, sent: 0, delivered: 0, opened: 0, bounced: 0, clicked: 0,
  unsubscribed: 0, repliesPositive: 0, repliesNegative: 0, repliesNeutral: 0,
  repliesAutoReply: 0,
  repliesDetail: {
    interested: 0, meetingBooked: 0, closed: 0, notInterested: 0,
    wrongPerson: 0, unsubscribe: 0, neutral: 0, autoReply: 0, outOfOffice: 0,
  },
};

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Default: all queries return zero/empty
    mockWhere.mockReturnValue(queryResult([{ count: 0 }]));
    mockFetchEmailGatewayStats.mockResolvedValue({});
    mockExecute.mockResolvedValue([]);
    // Default dynasty mocks
    mockResolveFeatureDynastySlugs.mockResolvedValue([]);
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);
    mockFetchFeatureDynastyMap.mockResolvedValue(new Map());
    mockFetchWorkflowDynastyMap.mockResolvedValue(new Map());
  });

  it("returns 400 for invalid groupBy value", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?groupBy=invalid");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("Invalid groupBy");
  });

  it("accepts groupBy=campaignId", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });
    const res = await request(app).get("/orgs/stats?groupBy=campaignId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(Array.isArray(res.body.groups)).toBe(true);
  });

  it("accepts groupBy=brandId", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });
    const res = await request(app).get("/orgs/stats?groupBy=brandId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("accepts groupBy=workflowSlug", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });
    const res = await request(app).get("/orgs/stats?groupBy=workflowSlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("accepts groupBy=featureSlug", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });
    const res = await request(app).get("/orgs/stats?groupBy=featureSlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("returns flat response with new shape (totalLeads, byOutreachStatus, repliesDetail)", async () => {
    const app = createApp();

    mockFetchEmailGatewayStats.mockResolvedValue({
      broadcast: {
        recipientStats: {
          ...ZERO_RECIPIENT_STATS,
          contacted: 5,
          sent: 10,
          delivered: 8,
        },
      },
    });

    const res = await request(app).get("/orgs/stats");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("totalLeads");
    expect(res.body).toHaveProperty("byOutreachStatus");
    expect(res.body).toHaveProperty("repliesDetail");
    expect(res.body).toHaveProperty("buffered");
    expect(res.body).toHaveProperty("skipped");
    expect(res.body).not.toHaveProperty("served");
    expect(res.body).not.toHaveProperty("contacted");
    expect(res.body).not.toHaveProperty("apollo");
    expect(res.body).not.toHaveProperty("groups");
    expect(res.body.byOutreachStatus.contacted).toBe(5);
    expect(res.body.byOutreachStatus.sent).toBe(10);
    expect(res.body.byOutreachStatus.delivered).toBe(8);
  });

  it("merges broadcast + transactional recipientStats", async () => {
    const app = createApp();

    mockFetchEmailGatewayStats.mockResolvedValue({
      broadcast: {
        recipientStats: { ...ZERO_RECIPIENT_STATS, contacted: 3, sent: 5 },
      },
      transactional: {
        recipientStats: { ...ZERO_RECIPIENT_STATS, contacted: 2, sent: 1 },
      },
    });

    const res = await request(app).get("/orgs/stats");
    expect(res.status).toBe(200);
    expect(res.body.byOutreachStatus.contacted).toBe(5);
    expect(res.body.byOutreachStatus.sent).toBe(6);
  });

  // --- Dynasty slug filtering ---

  it("filters by workflowSlug query param", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowSlug=cold-email-v2");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("filters by featureSlug query param", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?featureSlug=feat-alpha");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).not.toHaveBeenCalled();
  });

  it("resolves workflowDynastySlug to versioned slugs", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=cold-email");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalledWith("cold-email", expect.any(Object));
  });

  it("resolves featureDynastySlug to versioned slugs", async () => {
    mockResolveFeatureDynastySlugs.mockResolvedValue(["feat-alpha", "feat-alpha-v2"]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?featureDynastySlug=feat-alpha");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).toHaveBeenCalledWith("feat-alpha", expect.any(Object));
  });

  it("returns zero stats when workflowDynastySlug resolves to empty list", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=nonexistent");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      totalLeads: 0,
      byOutreachStatus: ZERO_RECIPIENT_STATS,
      repliesDetail: ZERO_RECIPIENT_STATS.repliesDetail,
      buffered: 0,
      skipped: 0,
    });
    expect(mockWhere).not.toHaveBeenCalled();
  });

  it("returns zero stats when featureDynastySlug resolves to empty list", async () => {
    mockResolveFeatureDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?featureDynastySlug=nonexistent");
    expect(res.status).toBe(200);
    expect(res.body.totalLeads).toBe(0);
    expect(mockWhere).not.toHaveBeenCalled();
  });

  it("returns empty groups for grouped empty dynasty", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=nonexistent&groupBy=campaignId");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ groups: [] });
  });

  // --- Multi-slug filtering ---

  it("filters by comma-separated workflowSlugs", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowSlugs=cold-email-v1,cold-email-v2");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("filters by comma-separated featureSlugs", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?featureSlugs=feat-a,feat-b");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).not.toHaveBeenCalled();
  });

  it("workflowSlugs takes priority over workflowSlug", async () => {
    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowSlugs=v1,v2&workflowSlug=v3");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("workflowDynastySlug takes priority over workflowSlugs", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=cold-email&workflowSlugs=v1,v2");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalledWith("cold-email", expect.any(Object));
  });

  it("groupBy=workflowSlug with workflowSlugs filter returns grouped stats", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });
    const res = await request(app).get("/orgs/stats?groupBy=workflowSlug&workflowSlugs=slug1,slug2");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("workflowDynastySlug takes priority over workflowSlug", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=cold-email&workflowSlug=cold-email-v2");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalledWith("cold-email", expect.any(Object));
  });

  // --- Dynasty groupBy ---

  it("accepts groupBy=workflowDynastySlug and fetches dynasty map", async () => {
    mockFetchWorkflowDynastyMap.mockResolvedValue(
      new Map([
        ["cold-email", "cold-email"],
        ["cold-email-v2", "cold-email"],
      ]),
    );
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });

    const app = createApp();
    const res = await request(app).get("/orgs/stats?groupBy=workflowDynastySlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(mockFetchWorkflowDynastyMap).toHaveBeenCalled();
  });

  it("accepts groupBy=featureDynastySlug and fetches dynasty map", async () => {
    mockFetchFeatureDynastyMap.mockResolvedValue(
      new Map([
        ["feat-alpha", "feat-alpha"],
        ["feat-alpha-v2", "feat-alpha"],
      ]),
    );
    mockFetchEmailGatewayStats.mockResolvedValue({ groups: [] });

    const app = createApp();
    const res = await request(app).get("/orgs/stats?groupBy=featureDynastySlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(mockFetchFeatureDynastyMap).toHaveBeenCalled();
  });

  it("combines workflowDynastySlug filter with other filters", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/orgs/stats?workflowDynastySlug=cold-email&brandId=b1&campaignId=c1");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalled();
  });

  it("calls fetchEmailGatewayStats instead of N+1 checkDeliveryStatus", async () => {
    const app = createApp();
    mockFetchEmailGatewayStats.mockResolvedValue({
      broadcast: { recipientStats: ZERO_RECIPIENT_STATS },
    });

    const res = await request(app).get("/orgs/stats");
    expect(res.status).toBe(200);
    expect(mockFetchEmailGatewayStats).toHaveBeenCalledOnce();
  });
});
