import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Response } from "express";

// Mock db — where() returns whatever mockWhere returns
const mockFrom = vi.fn();
const mockWhere = vi.fn();
const mockGroupBy = vi.fn();
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

vi.mock("../../src/lib/apollo-client.js", () => ({
  fetchApolloStats: vi.fn().mockResolvedValue({
    enrichedLeadsCount: 0,
    searchCount: 0,
    fetchedPeopleCount: 0,
    totalMatchingPeople: 0,
  }),
}));

const mockCheckDeliveryStatus = vi.fn();
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (...args: unknown[]) => mockCheckDeliveryStatus(...args),
  isContacted: (result: { broadcast?: { campaign: { lead: { contacted: boolean } } } }) =>
    !!(result.broadcast?.campaign.lead.contacted),
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

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Default: all queries return zero/empty
    mockWhere.mockReturnValue(queryResult([{ count: 0 }]));
    mockCheckDeliveryStatus.mockResolvedValue(null);
  });

  it("returns 400 for invalid groupBy value", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=invalid");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("Invalid groupBy");
  });

  it("accepts groupBy=campaignId", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=campaignId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(Array.isArray(res.body.groups)).toBe(true);
  });

  it("accepts groupBy=brandId", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=brandId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("returns flat response without groupBy including contacted=0", async () => {
    const app = createApp();
    const res = await request(app).get("/stats");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("served");
    expect(res.body).toHaveProperty("contacted");
    expect(res.body).toHaveProperty("buffered");
    expect(res.body).toHaveProperty("skipped");
    expect(res.body).toHaveProperty("apollo");
    expect(res.body).not.toHaveProperty("groups");
    expect(res.body.contacted).toBe(0);
  });

  it("counts contacted leads via email-gateway", async () => {
    const app = createApp();

    const servedLeadRows = [
      { leadId: "lead-1", email: "alice@acme.com", brandId: "b1", campaignId: "c1" },
      { leadId: "lead-2", email: "bob@acme.com", brandId: "b1", campaignId: "c1" },
    ];

    // Flat response Promise.all order:
    //   1. served count: .where().then(([r]) => r)
    //   2. countContacted: await .where()
    //   3. buffer: .where().groupBy()
    let whereCall = 0;
    mockWhere.mockImplementation(() => {
      whereCall++;
      const c = whereCall;
      if (c === 1) return queryResult([{ count: 2 }]);
      if (c === 2) return queryResult(servedLeadRows);
      return queryResult([]);
    });

    mockCheckDeliveryStatus.mockResolvedValue({
      results: [
        {
          leadId: "lead-1",
          email: "alice@acme.com",
          broadcast: {
            campaign: { lead: { contacted: true }, email: { contacted: true } },
            brand: { lead: { contacted: false }, email: { contacted: false } },
            global: { email: { contacted: false } },
          },
        },
        {
          leadId: "lead-2",
          email: "bob@acme.com",
          broadcast: {
            campaign: { lead: { contacted: false }, email: { contacted: false } },
            brand: { lead: { contacted: false }, email: { contacted: false } },
            global: { email: { contacted: false } },
          },
        },
      ],
    });

    const res = await request(app).get("/stats");
    expect(res.status).toBe(200);
    expect(res.body.served).toBe(2);
    expect(res.body.contacted).toBe(1);
    expect(mockCheckDeliveryStatus).toHaveBeenCalledWith(
      "b1", "c1",
      [{ leadId: "lead-1", email: "alice@acme.com" }, { leadId: "lead-2", email: "bob@acme.com" }],
      expect.objectContaining({ orgId: "org-1" }),
    );
  });

  it("returns contacted=0 when email-gateway is unreachable", async () => {
    const app = createApp();

    let whereCall = 0;
    mockWhere.mockImplementation(() => {
      whereCall++;
      if (whereCall === 1) return queryResult([{ count: 1 }]);
      if (whereCall === 2) {
        return queryResult([
          { leadId: "lead-1", email: "alice@acme.com", brandId: "b1", campaignId: "c1" },
        ]);
      }
      return queryResult([]);
    });

    mockCheckDeliveryStatus.mockResolvedValue(null);

    const res = await request(app).get("/stats");
    expect(res.status).toBe(200);
    expect(res.body.served).toBe(1);
    expect(res.body.contacted).toBe(0);
  });
});
