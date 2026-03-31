import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Response } from "express";

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
    mockExecute.mockResolvedValue([]);
    // Default dynasty mocks
    mockResolveFeatureDynastySlugs.mockResolvedValue([]);
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);
    mockFetchFeatureDynastyMap.mockResolvedValue(new Map());
    mockFetchWorkflowDynastyMap.mockResolvedValue(new Map());
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

  it("accepts groupBy=workflowSlug", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=workflowSlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("accepts groupBy=featureSlug", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=featureSlug");
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
      { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], campaignId: "c1" },
      { leadId: "lead-2", email: "bob@acme.com", brandIds: ["b1"], campaignId: "c1" },
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
          { leadId: "lead-1", email: "alice@acme.com", brandIds: ["b1"], campaignId: "c1" },
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

  // --- Dynasty slug filtering ---

  it("filters by workflowSlug query param", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?workflowSlug=cold-email-v2");
    expect(res.status).toBe(200);
    // Verify dynasty resolver was NOT called (exact slug, not dynasty)
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("filters by featureSlug query param", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?featureSlug=feat-alpha");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).not.toHaveBeenCalled();
  });

  it("resolves workflowDynastySlug to versioned slugs", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=cold-email");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalledWith("cold-email", expect.any(Object));
  });

  it("resolves featureDynastySlug to versioned slugs", async () => {
    mockResolveFeatureDynastySlugs.mockResolvedValue(["feat-alpha", "feat-alpha-v2"]);

    const app = createApp();
    const res = await request(app).get("/stats?featureDynastySlug=feat-alpha");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).toHaveBeenCalledWith("feat-alpha", expect.any(Object));
  });

  it("returns zero stats when workflowDynastySlug resolves to empty list", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=nonexistent");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      served: 0,
      contacted: 0,
      buffered: 0,
      skipped: 0,
      apollo: { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 },
    });
    // Should NOT hit the database
    expect(mockWhere).not.toHaveBeenCalled();
  });

  it("returns zero stats when featureDynastySlug resolves to empty list", async () => {
    mockResolveFeatureDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/stats?featureDynastySlug=nonexistent");
    expect(res.status).toBe(200);
    expect(res.body.served).toBe(0);
    expect(mockWhere).not.toHaveBeenCalled();
  });

  it("returns empty groups for grouped empty dynasty", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue([]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=nonexistent&groupBy=campaignId");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ groups: [] });
  });

  // --- Multi-slug filtering ---

  it("filters by comma-separated workflowSlugs", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?workflowSlugs=cold-email-v1,cold-email-v2");
    expect(res.status).toBe(200);
    // Dynasty resolver should NOT be called
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("filters by comma-separated featureSlugs", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?featureSlugs=feat-a,feat-b");
    expect(res.status).toBe(200);
    expect(mockResolveFeatureDynastySlugs).not.toHaveBeenCalled();
  });

  it("workflowSlugs takes priority over workflowSlug", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?workflowSlugs=v1,v2&workflowSlug=v3");
    expect(res.status).toBe(200);
    // Should use the plural param, not the singular
    expect(mockResolveWorkflowDynastySlugs).not.toHaveBeenCalled();
  });

  it("workflowDynastySlug takes priority over workflowSlugs", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=cold-email&workflowSlugs=v1,v2");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalledWith("cold-email", expect.any(Object));
  });

  it("groupBy=workflowSlug with workflowSlugs filter returns grouped stats", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=workflowSlug&workflowSlugs=slug1,slug2");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("workflowDynastySlug takes priority over workflowSlug", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=cold-email&workflowSlug=cold-email-v2");
    expect(res.status).toBe(200);
    // Dynasty resolver should be called (takes priority)
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

    const app = createApp();
    const res = await request(app).get("/stats?groupBy=workflowDynastySlug");
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

    const app = createApp();
    const res = await request(app).get("/stats?groupBy=featureDynastySlug");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(mockFetchFeatureDynastyMap).toHaveBeenCalled();
  });

  it("combines workflowDynastySlug filter with other filters", async () => {
    mockResolveWorkflowDynastySlugs.mockResolvedValue(["cold-email", "cold-email-v2"]);

    const app = createApp();
    const res = await request(app).get("/stats?workflowDynastySlug=cold-email&brandId=b1&campaignId=c1");
    expect(res.status).toBe(200);
    expect(mockResolveWorkflowDynastySlugs).toHaveBeenCalled();
  });
});
