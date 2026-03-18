import { describe, it, expect, vi, beforeEach } from "vitest";
import type { Response } from "express";

// Mock db
const mockFrom = vi.fn();
const mockWhere = vi.fn();
const mockGroupBy = vi.fn();
const mockSelect = vi.fn();
const mockThen = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: {
    select: (...args: unknown[]) => {
      mockSelect(...args);
      return {
        from: (...fArgs: unknown[]) => {
          mockFrom(...fArgs);
          return {
            where: (...wArgs: unknown[]) => {
              mockWhere(...wArgs);
              return {
                then: mockThen,
                groupBy: (...gArgs: unknown[]) => {
                  mockGroupBy(...gArgs);
                  return Promise.resolve([]);
                },
              };
            },
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

vi.mock("../../src/middleware/auth.js", () => ({
  authenticate: (_req: unknown, _res: unknown, next: () => void) => next(),
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

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Default: flat query returns served=0, buffer=[]
    mockThen.mockImplementation((fn: (rows: unknown[]) => unknown) =>
      Promise.resolve(fn([{ count: 0 }])),
    );
  });

  it("returns 400 for invalid groupBy value", async () => {
    const app = createApp();
    const res = await request(app).get("/stats?groupBy=invalid");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("Invalid groupBy");
  });

  it("accepts groupBy=campaignId", async () => {
    const app = createApp();
    // Mock grouped queries to return empty arrays
    mockGroupBy.mockReturnValue(Promise.resolve([]));
    mockThen.mockImplementation((fn: (rows: unknown[]) => unknown) =>
      Promise.resolve(fn([{ count: 0 }])),
    );

    const res = await request(app).get("/stats?groupBy=campaignId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
    expect(Array.isArray(res.body.groups)).toBe(true);
  });

  it("accepts groupBy=brandId", async () => {
    const app = createApp();
    mockGroupBy.mockReturnValue(Promise.resolve([]));

    const res = await request(app).get("/stats?groupBy=brandId");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("groups");
  });

  it("returns flat response without groupBy", async () => {
    const app = createApp();
    const res = await request(app).get("/stats");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("served");
    expect(res.body).toHaveProperty("contacted");
    expect(res.body).toHaveProperty("buffered");
    expect(res.body).toHaveProperty("skipped");
    expect(res.body).toHaveProperty("apollo");
    expect(res.body).not.toHaveProperty("groups");
  });
});
