import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const { mockCreateRun, mockUpdateRun, mockPullNext } = vi.hoisted(() => ({
  mockCreateRun: vi.fn().mockResolvedValue({ id: "mock-run-id" }),
  mockUpdateRun: vi.fn().mockResolvedValue(undefined),
  mockPullNext: vi.fn(),
}));

vi.mock("../../src/lib/runs-client.js", () => ({
  createRun: mockCreateRun,
  updateRun: mockUpdateRun,
  addCosts: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../../src/lib/buffer.js", () => ({
  pullNext: mockPullNext,
}));

vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
      idempotencyCache: { findFirst: vi.fn().mockResolvedValue(null) },
    },
    insert: vi.fn().mockReturnValue({
      values: vi.fn().mockResolvedValue([]),
    }),
    delete: vi.fn().mockReturnValue({
      where: vi.fn().mockResolvedValue([]),
    }),
  },
  sql: vi.fn(),
}));

vi.mock("../../src/middleware/auth.js", () => ({
  authenticate: (_req: any, _res: any, next: any) => {
    _req.orgId = "test-org";
    _req.userId = "test-user";
    _req.runId = "parent-run";
    _req.campaignId = _req.headers["x-campaign-id"];
    _req.brandId = _req.headers["x-brand-id"];
    next();
  },
  AuthenticatedRequest: {},
}));

vi.mock("../../src/db/schema.js", () => ({
  idempotencyCache: { idempotencyKey: "idempotency_key", createdAt: "created_at" },
}));

import bufferRoutes from "../../src/routes/buffer.js";

function createApp() {
  const app = express();
  app.use(express.json());
  app.use(bufferRoutes);
  return app;
}

describe("POST /buffer/next run status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCreateRun.mockResolvedValue({ id: "mock-run-id" });
    mockUpdateRun.mockResolvedValue(undefined);
  });

  it("marks run as failed when pullNext returns found: false", async () => {
    mockPullNext.mockResolvedValue({ found: false });

    const app = createApp();
    const res = await request(app)
      .post("/buffer/next")
      .set("x-campaign-id", "c1")
      .set("x-brand-id", "b1")
      .send({ sourceType: "apollo" });

    expect(res.status).toBe(200);
    expect(res.body.found).toBe(false);
    expect(mockUpdateRun).toHaveBeenCalledWith(
      "mock-run-id",
      "failed",
      expect.objectContaining({ campaignId: "c1", brandId: "b1" })
    );
  });

  it("marks run as completed when pullNext returns found: true", async () => {
    mockPullNext.mockResolvedValue({
      found: true,
      lead: { leadId: "lid", email: "a@b.com", externalId: null, data: {}, brandId: "b1", orgId: "test-org", userId: null },
    });

    const app = createApp();
    const res = await request(app)
      .post("/buffer/next")
      .set("x-campaign-id", "c1")
      .set("x-brand-id", "b1")
      .send({ sourceType: "apollo" });

    expect(res.status).toBe(200);
    expect(res.body.found).toBe(true);
    expect(mockUpdateRun).toHaveBeenCalledWith(
      "mock-run-id",
      "completed",
      expect.objectContaining({ campaignId: "c1", brandId: "b1" })
    );
  });

  it("marks run as failed when pullNext throws", async () => {
    mockPullNext.mockRejectedValue(new Error("DB down"));

    const app = createApp();
    const res = await request(app)
      .post("/buffer/next")
      .set("x-campaign-id", "c1")
      .set("x-brand-id", "b1")
      .send({ sourceType: "apollo" });

    expect(res.status).toBe(500);
    expect(mockUpdateRun).toHaveBeenCalledWith(
      "mock-run-id",
      "failed",
      expect.objectContaining({ campaignId: "c1", brandId: "b1" })
    );
  });
});
