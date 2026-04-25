import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockUpdate = vi.fn();
const mockSet = vi.fn();
const mockWhere = vi.fn();
const mockReturning = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: {
    update: (...args: any[]) => mockUpdate(...args),
  },
  sql: vi.fn(),
}));

vi.mock("../../src/middleware/auth.js", () => ({
  apiKeyAuth: (_req: any, _res: any, next: any) => next(),
  AuthenticatedRequest: {},
}));

// Dynamic import after mocks
const { default: transferBrandRoutes } = await import(
  "../../src/routes/transfer-brand.js"
);

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use(transferBrandRoutes);
  return app;
}

const VALID_BODY = {
  sourceBrandId: "11111111-1111-4111-a111-111111111111",
  sourceOrgId: "22222222-2222-4222-a222-222222222222",
  targetOrgId: "33333333-3333-4333-a333-333333333333",
};

describe("POST /internal/transfer-brand", () => {
  beforeEach(() => {
    mockUpdate.mockReset();
    mockSet.mockReset();
    mockWhere.mockReset();
    mockReturning.mockReset();

    // Chain: db.update(table).set(values).where(cond).returning(cols)
    mockReturning.mockResolvedValue([]);
    mockWhere.mockReturnValue({ returning: mockReturning });
    mockSet.mockReturnValue({ where: mockWhere });
    mockUpdate.mockReturnValue({ set: mockSet });
  });

  it("returns 400 for missing body fields", async () => {
    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({});

    expect(res.status).toBe(400);
  });

  it("returns 400 for invalid UUIDs", async () => {
    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({
        sourceBrandId: "not-a-uuid",
        sourceOrgId: "also-not",
        targetOrgId: "nope",
      });

    expect(res.status).toBe(400);
  });

  it("returns 400 for invalid targetBrandId", async () => {
    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({ ...VALID_BODY, targetBrandId: "not-a-uuid" });

    expect(res.status).toBe(400);
  });

  it("updates both tables and returns counts (no targetBrandId)", async () => {
    mockReturning
      .mockResolvedValueOnce([{ id: "a" }, { id: "b" }])
      .mockResolvedValueOnce([{ id: "c" }]);

    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send(VALID_BODY);

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "served_leads", count: 2 },
      { tableName: "lead_buffer", count: 1 },
    ]);

    // db.update called twice (once per table)
    expect(mockUpdate).toHaveBeenCalledTimes(2);

    // .set() called with only orgId (no brand rewrite)
    expect(mockSet).toHaveBeenCalledWith({
      orgId: "33333333-3333-4333-a333-333333333333",
    });
  });

  it("rewrites brand_ids when targetBrandId is present", async () => {
    mockReturning
      .mockResolvedValueOnce([{ id: "a" }])
      .mockResolvedValueOnce([]);

    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({
        ...VALID_BODY,
        targetBrandId: "44444444-4444-4444-a444-444444444444",
      });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "served_leads", count: 1 },
      { tableName: "lead_buffer", count: 0 },
    ]);

    // .set() called with orgId AND brandIds
    expect(mockSet).toHaveBeenCalledWith(
      expect.objectContaining({
        orgId: "33333333-3333-4333-a333-333333333333",
        brandIds: expect.anything(),
      })
    );
  });

  it("is idempotent — returns 0 counts when already transferred", async () => {
    mockReturning
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send(VALID_BODY);

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "served_leads", count: 0 },
      { tableName: "lead_buffer", count: 0 },
    ]);
  });
});
