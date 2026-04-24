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
        brandId: "not-a-uuid",
        sourceOrgId: "also-not",
        targetOrgId: "nope",
      });

    expect(res.status).toBe(400);
  });

  it("updates both tables and returns counts", async () => {
    // First call (served_leads) returns 2 rows, second (lead_buffer) returns 1
    mockReturning
      .mockResolvedValueOnce([{ id: "a" }, { id: "b" }])
      .mockResolvedValueOnce([{ id: "c" }]);

    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({
        brandId: "11111111-1111-4111-a111-111111111111",
        sourceOrgId: "22222222-2222-4222-a222-222222222222",
        targetOrgId: "33333333-3333-4333-a333-333333333333",
      });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "served_leads", count: 2 },
      { tableName: "lead_buffer", count: 1 },
    ]);

    // db.update called twice (once per table)
    expect(mockUpdate).toHaveBeenCalledTimes(2);

    // .set() called with targetOrgId both times
    expect(mockSet).toHaveBeenCalledWith({
      orgId: "33333333-3333-4333-a333-333333333333",
    });
  });

  it("is idempotent — returns 0 counts when already transferred", async () => {
    mockReturning
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    const app = buildApp();
    const res = await request(app)
      .post("/internal/transfer-brand")
      .send({
        brandId: "11111111-1111-4111-a111-111111111111",
        sourceOrgId: "22222222-2222-4222-a222-222222222222",
        targetOrgId: "33333333-3333-4333-a333-333333333333",
      });

    expect(res.status).toBe(200);
    expect(res.body.updatedTables).toEqual([
      { tableName: "served_leads", count: 0 },
      { tableName: "lead_buffer", count: 0 },
    ]);
  });
});
