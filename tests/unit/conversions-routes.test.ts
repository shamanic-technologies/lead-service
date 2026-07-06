import { describe, it, expect, vi, beforeAll, beforeEach } from "vitest";
import express from "express";
import request from "supertest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

const execute = vi.fn();
const matchConversion = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: { execute: (...args: unknown[]) => execute(...args) },
}));

vi.mock("../../src/config.js", () => ({
  LEAD_SERVICE_API_KEY: "test-api-key",
  CONVERSION_INGEST_URL: "https://api.distribute.you/public/conversions",
}));

// Keep the pure helpers real; only stub the DB-backed waterfall.
vi.mock("../../src/lib/conversions.js", async (orig) => {
  const actual = (await orig()) as Record<string, unknown>;
  return { ...actual, matchConversion: (...a: unknown[]) => matchConversion(...a) };
});

const dialect = new PgDialect();
function compile(call: unknown): { sql: string; params: unknown[] } {
  return dialect.sqlToQuery(call as SQL);
}
function lastSql(): string {
  const call = execute.mock.calls[execute.mock.calls.length - 1][0];
  return compile(call).sql.toLowerCase();
}
function sqlAt(i: number): string {
  return compile(execute.mock.calls[i][0]).sql.toLowerCase();
}

async function buildApp() {
  const { default: route } = await import("../../src/routes/conversions.js");
  const app = express();
  app.use(express.json());
  app.use(route);
  // Mirror index.ts 500 handler so wrapped async rejections surface as 500.
  app.use((_err: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    res.status(500).json({ error: "Internal server error" });
  });
  return app;
}

describe("POST /public/conversions", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => {
    execute.mockReset().mockResolvedValue([]);
    matchConversion.mockReset();
  });

  it("401 on missing token", async () => {
    const res = await request(app).post("/public/conversions").send({ event: "signup" });
    expect(res.status).toBe(401);
    expect(execute).not.toHaveBeenCalled();
  });

  it("401 on unknown token (token lookup empty)", async () => {
    execute.mockResolvedValueOnce([]); // token lookup → no row
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_nope")
      .send({ event: "signup", email: "x@y.com" });
    expect(res.status).toBe(401);
  });

  it("400 on missing/invalid event", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]); // token ok
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ email: "x@y.com" });
    expect(res.status).toBe(400);
    expect(matchConversion).not.toHaveBeenCalled();
  });

  it("valid email match → 200 {received:true}, stores attributed/deterministic", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]); // token
    execute.mockResolvedValueOnce([]); // dedupe check → no dup
    execute.mockResolvedValueOnce([]); // insert
    matchConversion.mockResolvedValueOnce({
      matchedLeadId: "lead-1",
      matchMethod: "email",
      matchConfidence: "deterministic",
      attributionStatus: "attributed",
      candidateCount: 1,
    });

    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ event: "signup", email: "Jane@Acme.com" });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ received: true });
    // last execute = insert into conversion_events with the attribution
    expect(lastSql()).toContain("insert into conversion_events");
    const insertParams = compile(execute.mock.calls[2][0]).params;
    expect(insertParams).toContain("attributed");
    expect(insertParams).toContain("deterministic");
    expect(insertParams).toContain("lead-1");
  });

  it("accepts Authorization: Bearer token", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]);
    matchConversion.mockResolvedValueOnce({
      matchedLeadId: null,
      matchMethod: "last_name",
      matchConfidence: "probabilistic",
      attributionStatus: "needs_review",
      candidateCount: 1,
    });
    const res = await request(app)
      .post("/public/conversions")
      .set("Authorization", "Bearer pk_conv_ok")
      .send({ event: "signup", lastName: "Doe" });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ received: true });
  });

  it("lastName-only match stores attributed (name is enough), NOT needs_review", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]); // token
    execute.mockResolvedValueOnce([]); // dedupe (signature null → actually skipped; see below)
    matchConversion.mockResolvedValueOnce({
      matchedLeadId: "lead-7",
      matchMethod: "last_name",
      matchConfidence: "probabilistic",
      attributionStatus: "attributed",
      candidateCount: 2,
    });
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ event: "signup", lastName: "Doe" });
    expect(res.status).toBe(200);
    // no email/phone/dedupeKey → dedupe signature is null → no dedupe SELECT, straight to insert.
    expect(lastSql()).toContain("insert into conversion_events");
    const insertParams = compile(execute.mock.calls[execute.mock.calls.length - 1][0]).params;
    expect(insertParams).toContain("attributed");
    expect(insertParams).toContain("lead-7");
    expect(insertParams).not.toContain("needs_review");
  });

  it("duplicate dedupeKey → 200, no second attribution insert", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]); // token
    execute.mockResolvedValueOnce([{ "1": 1 }]); // dedupe check → existing row found
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ event: "signup", email: "x@y.com", dedupeKey: "dup-1" });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ received: true });
    expect(matchConversion).not.toHaveBeenCalled();
    // only token lookup + dedupe check ran; NO insert.
    expect(execute).toHaveBeenCalledTimes(2);
    expect(sqlAt(1)).toContain("from conversion_events");
    expect(sqlAt(1)).not.toContain("insert");
  });

  it("500 when the DB errors (fail loud, not a hung socket)", async () => {
    execute.mockRejectedValueOnce(new Error("db down")); // token lookup throws
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ event: "signup", email: "x@y.com" });
    expect(res.status).toBe(500);
  });

  it("ping heartbeat → 200 {received:true}, stamps last_ping_at, NO attribution/insert", async () => {
    execute.mockResolvedValueOnce([{ brand_id: "brand-1", org_id: "org-1" }]); // token
    execute.mockResolvedValueOnce([]); // update last_ping_at
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_ok")
      .send({ event: "ping" });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ received: true });
    // ping never runs the match waterfall and never touches conversion_events.
    expect(matchConversion).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(2); // token lookup + last_ping_at update only
    const updateSql = lastSql();
    expect(updateSql).toContain("update brand_conversion_tokens");
    expect(updateSql).toContain("last_ping_at");
    expect(updateSql).not.toContain("conversion_events");
  });

  it("ping still requires a valid token (401, no update)", async () => {
    execute.mockResolvedValueOnce([]); // token lookup → no row
    const res = await request(app)
      .post("/public/conversions")
      .set("x-conversion-token", "pk_conv_nope")
      .send({ event: "ping" });
    expect(res.status).toBe(401);
    expect(execute).toHaveBeenCalledTimes(1); // only the token lookup
  });
});

describe("GET /orgs/brands/:brandId/conversion-token", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => execute.mockReset().mockResolvedValue([]));

  it("401 without x-api-key", async () => {
    const res = await request(app).get("/orgs/brands/brand-1/conversion-token");
    expect(res.status).toBe(401);
  });

  it("400 without x-org-id", async () => {
    const res = await request(app)
      .get("/orgs/brands/brand-1/conversion-token")
      .set("x-api-key", "test-api-key");
    expect(res.status).toBe(400);
  });

  it("nothing received → not_set_up, both timestamps null, eventTypesSeen []", async () => {
    execute.mockResolvedValueOnce([{ token: "pk_conv_existing", last_ping_at: null }]); // upsert
    execute.mockResolvedValueOnce([{ last_event_at: null, event_types: null }]); // agg
    const res = await request(app)
      .get("/orgs/brands/brand-1/conversion-token")
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      token: "pk_conv_existing",
      ingestUrl: "https://api.distribute.you/public/conversions",
      status: "not_set_up",
      lastEventAt: null,
      lastPingAt: null,
      eventTypesSeen: [],
    });
    expect(sqlAt(0)).toContain("insert into brand_conversion_tokens");
    expect(sqlAt(0)).toContain("on conflict");
    expect(sqlAt(0)).not.toContain("excluded.token"); // GET must NOT replace the token
    expect(sqlAt(0)).toContain("last_ping_at"); // RETURNING now carries the ping time
    expect(sqlAt(1)).toContain("from conversion_events"); // liveness overlay aggregate
  });

  it("ping received, no real conversion → live_waiting, lastPingAt set, eventTypesSeen still []", async () => {
    execute.mockResolvedValueOnce([
      { token: "pk_conv_existing", last_ping_at: new Date("2026-07-06T11:59:00.000Z") },
    ]);
    execute.mockResolvedValueOnce([{ last_event_at: null, event_types: null }]);
    const res = await request(app)
      .get("/orgs/brands/brand-1/conversion-token")
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
    expect(res.status).toBe(200);
    expect(res.body.status).toBe("live_waiting");
    expect(res.body.lastPingAt).toBe("2026-07-06T11:59:00.000Z");
    expect(res.body.lastEventAt).toBeNull();
    expect(res.body.eventTypesSeen).toEqual([]);
  });

  it("real conversion received → live, lastEventAt set, eventTypesSeen has signup (never ping)", async () => {
    execute.mockResolvedValueOnce([
      { token: "pk_conv_existing", last_ping_at: new Date("2026-07-06T11:59:00.000Z") },
    ]);
    execute.mockResolvedValueOnce([
      { last_event_at: "2026-07-06T12:00:00.000Z", event_types: ["signup"] },
    ]);
    const res = await request(app)
      .get("/orgs/brands/brand-1/conversion-token")
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
    expect(res.status).toBe(200);
    expect(res.body.status).toBe("live");
    expect(res.body.lastEventAt).toBe("2026-07-06T12:00:00.000Z");
    expect(res.body.eventTypesSeen).toEqual(["signup"]);
    expect(res.body.eventTypesSeen).not.toContain("ping");
  });
});

describe("POST /orgs/brands/:brandId/conversion-token/rotate", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => execute.mockReset().mockResolvedValue([]));

  it("rotate replaces the token (EXCLUDED.token) and returns the new one", async () => {
    execute.mockResolvedValueOnce([{ token: "pk_conv_new" }]);
    const res = await request(app)
      .post("/orgs/brands/brand-1/conversion-token/rotate")
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
    expect(res.status).toBe(200);
    expect(res.body.token).toBe("pk_conv_new");
    expect(res.body.ingestUrl).toBe("https://api.distribute.you/public/conversions");
    expect(sqlAt(0)).toContain("excluded.token"); // rotate DOES replace
    expect(sqlAt(0)).toContain("rotated_at");
  });
});
