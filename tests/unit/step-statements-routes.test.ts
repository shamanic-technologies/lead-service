import { describe, it, expect, vi, beforeAll, beforeEach } from "vitest";
import express from "express";
import request from "supertest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

const execute = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: { execute: (...args: unknown[]) => execute(...args) },
}));

vi.mock("../../src/config.js", () => ({
  LEAD_SERVICE_API_KEY: "test-api-key",
  CONVERSION_INGEST_URL: "https://api.distribute.you/public/conversions",
}));

const dialect = new PgDialect();
function compile(call: unknown): { sql: string; params: unknown[] } {
  return dialect.sqlToQuery(call as SQL);
}
function sqlAt(i: number): string {
  return compile(execute.mock.calls[i][0]).sql.toLowerCase();
}
function paramsAt(i: number): unknown[] {
  return compile(execute.mock.calls[i][0]).params;
}
function allSql(): string {
  return execute.mock.calls.map((c) => compile(c[0]).sql.toLowerCase()).join("\n");
}

/**
 * Faithfully reproduce postgres.js `Bind`: a raw `sql` template hands params straight to the
 * driver, which cannot serialize a JS `Date`. A plain vi.fn() mock never encodes params, which is
 * exactly how a 100%-broken handler shipped green once (#357/#370) — so assert on bind here.
 */
function assertBindable(): void {
  for (const call of execute.mock.calls) {
    for (const p of compile(call[0]).params) {
      if (p instanceof Date) {
        throw new TypeError(
          'The "string" argument must be of type string or an instance of Buffer or ArrayBuffer. Received an instance of Date',
        );
      }
    }
  }
}

const LEAD_ROW_ID = "40000000-0000-0000-0000-000000000001";
const LEAD_ID = "50000000-0000-0000-0000-000000000001";

function leadRow(brandIds: string[] = ["brand-1"]) {
  return [
    {
      id: LEAD_ROW_ID,
      lead_id: LEAD_ID,
      campaign_id: "campaign-1",
      brand_ids: brandIds,
    },
  ];
}

async function buildApp() {
  const { default: route } = await import("../../src/routes/step-statements.js");
  const app = express();
  app.use(express.json());
  app.use(route);
  app.use((_e: Error, _r: express.Request, res: express.Response, _n: express.NextFunction) => {
    res.status(500).json({ error: "Internal server error" });
  });
  return app;
}

function post(app: express.Express, body: unknown) {
  return request(app)
    .post(`/orgs/leads/${LEAD_ROW_ID}/step-statements`)
    .set("x-api-key", "test-api-key")
    .set("x-org-id", "org-1")
    .set("x-user-id", "user-1")
    .send(body as object);
}

describe("POST /orgs/leads/:id/step-statements", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => {
    execute.mockReset().mockResolvedValue([]);
  });

  it("401 without the service api key", async () => {
    const res = await request(app)
      .post(`/orgs/leads/${LEAD_ROW_ID}/step-statements`)
      .set("x-org-id", "org-1")
      .send({ step: "sale", kind: "outcome" });
    expect(res.status).toBe(401);
    expect(execute).not.toHaveBeenCalled();
  });

  it("400 without an org — the caller is organisation-authenticated, not token-authenticated", async () => {
    const res = await request(app)
      .post(`/orgs/leads/${LEAD_ROW_ID}/step-statements`)
      .set("x-api-key", "test-api-key")
      .send({ step: "sale", kind: "outcome" });
    expect(res.status).toBe(400);
  });

  it("400 on an unknown step, and never touches the database", async () => {
    const res = await post(app, { step: "vibes", kind: "outcome" });
    expect(res.status).toBe(400);
    expect(execute).not.toHaveBeenCalled();
  });

  it("400 on a missing kind", async () => {
    const res = await post(app, { step: "sale" });
    expect(res.status).toBe(400);
  });

  it("400 when a \"never\" carries a value — refused, not silently dropped", async () => {
    const res = await post(app, { step: "sale", kind: "never", valueCents: 1000 });
    expect(res.status).toBe(400);
    expect(execute).not.toHaveBeenCalled();
  });

  it("400 on a \"sale\" outcome with no value — a won deal is never priced at the brand average", async () => {
    const res = await post(app, { step: "sale", kind: "outcome" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/valueCents is required/);
    expect(execute).not.toHaveBeenCalled();
  });

  it("the legacy \"purchase\" spelling is the same step, so it needs a value too", async () => {
    const res = await post(app, { step: "purchase", kind: "outcome" });
    expect(res.status).toBe(400);
    expect(execute).not.toHaveBeenCalled();
  });

  it("every OTHER step still records with no value — a large lead is worth stating before it closes", async () => {
    for (const step of ["signup", "meeting_booked", "meeting_attended", "form_submission"]) {
      execute
        .mockReset()
        .mockResolvedValueOnce(leadRow())
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([{ id: "ce-1", received_at: "2026-08-19 14:30:00+00" }]);
      const res = await post(app, { step, kind: "outcome" });
      expect(res.status).toBe(201);
      expect(res.body.statement.valueCents).toBeNull();
    }
  });

  it("a \"sale\" that is stated as never still needs no value", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([]) // no existing outcome
      .mockResolvedValueOnce([{ id: "d-1", created_at: "2026-08-19 14:30:00+00", updated_at: "2026-08-19 14:30:00+00" }]);
    const res = await post(app, { step: "sale", kind: "never" });
    expect(res.status).toBe(201);
    expect(res.body.statement).toMatchObject({ step: "sale", kind: "never", valueCents: null });
  });

  it("400 on an unparseable occurredAt rather than falling back to now()", async () => {
    const res = await post(app, {
      step: "sale",
      kind: "outcome",
      valueCents: 1000,
      occurredAt: "last tuesday",
    });
    expect(res.status).toBe(400);
    expect(execute).not.toHaveBeenCalled();
  });

  it("404 when the row belongs to another org", async () => {
    execute.mockResolvedValueOnce([]); // lookup scoped by org_id finds nothing
    const res = await post(app, { step: "sale", kind: "outcome", valueCents: 1000 });
    expect(res.status).toBe(404);
    expect(sqlAt(0)).toContain("org_id =");
  });

  it("records a hand-stated outcome into the ledger the counts read, tagged manual", async () => {
    execute
      .mockResolvedValueOnce(leadRow()) // lead row
      .mockResolvedValueOnce([]) // retract "never" — none
      .mockResolvedValueOnce([{ id: "ce-1", received_at: "2026-08-19 14:30:00+00" }]);

    const res = await post(app, {
      step: "sale",
      kind: "outcome",
      valueCents: 490000,
      note: "Closed on the call.",
      occurredAt: "2026-08-19T14:30:00.000Z",
    });

    expect(res.status).toBe(201);
    expect(res.body.statement).toMatchObject({
      leadCampaignId: LEAD_ROW_ID,
      leadId: LEAD_ID,
      campaignId: "campaign-1",
      brandId: "brand-1",
      step: "sale",
      kind: "outcome",
      source: "manual",
      valueCents: 490000,
      statedByUserId: "user-1",
    });
    expect(res.body.retractedNever).toBe(false);

    const insert = sqlAt(2);
    expect(insert).toContain("insert into conversion_events");
    expect(insert).toContain("on conflict");
    // The identity was NAMED by the caller, so nothing is matched and nothing is guessed.
    expect(paramsAt(2)).toContain(LEAD_ID);
    expect(paramsAt(2)).toContain("2026-08-19T14:30:00.000Z");
    // Attributable to the campaign it was stated on, not only to the brand.
    expect(paramsAt(2)).toContain("campaign-1");
    assertBindable();
  });

  it("folds the legacy \"purchase\" spelling onto \"sale\"", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ id: "ce-1", received_at: "2026-08-19 14:30:00+00" }]);
    const res = await post(app, { step: "purchase", kind: "outcome", valueCents: 1000 });
    expect(res.status).toBe(201);
    expect(res.body.statement.step).toBe("sale");
  });

  it("accepts meeting_attended — the step no tracker can observe", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([{ id: "ce-1", received_at: "2026-08-19 14:30:00+00" }]);
    const res = await post(app, { step: "meeting_attended", kind: "outcome" });
    expect(res.status).toBe(201);
    expect(res.body.statement.step).toBe("meeting_attended");
    expect(sqlAt(2)).toContain("insert into conversion_events");
  });

  it("an outcome retracts an earlier \"never\" for the same step, and says so", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([{ id: "d-1" }]) // a "never" existed
      .mockResolvedValueOnce([{ id: "ce-1", received_at: "2026-08-19 14:30:00+00" }]);
    const res = await post(app, { step: "sale", kind: "outcome", valueCents: 1000 });
    expect(res.status).toBe(201);
    expect(res.body.retractedNever).toBe(true);
    expect(sqlAt(1)).toContain("delete from lead_step_disqualifications");
  });

  it("a \"never\" NEVER lands in the conversion ledger", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([]) // no existing outcome
      .mockResolvedValueOnce([
        { id: "d-1", created_at: "2026-08-19 14:30:00+00", updated_at: "2026-08-19 14:30:00+00" },
      ]);
    const res = await post(app, { step: "meeting_booked", kind: "never", note: "left the company" });
    expect(res.status).toBe(201);
    expect(res.body.statement).toMatchObject({ step: "meeting_booked", kind: "never", valueCents: null });
    expect(allSql()).toContain("insert into lead_step_disqualifications");
    expect(allSql()).not.toContain("insert into conversion_events");
    assertBindable();
  });

  it("409 on \"never\" for a step that already happened", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([{ "?column?": 1 }]); // an attributed outcome exists
    const res = await post(app, { step: "sale", kind: "never" });
    expect(res.status).toBe(409);
    expect(allSql()).not.toContain("insert into lead_step_disqualifications");
  });

  it("404 for a brand scope the row is not part of, indistinguishable from an absent row", async () => {
    execute.mockResolvedValueOnce(leadRow(["brand-1"]));
    const res = await request(app)
      .post(`/orgs/leads/${LEAD_ROW_ID}/step-statements`)
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1")
      .set("x-brand-id", "brand-other")
      .send({ step: "sale", kind: "outcome", valueCents: 1000 });
    expect(res.status).toBe(404);
    expect(allSql()).not.toContain("insert into conversion_events");
  });

  it("500 (not a hung socket) when the database throws", async () => {
    execute.mockRejectedValueOnce(new Error("boom"));
    const res = await post(app, { step: "sale", kind: "outcome", valueCents: 1000 });
    expect(res.status).toBe(500);
  });
});

describe("GET /orgs/leads/:id/step-statements", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => {
    execute.mockReset().mockResolvedValue([]);
  });

  function get() {
    return request(app)
      .get(`/orgs/leads/${LEAD_ROW_ID}/step-statements`)
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
  }

  it("names every step, and pending is a state of its own", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([]) // outcomes
      .mockResolvedValueOnce([]); // nevers
    const res = await get();
    expect(res.status).toBe(200);
    expect(res.body.steps.map((s: { step: string }) => s.step)).toEqual([
      "signup",
      "meeting_booked",
      "form_submission",
      "sale",
      "meeting_attended",
      "website_visit",
    ]);
    expect(res.body.steps.every((s: { state: string }) => s.state === "pending")).toBe(true);
  });

  it("separates dead from pending, and hand-stated from tracker-reported", async () => {
    execute
      .mockResolvedValueOnce(leadRow())
      .mockResolvedValueOnce([
        {
          event: "signup",
          source: "tracker",
          value_cents: null,
          note: null,
          stated_by_user_id: null,
          received_at: "2026-08-01 09:00:00+00",
        },
        {
          event: "meeting_attended",
          source: "manual",
          value_cents: null,
          note: "showed up",
          stated_by_user_id: "user-1",
          received_at: "2026-08-05 09:00:00+00",
        },
      ])
      .mockResolvedValueOnce([
        {
          step: "sale",
          note: "budget cut",
          stated_by_user_id: "user-1",
          updated_at: "2026-08-06 09:00:00+00",
        },
      ]);

    const res = await get();
    expect(res.status).toBe(200);
    const byStep = Object.fromEntries(
      res.body.steps.map((s: { step: string }) => [s.step, s]),
    ) as Record<string, { state: string; source: string | null; note: string | null }>;
    expect(byStep.signup).toMatchObject({ state: "outcome", source: "tracker" });
    expect(byStep.meeting_attended).toMatchObject({ state: "outcome", source: "manual" });
    expect(byStep.sale).toMatchObject({ state: "never", note: "budget cut" });
    expect(byStep.form_submission.state).toBe("pending");
  });

  it("400 when the id is not a lead row id", async () => {
    const res = await request(app)
      .get("/orgs/leads/not-a-uuid/step-statements")
      .set("x-api-key", "test-api-key")
      .set("x-org-id", "org-1");
    expect(res.status).toBe(400);
    expect(execute).not.toHaveBeenCalled();
  });
});

describe("GET /internal/brands/:brandId/step-disqualifications", () => {
  let app: express.Express;
  beforeAll(async () => {
    app = await buildApp();
  }, 30_000);
  beforeEach(() => {
    execute.mockReset().mockResolvedValue([]);
  });

  it("answers all-zero for a brand nobody disqualified anyone for", async () => {
    const res = await request(app)
      .get("/internal/brands/brand-1/step-disqualifications")
      .set("x-api-key", "test-api-key");
    expect(res.status).toBe(200);
    expect(res.body.counts).toEqual({
      signup: 0,
      meeting_booked: 0,
      form_submission: 0,
      sale: 0,
      meeting_attended: 0,
      website_visit: 0,
    });
    expect(res.body.byStep.sale).toEqual([]);
  });

  it("returns the canonical email join key per step", async () => {
    execute
      .mockResolvedValueOnce([{ step: "sale", n: 2 }])
      .mockResolvedValueOnce([
        { step: "sale", email: "a@x.com" },
        { step: "sale", email: "b@x.com" },
      ]);
    const res = await request(app)
      .get("/internal/brands/brand-1/step-disqualifications")
      .set("x-api-key", "test-api-key");
    expect(res.status).toBe(200);
    expect(res.body.counts.sale).toBe(2);
    expect(res.body.byStep.sale).toEqual(["a@x.com", "b@x.com"]);
  });

  it("401 without the service api key", async () => {
    const res = await request(app).get("/internal/brands/brand-1/step-disqualifications");
    expect(res.status).toBe(401);
  });
});
