import { describe, it, expect, vi, beforeEach } from "vitest";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

const execute = vi.fn();
const checkDeliveryStatus = vi.fn();

vi.mock("../../src/db/index.js", () => ({
  db: { execute: (...args: unknown[]) => execute(...args) },
}));

vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (...args: unknown[]) => checkDeliveryStatus(...args),
}));

const {
  FOLLOWUP_BOOKED_OUTCOMES,
  FOLLOWUP_DUE_MAX_HORIZON_MS,
  FOLLOWUP_DUE_PAST_TOLERANCE_MS,
  FollowupOptOutLookupError,
  claimFollowup,
  isOptedOut,
  loadFollowupCandidates,
  parseDueDate,
  pickFollowupCandidate,
  writeFollowupStatement,
} = await import("../../src/lib/followup-queue.js");

const dialect = new PgDialect();
function compile(call: unknown): { sql: string; params: unknown[] } {
  return dialect.sqlToQuery(call as SQL);
}
function lastSql(): string {
  return compile(execute.mock.calls[execute.mock.calls.length - 1][0]).sql.toLowerCase();
}
/**
 * Faithfully reproduce postgres.js `Bind`: a raw `sql` template hands params straight to the
 * driver, which cannot serialize a JS `Date`. A plain mock never serializes, which is how a
 * 100%-broken handler ships green (#357/#370) — so every param this module binds is checked.
 */
function assertNoDateParams(): void {
  for (const call of execute.mock.calls) {
    for (const p of compile(call[0]).params) {
      expect(p, "a raw Date param would throw at Bind time in production").not.toBeInstanceOf(Date);
    }
  }
}

const NOW = Date.parse("2026-09-02T10:00:00.000Z");

beforeEach(() => {
  execute.mockReset().mockResolvedValue([]);
  checkDeliveryStatus.mockReset();
});

describe("parseDueDate — bounded, refused loudly, never clamped", () => {
  it("accepts a date inside the horizon", () => {
    const parsed = parseDueDate("2026-09-09T09:00:00.000Z", NOW);
    expect(parsed).toEqual({
      ok: true,
      iso: "2026-09-09T09:00:00.000Z",
      ms: Date.parse("2026-09-09T09:00:00.000Z"),
    });
  });

  it("accepts 'now' — a reply just landed and we owe an answer immediately", () => {
    expect(parseDueDate(new Date(NOW).toISOString(), NOW).ok).toBe(true);
  });

  it("accepts a date inside the clock-skew tolerance", () => {
    const slightlyPast = new Date(NOW - FOLLOWUP_DUE_PAST_TOLERANCE_MS + 1_000).toISOString();
    expect(parseDueDate(slightlyPast, NOW).ok).toBe(true);
  });

  it("refuses a date in the past — never clamped to now", () => {
    const past = new Date(NOW - 30 * 24 * 60 * 60 * 1000).toISOString();
    expect(parseDueDate(past, NOW)).toEqual({ ok: false, code: "due_date_out_of_bounds" });
  });

  it("refuses a date past the horizon", () => {
    const absurd = new Date(NOW + FOLLOWUP_DUE_MAX_HORIZON_MS + 60_000).toISOString();
    expect(parseDueDate(absurd, NOW)).toEqual({ ok: false, code: "due_date_out_of_bounds" });
  });

  it("refuses garbage and non-strings", () => {
    expect(parseDueDate("january-ish", NOW)).toEqual({ ok: false, code: "due_date_unparseable" });
    expect(parseDueDate(undefined, NOW)).toEqual({ ok: false, code: "due_date_unparseable" });
    expect(parseDueDate(1757000000000, NOW)).toEqual({ ok: false, code: "due_date_unparseable" });
  });

  it("honours a far-out date a prospect asked for", () => {
    // "recontact me in January" — four months out, chosen by the worker, not by a ladder here.
    expect(parseDueDate("2027-01-15T09:00:00.000Z", NOW).ok).toBe(true);
  });
});

describe("isOptedOut — the prospect's own act, and nothing overrides it", () => {
  const scope = (over: Record<string, unknown> = {}) => ({ unsubscribed: false, ...over });

  it("reads a campaign-scoped unsubscribe", () => {
    const result = {
      email: "a@b.com",
      broadcast: { campaign: scope({ unsubscribed: true }) },
    } as never;
    expect(isOptedOut(result, "camp-1")).toBe(true);
  });

  it("reads a global unsubscribe — an answer about the address itself", () => {
    const result = {
      email: "a@b.com",
      broadcast: { global: { email: { bounced: false, unsubscribed: true } } },
    } as never;
    expect(isOptedOut(result, "camp-1")).toBe(true);
  });

  it("reads a byCampaign entry", () => {
    const result = {
      email: "a@b.com",
      broadcast: { byCampaign: { "camp-1": scope({ unsubscribed: true }) } },
    } as never;
    expect(isOptedOut(result, "camp-1")).toBe(true);
  });

  it("is false when nothing says so, and for an absent result", () => {
    expect(isOptedOut({ email: "a@b.com", broadcast: { campaign: scope() } } as never, "camp-1")).toBe(
      false,
    );
    expect(isOptedOut(undefined, "camp-1")).toBe(false);
  });
});

describe("loadFollowupCandidates", () => {
  it("reads due rows oldest-due-first, skipping live claims and booked meetings", async () => {
    await loadFollowupCandidates({ orgId: "org-1", campaignId: "camp-1", nowMs: NOW });
    const text = lastSql();

    expect(text).toContain("followup_due_at is not null");
    expect(text).toContain("followup_due_at <=");
    expect(text).toContain("followup_claimed_at is null or");
    expect(text).toContain("order by lc.followup_due_at asc");
    expect(text).toContain("not exists");
    expect(text).toContain("conversion_events");
    expect(text).toContain("withdrawn_at is null");
    assertNoDateParams();
  });

  it("binds the booked-outcome list as an array parameter, never a bare JS array", () => {
    // A bare array compiles to `ANY(($1, $2, $3)::text[])`, which Postgres rejects with
    // `op ANY/ALL (array) requires array on right side`.
    return loadFollowupCandidates({ orgId: "org-1", campaignId: "camp-1", nowMs: NOW }).then(() => {
      const { sql: text, params } = compile(execute.mock.calls[0][0]);
      expect(text).toMatch(/= ANY\(\$\d+::text\[\]\)/);
      expect(params).toContainEqual([...FOLLOWUP_BOOKED_OUTCOMES]);
    });
  });

  it("drops a row with no registered email — delivery evidence is keyed on it", async () => {
    execute.mockResolvedValueOnce([
      { id: "r1", lead_id: "l1", campaign_id: "camp-1", brand_ids: ["b1"], email: null, followup_due_at: "2026-09-01 09:00:00+00", followup_count: 0, followup_last_action_at: null, audience_id: null },
      { id: "r2", lead_id: "l2", campaign_id: "camp-1", brand_ids: ["b1"], email: "b@c.com", followup_due_at: "2026-09-01 10:00:00+00", followup_count: 2, followup_last_action_at: null, audience_id: "aud-1" },
    ]);

    const out = await loadFollowupCandidates({ orgId: "org-1", campaignId: "camp-1", nowMs: NOW });

    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({ id: "r2", email: "b@c.com", followupCount: 2, brandId: "b1" });
  });
});

describe("claimFollowup — the arbiter", () => {
  it("is a conditional UPDATE that re-checks the row is still due and unclaimed", async () => {
    execute.mockResolvedValueOnce([{ id: "r1" }]);
    const ok = await claimFollowup({ id: "r1", nowMs: NOW, runId: "run-1" });

    const text = lastSql();
    expect(ok).toBe(true);
    expect(text).toContain("update leads_campaigns");
    expect(text).toContain("set followup_claimed_at");
    expect(text).toContain("followup_due_at is not null");
    expect(text).toContain("returning id");
    assertNoDateParams();
  });

  it("reports false when the row was taken in the interval — zero rows returned", async () => {
    execute.mockResolvedValueOnce([]);
    expect(await claimFollowup({ id: "r1", nowMs: NOW, runId: "run-1" })).toBe(false);
  });
});

const dueRow = (id: string, email: string, dueAt: string) => ({
  id,
  lead_id: `lead-${id}`,
  campaign_id: "camp-1",
  brand_ids: ["brand-1"],
  email,
  followup_due_at: dueAt,
  followup_count: 1,
  followup_last_action_at: null,
  audience_id: null,
});

describe("pickFollowupCandidate", () => {
  it("returns nobody, named, when nothing is due", async () => {
    execute.mockResolvedValueOnce([]);
    const out = await pickFollowupCandidate({
      orgId: "org-1",
      campaignId: "camp-1",
      runId: null,
      context: {},
      nowMs: NOW,
    });
    expect(out).toEqual({ claimed: null, reason: "nothing_due" });
    expect(checkDeliveryStatus).not.toHaveBeenCalled();
  });

  it("hands back the oldest due person and claims them", async () => {
    execute
      .mockResolvedValueOnce([dueRow("r1", "a@b.com", "2026-09-01 09:00:00+00")])
      .mockResolvedValueOnce([{ id: "r1" }]);
    checkDeliveryStatus.mockResolvedValue({ results: [] });

    const out = await pickFollowupCandidate({
      orgId: "org-1",
      campaignId: "camp-1",
      runId: "run-1",
      context: { orgId: "org-1" },
      nowMs: NOW,
    });

    expect(out).toEqual({ claimed: expect.objectContaining({ id: "r1", email: "a@b.com" }) });
    assertNoDateParams();
  });

  it("never returns somebody who opted out, and clears their schedule for good", async () => {
    execute
      .mockResolvedValueOnce([
        dueRow("r1", "out@b.com", "2026-09-01 09:00:00+00"),
        dueRow("r2", "ok@b.com", "2026-09-01 10:00:00+00"),
      ])
      .mockResolvedValueOnce([]) // stopFollowups
      .mockResolvedValueOnce([{ id: "r2" }]); // claim
    checkDeliveryStatus.mockResolvedValue({
      results: [{ email: "out@b.com", broadcast: { campaign: { unsubscribed: true } } }],
    });

    const out = await pickFollowupCandidate({
      orgId: "org-1",
      campaignId: "camp-1",
      runId: null,
      context: {},
      nowMs: NOW,
    });

    expect(out).toEqual({ claimed: expect.objectContaining({ id: "r2" }) });
    const stop = compile(execute.mock.calls[1][0]);
    expect(stop.sql.toLowerCase()).toContain("set followup_due_at = null");
    expect(stop.params).toContain("opted_out");
    expect(stop.params).toContainEqual(["r1"]);
  });

  it("walks past a candidate another worker took and claims the next", async () => {
    execute
      .mockResolvedValueOnce([
        dueRow("r1", "a@b.com", "2026-09-01 09:00:00+00"),
        dueRow("r2", "b@b.com", "2026-09-01 10:00:00+00"),
      ])
      .mockResolvedValueOnce([]) // r1 claim lost
      .mockResolvedValueOnce([{ id: "r2" }]); // r2 claim won
    checkDeliveryStatus.mockResolvedValue({ results: [] });

    const out = await pickFollowupCandidate({
      orgId: "org-1",
      campaignId: "camp-1",
      runId: null,
      context: {},
      nowMs: NOW,
    });

    expect(out).toEqual({ claimed: expect.objectContaining({ id: "r2" }) });
  });

  it("reports all_claimed when every due row was taken", async () => {
    execute
      .mockResolvedValueOnce([dueRow("r1", "a@b.com", "2026-09-01 09:00:00+00")])
      .mockResolvedValueOnce([]);
    checkDeliveryStatus.mockResolvedValue({ results: [] });

    const out = await pickFollowupCandidate({
      orgId: "org-1",
      campaignId: "camp-1",
      runId: null,
      context: {},
      nowMs: NOW,
    });

    expect(out).toEqual({ claimed: null, reason: "all_claimed" });
  });

  it("THROWS when the delivery layer cannot answer — never claims on a guess", async () => {
    execute.mockResolvedValueOnce([dueRow("r1", "a@b.com", "2026-09-01 09:00:00+00")]);
    checkDeliveryStatus.mockRejectedValue(new Error("gateway down"));

    await expect(
      pickFollowupCandidate({
        orgId: "org-1",
        campaignId: "camp-1",
        runId: null,
        context: {},
        nowMs: NOW,
      }),
    ).rejects.toBeInstanceOf(FollowupOptOutLookupError);

    // Nothing was claimed: the only statement executed was the read.
    expect(execute).toHaveBeenCalledTimes(1);
  });
});

describe("writeFollowupStatement", () => {
  const state = [
    {
      id: "r1",
      lead_id: "l1",
      campaign_id: "camp-1",
      followup_due_at: "2026-09-09 09:00:00+00",
      followup_claimed_at: null,
      followup_count: 4,
      followup_last_action_at: "2026-09-02 10:00:00+00",
      followup_stopped_reason: null,
    },
  ];

  it("acted increments the count, stamps the action and releases the claim", async () => {
    execute.mockResolvedValueOnce(state);
    const out = await writeFollowupStatement({
      orgId: "org-1",
      id: "r1",
      kind: "acted",
      dueAtIso: "2026-09-09T09:00:00.000Z",
      reason: null,
      nowMs: NOW,
    });

    const { sql: text, params } = compile(execute.mock.calls[0][0]);
    expect(text.toLowerCase()).toContain("followup_claimed_at = null");
    expect(params).toContain(1); // the increment
    expect(out).toMatchObject({ followupCount: 4, dueAt: "2026-09-09T09:00:00.000Z" });
    assertNoDateParams();
  });

  it("scheduled does not increment — nobody answered anybody yet", async () => {
    execute.mockResolvedValueOnce(state);
    await writeFollowupStatement({
      orgId: "org-1",
      id: "r1",
      kind: "scheduled",
      dueAtIso: "2026-09-02T12:00:00.000Z",
      reason: null,
      nowMs: NOW,
    });
    expect(compile(execute.mock.calls[0][0]).params).toContain(0);
  });

  it("stopped clears the due date and records why", async () => {
    execute.mockResolvedValueOnce([
      { ...state[0], followup_due_at: null, followup_stopped_reason: "answered_again" },
    ]);
    const out = await writeFollowupStatement({
      orgId: "org-1",
      id: "r1",
      kind: "stopped",
      dueAtIso: null,
      reason: "answered_again",
      nowMs: NOW,
    });
    expect(out).toMatchObject({ dueAt: null, stoppedReason: "answered_again" });
  });

  it("scopes the write to the org — a foreign row is simply not there", async () => {
    execute.mockResolvedValueOnce([]);
    const out = await writeFollowupStatement({
      orgId: "org-2",
      id: "r1",
      kind: "stopped",
      dueAtIso: null,
      reason: "x",
      nowMs: NOW,
    });
    expect(compile(execute.mock.calls[0][0]).sql.toLowerCase()).toContain("org_id =");
    expect(out).toBeNull();
  });

  it("normalizes a timestamp the driver hands back as a Date", async () => {
    execute.mockResolvedValueOnce([
      { ...state[0], followup_due_at: new Date("2026-09-09T09:00:00.000Z") },
    ]);
    const out = await writeFollowupStatement({
      orgId: "org-1",
      id: "r1",
      kind: "acted",
      dueAtIso: "2026-09-09T09:00:00.000Z",
      reason: null,
      nowMs: NOW,
    });
    expect(out?.dueAt).toBe("2026-09-09T09:00:00.000Z");
  });
});
