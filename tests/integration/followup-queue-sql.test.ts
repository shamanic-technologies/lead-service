/**
 * The follow-up queue's concurrency, executed against a REAL database.
 *
 * Two workers must never answer the same prospect twice: several replies land at the same moment,
 * and a read-then-write race would double-email a person, which is the one failure this feature
 * cannot take back. That guarantee lives in a conditional `UPDATE ... RETURNING` — a claim only
 * one transaction can win — and a mocked database cannot fail the way a real one does, which is
 * exactly how the paid pool's `cannot cast type record to text[]` reached production. So this file
 * RUNS the statements, concurrently, rather than asserting their shape.
 */
import { afterAll, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";

vi.mock("../../src/lib/email-gateway-client.js", () => ({
  // Nobody in these fixtures opted out. The opt-out stop has its own unit coverage; what is under
  // test here is the claim's exclusivity against a real Postgres.
  checkDeliveryStatus: async () => ({ results: [] }),
}));

const { db } = await import("../../src/db/index.js");
const { leads, leadContactMethods, leadsCampaigns } = await import("../../src/db/schema.js");
const {
  claimFollowup,
  loadFollowupCandidates,
  pickFollowupCandidate,
  readFollowupState,
  stopFollowups,
  writeFollowupStatement,
} = await import("../../src/lib/followup-queue.js");

/**
 * `tests/setup.ts` fills the DSN only when one is absent, so CI's throwaway Postgres wins there and
 * this placeholder is what a laptop with no database sees.
 */
const PLACEHOLDER_DSN = "postgresql://test:test@localhost:5432/test";
const hasRealDatabase = process.env.LEAD_SERVICE_DATABASE_URL !== PLACEHOLDER_DSN;

describe.skipIf(!hasRealDatabase)("follow-up queue against a real database", () => {
  const campaignId = `itest-followup-${randomUUID()}`;
  const orgId = randomUUID();
  const brandId = randomUUID();

  async function seedDueRow(dueAt: Date): Promise<string> {
    const [lead] = await db
      .insert(leads)
      .values({ name: `itest ${randomUUID()}` })
      .returning({ id: leads.id });
    await db.insert(leadContactMethods).values({
      leadId: lead.id,
      channel: "email",
      value: `${randomUUID()}@example.test`,
      source: "itest",
    });
    const [row] = await db
      .insert(leadsCampaigns)
      .values({
        leadId: lead.id,
        campaignId,
        orgId,
        brandIds: [brandId],
        status: "served",
        servedAt: new Date(),
        followupDueAt: dueAt,
      })
      .returning({ id: leadsCampaigns.id });
    return row.id;
  }

  beforeEach(async () => {
    await db.delete(leadsCampaigns).where(eq(leadsCampaigns.campaignId, campaignId));
  });

  afterAll(async () => {
    await db.delete(leadsCampaigns).where(eq(leadsCampaigns.campaignId, campaignId));
  });

  it("claims a due row once — a second claim of the same row is refused", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));
    const now = Date.now();

    const first = await claimFollowup({ id, nowMs: now, runId: randomUUID() });
    const second = await claimFollowup({ id, nowMs: now, runId: randomUUID() });

    expect(first).toBe(true);
    expect(second).toBe(false);
  });

  it("eight workers racing for ONE row: exactly one wins", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));
    const now = Date.now();

    const outcomes = await Promise.all(
      Array.from({ length: 8 }, () => claimFollowup({ id, nowMs: now, runId: randomUUID() })),
    );

    expect(outcomes.filter(Boolean)).toHaveLength(1);
  });

  it("four concurrent claims over two due people never hand the same person to two workers", async () => {
    const a = await seedDueRow(new Date(Date.now() - 120_000));
    const b = await seedDueRow(new Date(Date.now() - 60_000));

    const claims = await Promise.all(
      Array.from({ length: 4 }, () =>
        pickFollowupCandidate({
          orgId,
          campaignId,
          runId: randomUUID(),
          context: { orgId },
        }),
      ),
    );

    const claimedIds = claims.map((c) => c.claimed?.id).filter((x): x is string => Boolean(x));

    // Every worker either got somebody nobody else got, or got nobody. Never a duplicate.
    expect(new Set(claimedIds).size).toBe(claimedIds.length);
    expect(claimedIds.length).toBeGreaterThan(0);
    expect(claimedIds.length).toBeLessThanOrEqual(2);
    for (const id of claimedIds) expect([a, b]).toContain(id);
  });

  it("serves the oldest due person first — a backlog cannot starve whoever waited longest", async () => {
    const oldest = await seedDueRow(new Date(Date.now() - 10 * 24 * 60 * 60 * 1000));
    await seedDueRow(new Date(Date.now() - 60_000));

    const claim = await pickFollowupCandidate({
      orgId,
      campaignId,
      runId: randomUUID(),
      context: { orgId },
    });

    expect(claim.claimed?.id).toBe(oldest);
  });

  it("does not hand back a row whose due date is still in the future", async () => {
    await seedDueRow(new Date(Date.now() + 60 * 60 * 1000));

    const claim = await pickFollowupCandidate({
      orgId,
      campaignId,
      runId: randomUUID(),
      context: { orgId },
    });

    expect(claim).toEqual({ claimed: null, reason: "nothing_due" });
  });

  it("never returns a person with a booked meeting on record", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));
    const [row] = await db
      .select({ leadId: leadsCampaigns.leadId })
      .from(leadsCampaigns)
      .where(eq(leadsCampaigns.id, id));

    const { conversionEvents } = await import("../../src/db/schema.js");
    await db.insert(conversionEvents).values({
      brandId,
      orgId,
      event: "meeting_booked",
      matchConfidence: "deterministic",
      attributionStatus: "attributed",
      source: "manual",
      campaignId,
      leadCampaignId: id,
      matchedLeadId: row.leadId,
    });

    const candidates = await loadFollowupCandidates({ orgId, campaignId, nowMs: Date.now() });
    expect(candidates).toHaveLength(0);
  });

  it("a withdrawn booking stops stopping them — the read filters withdrawn statements", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));
    const { conversionEvents } = await import("../../src/db/schema.js");
    await db.insert(conversionEvents).values({
      brandId,
      orgId,
      event: "meeting_booked",
      matchConfidence: "deterministic",
      attributionStatus: "attributed",
      source: "manual",
      campaignId,
      leadCampaignId: id,
      withdrawnAt: new Date(),
    });

    const candidates = await loadFollowupCandidates({ orgId, campaignId, nowMs: Date.now() });
    expect(candidates.map((c) => c.id)).toEqual([id]);
  });

  it("records an action, releases the claim, and re-enters the person at the stated date", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));
    await claimFollowup({ id, nowMs: Date.now(), runId: randomUUID() });

    const nextDue = new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString();
    const state = await writeFollowupStatement({
      orgId,
      id,
      kind: "acted",
      dueAtIso: nextDue,
      reason: null,
      nowMs: Date.now(),
    });

    expect(state).toMatchObject({ followupCount: 1, claimedAt: null, stoppedReason: null });
    expect(state?.dueAt).toBe(new Date(nextDue).toISOString());
    expect(state?.lastActionAt).not.toBeNull();

    // Still in the future, so nobody is due yet.
    const claim = await pickFollowupCandidate({
      orgId,
      campaignId,
      runId: randomUUID(),
      context: { orgId },
    });
    expect(claim.claimed).toBeNull();
  });

  it("a stop empties the schedule, and a later schedule re-enters them — they answered again", async () => {
    const id = await seedDueRow(new Date(Date.now() - 60_000));

    const stopped = await writeFollowupStatement({
      orgId,
      id,
      kind: "stopped",
      dueAtIso: null,
      reason: "answered_again",
      nowMs: Date.now(),
    });
    expect(stopped).toMatchObject({ dueAt: null, stoppedReason: "answered_again" });
    expect(
      (await pickFollowupCandidate({ orgId, campaignId, runId: null, context: { orgId } })).claimed,
    ).toBeNull();

    const requeued = await writeFollowupStatement({
      orgId,
      id,
      kind: "scheduled",
      dueAtIso: new Date(Date.now() - 1_000).toISOString(),
      reason: null,
      nowMs: Date.now(),
    });
    expect(requeued).toMatchObject({ stoppedReason: null, followupCount: 0 });
    expect(
      (await pickFollowupCandidate({ orgId, campaignId, runId: null, context: { orgId } })).claimed
        ?.id,
    ).toBe(id);
  });

  it("stopFollowups clears a batch and readFollowupState reads it back", async () => {
    const a = await seedDueRow(new Date(Date.now() - 60_000));
    const b = await seedDueRow(new Date(Date.now() - 60_000));

    await stopFollowups([a, b], "opted_out", Date.now());

    for (const id of [a, b]) {
      const state = await readFollowupState(orgId, id);
      expect(state).toMatchObject({ dueAt: null, stoppedReason: "opted_out" });
    }
    expect(await readFollowupState(randomUUID(), a)).toBeNull();
  });
});
