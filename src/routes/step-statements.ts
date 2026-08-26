import { Router, Request, Response, NextFunction } from "express";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { apiKeyAuth, requireOrgId, AuthenticatedRequest } from "../middleware/auth.js";
import { toIsoTimestamp } from "../lib/basic-leads.js";
import {
  LEAD_STEP_OUTCOMES,
  canonicalizeStepOutcome,
  manualOutcomeSignature,
  type LeadStepOutcomeName,
  type StatementSource,
  type StepState,
} from "../lib/step-statements.js";

const router = Router();

// Express 4 does not forward async handler rejections to the error middleware — an unguarded
// throw hangs the caller's socket instead of answering. Wrap so a DB error is a clean 500.
function wrap<Req extends Request = Request>(
  fn: (req: Req, res: Response, next: NextFunction) => Promise<void>,
) {
  return (req: Request, res: Response, next: NextFunction) => {
    fn(req as Req, res, next).catch(next);
  };
}

/** `leads_campaigns.id` is a uuid column, so anything else is a caller error, not a miss. */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const StepStatementBodySchema = z.object({
  step: z.string(),
  kind: z.enum(["outcome", "never"]),
  valueCents: z.number().int().optional(),
  note: z.string().optional(),
  occurredAt: z.string().optional(),
});

interface LeadRow {
  id: string;
  lead_id: string;
  campaign_id: string;
  brand_ids: string[];
}

/**
 * The ONE membership row a caller names, or null. `org_id` is the entitlement boundary and sits
 * IN the predicate, so a row belonging to another org is indistinguishable from one that does
 * not exist — never a check afterwards.
 */
async function fetchLeadRow(orgId: string, id: string): Promise<LeadRow | null> {
  const rows = (await db.execute(sql`
    SELECT lc.id, lc.lead_id, lc.campaign_id, lc.brand_ids
    FROM leads_campaigns lc
    WHERE lc.id = ${id} AND lc.org_id = ${orgId}
    LIMIT 1
  `)) as unknown as LeadRow[];
  return rows[0] ?? null;
}

/**
 * Which brand the statement is about. A caller scoping to a brand it did not list the row under
 * is naming a lead it is not reading in that scope: answer exactly as if the row did not exist
 * rather than leaking its existence. Unscoped, the row's own primary brand answers.
 *
 * The scope is read from `?brandId=` — the same query parameter `GET /orgs/leads/:id` takes, so a
 * panel passes back exactly what it listed with — and from the `x-brand-id` identity header when
 * the caller carries one instead.
 */
function resolveBrandId(row: LeadRow, req: AuthenticatedRequest): string | null {
  const fromQuery = typeof req.query.brandId === "string" ? req.query.brandId : undefined;
  const requested = fromQuery ? [fromQuery] : (req.brandIds ?? []);
  if (requested.length > 0) return requested.find((b) => row.brand_ids.includes(b)) ?? null;
  return row.brand_ids[0] ?? null;
}

/**
 * POST /orgs/leads/:id/step-statements
 *
 * A HUMAN states what happened to ONE lead at ONE step of its campaign's sales funnel — or that
 * it never will. Organisation-authenticated (the customer dashboard and the staff console are
 * both org-authenticated callers); the publishable website-tracker token is deliberately NOT a
 * door to this: it is write-only, brand-scoped, and meant for a third party's page.
 *
 * `:id` is the id a list row already carries (the `leads_campaigns` membership row), so the
 * caller re-supplies no identity for a lead it has already resolved, there is nothing to match
 * and nothing to guess — which is exactly what repairs the ~90% unmatched rate the tracker's
 * identity waterfall carries for hand-stated facts. It also fixes the campaign: the row belongs
 * to one, so the statement is attributable to the campaign it was made on, not only to the brand.
 *
 * `kind: "outcome"` writes a conversion_events row tagged `source = 'manual'` — the ledger every
 * consumer already counts, so the brand's outcome counts move on the next read with nothing
 * downstream changed, and `source` keeps it distinguishable from a tracker-reported one.
 * Restating the same step corrects the first statement rather than counting twice.
 *
 * `kind: "never"` is NOT an outcome and nothing counts it: it writes a lead_step_disqualifications
 * row, a table no count reads. It is what lets a consumer tell a lead that is DEAD at a step from
 * one still PENDING.
 *
 * The two are mutually exclusive per step, and the contradiction is resolved in the only direction
 * that can be true: stating an outcome for a step previously marked "never" RETRACTS the never
 * (people change their mind and buy), and the response says so. Marking "never" on a step that
 * already has an outcome is refused (409) — a step that already happened cannot never happen.
 */
router.post(
  "/orgs/leads/:id/step-statements",
  apiKeyAuth,
  requireOrgId,
  wrap(async (req: AuthenticatedRequest, res: Response) => {
    const id = req.params.id;
    if (!UUID_RE.test(id)) {
      res.status(400).json({ error: "id must be the `id` of a lead row, a uuid" });
      return;
    }

    const parsed = StepStatementBodySchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid body: step and kind are required" });
      return;
    }
    const body = parsed.data;

    const step = canonicalizeStepOutcome(body.step);
    if (!step) {
      res.status(400).json({ error: `step must be one of ${LEAD_STEP_OUTCOMES.join(" | ")}` });
      return;
    }

    // A "never" is not an outcome, so it can carry no value. Refused rather than dropped: a
    // silently-ignored amount reads to the caller as recorded revenue that nothing will ever show.
    if (body.kind === "never" && body.valueCents !== undefined) {
      res.status(400).json({ error: "valueCents is not accepted on a \"never\" statement" });
      return;
    }

    // A stated SALE must say what it was worth. It is the one place in the whole system where
    // estimating has no excuse: with no value, every downstream money figure — pipeline, ROI,
    // cost per acquisition — prices the deal at the brand's AVERAGE lifetime revenue, a number
    // that describes no real customer, and does it silently. Every OTHER step stays optional: an
    // unusually large lead is worth stating early, long before it closes.
    if (body.kind === "outcome" && step === "sale" && body.valueCents === undefined) {
      res.status(400).json({
        error:
          "valueCents is required on a \"sale\" outcome — a won deal states what it was worth, " +
          "it is never estimated",
      });
      return;
    }

    // The moment the outcome happened, when the caller states a past fact. Bound as an ISO string
    // (a raw `sql` template hands params straight to postgres.js Bind, which cannot serialize a
    // Date), and rejected outright when unparseable — never silently replaced by now().
    let occurredAtIso: string | null = null;
    if (body.occurredAt !== undefined) {
      const parsedDate = new Date(body.occurredAt);
      if (Number.isNaN(parsedDate.getTime())) {
        res.status(400).json({ error: "occurredAt must be an ISO-8601 timestamp" });
        return;
      }
      occurredAtIso = parsedDate.toISOString();
    }

    const row = await fetchLeadRow(req.orgId!, id);
    if (!row) {
      res.status(404).json({ error: "Lead not found" });
      return;
    }

    const brandId = resolveBrandId(row, req);
    if (!brandId) {
      // Either the caller scoped to a brand this row is not part of (indistinguishable from an
      // absent row, deliberately), or the row carries no brand at all and there is nothing to
      // attribute the statement to. Both fail loud.
      res.status(404).json({ error: "Lead not found" });
      return;
    }

    const nowIso = new Date().toISOString();
    const statedBy = req.userId ?? null;

    if (body.kind === "never") {
      const existingOutcome = (await db.execute(sql`
        SELECT 1
        FROM conversion_events
        WHERE lead_campaign_id = ${row.id}
          AND event = ${step}
          AND attribution_status = 'attributed'
        LIMIT 1
      `)) as unknown as Array<unknown>;
      if (existingOutcome.length > 0) {
        res.status(409).json({
          error: `${step} already happened for this lead — it cannot be stated as never`,
        });
        return;
      }

      const inserted = (await db.execute(sql`
        INSERT INTO lead_step_disqualifications (
          lead_id, lead_campaign_id, campaign_id, brand_id, org_id, step, note, stated_by_user_id
        ) VALUES (
          ${row.lead_id}, ${row.id}, ${row.campaign_id}, ${brandId}, ${req.orgId!}, ${step},
          ${body.note ?? null}, ${statedBy}
        )
        ON CONFLICT (lead_id, campaign_id, step) DO UPDATE SET
          note = EXCLUDED.note,
          stated_by_user_id = EXCLUDED.stated_by_user_id,
          lead_campaign_id = EXCLUDED.lead_campaign_id,
          brand_id = EXCLUDED.brand_id,
          updated_at = now()
        RETURNING id, created_at, updated_at
      `)) as unknown as Array<{ id: string; created_at: Date | string; updated_at: Date | string }>;

      res.status(201).json({
        statement: {
          id: inserted[0].id,
          leadCampaignId: row.id,
          leadId: row.lead_id,
          campaignId: row.campaign_id,
          brandId,
          step,
          kind: "never",
          // A "never" has no source: nothing observes it, a person states it. Counted by nothing.
          source: "manual" as StatementSource,
          valueCents: null,
          note: body.note ?? null,
          statedByUserId: statedBy,
          statedAt: toIsoTimestamp(inserted[0].updated_at),
        },
      });
      return;
    }

    // An outcome supersedes a "never" for the same step — the person did the thing after all.
    const retracted = (await db.execute(sql`
      DELETE FROM lead_step_disqualifications
      WHERE lead_id = ${row.lead_id} AND campaign_id = ${row.campaign_id} AND step = ${step}
      RETURNING id
    `)) as unknown as Array<{ id: string }>;

    // Written into conversion_events — the ledger the counts already read — so a hand-stated
    // outcome moves the brand's numbers with no consumer change. match_* records how the identity
    // was established: the caller NAMED the lead, which is as deterministic as identity gets.
    const inserted = (await db.execute(sql`
      INSERT INTO conversion_events (
        brand_id, org_id, event, dedupe_signature, value_cents, matched_lead_id, match_method,
        match_confidence, attribution_status, candidate_count, received_at, source, campaign_id,
        lead_campaign_id, stated_by_user_id, note
      ) VALUES (
        ${brandId}, ${req.orgId!}, ${step}, ${manualOutcomeSignature(row.id, step)},
        ${body.valueCents ?? null}, ${row.lead_id}, 'manual', 'deterministic', 'attributed', 1,
        ${occurredAtIso ?? nowIso}, 'manual', ${row.campaign_id}, ${row.id}, ${statedBy},
        ${body.note ?? null}
      )
      ON CONFLICT (brand_id, dedupe_signature) WHERE dedupe_signature IS NOT NULL DO UPDATE SET
        value_cents = EXCLUDED.value_cents,
        note = EXCLUDED.note,
        received_at = EXCLUDED.received_at,
        stated_by_user_id = EXCLUDED.stated_by_user_id,
        campaign_id = EXCLUDED.campaign_id
      RETURNING id, received_at
    `)) as unknown as Array<{ id: string; received_at: Date | string }>;

    res.status(201).json({
      statement: {
        id: inserted[0].id,
        leadCampaignId: row.id,
        leadId: row.lead_id,
        campaignId: row.campaign_id,
        brandId,
        step,
        kind: "outcome",
        source: "manual" as StatementSource,
        valueCents: body.valueCents ?? null,
        note: body.note ?? null,
        statedByUserId: statedBy,
        statedAt: toIsoTimestamp(inserted[0].received_at),
      },
      retractedNever: retracted.length > 0,
    });
  }),
);

interface StepRead {
  step: LeadStepOutcomeName;
  state: StepState;
  source: StatementSource | null;
  valueCents: number | null;
  note: string | null;
  statedByUserId: string | null;
  at: string | null;
}

/**
 * GET /orgs/leads/:id/step-statements
 *
 * What is known about EVERY step of this lead's funnel, so a panel can state each one by hand and
 * read back what it stated. One entry per step of the outcome vocabulary, always all of them:
 *
 *   outcome — it happened. `source` says whether a human stated it or the website tracker
 *             reported it (a tracker event is attributed to the lead, not to a campaign, so it is
 *             read here at brand grain).
 *   never   — a human stated it never will. Nothing counts this; it is here so the reader can
 *             tell dead from pending.
 *   pending — neither has been stated. The honest third state, absent from every ledger, which is
 *             precisely why it had to be named rather than inferred from an empty count.
 */
router.get(
  "/orgs/leads/:id/step-statements",
  apiKeyAuth,
  requireOrgId,
  wrap(async (req: AuthenticatedRequest, res: Response) => {
    const id = req.params.id;
    if (!UUID_RE.test(id)) {
      res.status(400).json({ error: "id must be the `id` of a lead row, a uuid" });
      return;
    }

    const row = await fetchLeadRow(req.orgId!, id);
    if (!row) {
      res.status(404).json({ error: "Lead not found" });
      return;
    }
    const brandId = resolveBrandId(row, req);
    if (!brandId) {
      res.status(404).json({ error: "Lead not found" });
      return;
    }

    // Outcomes credited to this person for this brand. A hand-stated one is keyed to the row it
    // was stated on; a tracker-reported one knows only the brand, so it is matched on the lead.
    const outcomes = (await db.execute(sql`
      SELECT event, source, value_cents, note, stated_by_user_id, received_at
      FROM conversion_events
      WHERE brand_id = ${brandId}
        AND matched_lead_id = ${row.lead_id}
        AND attribution_status = 'attributed'
        AND (lead_campaign_id IS NULL OR lead_campaign_id = ${row.id})
      ORDER BY received_at DESC NULLS LAST
    `)) as unknown as Array<{
      event: string;
      source: string;
      value_cents: number | null;
      note: string | null;
      stated_by_user_id: string | null;
      received_at: Date | string | null;
    }>;

    const nevers = (await db.execute(sql`
      SELECT step, note, stated_by_user_id, updated_at
      FROM lead_step_disqualifications
      WHERE lead_id = ${row.lead_id} AND campaign_id = ${row.campaign_id}
    `)) as unknown as Array<{
      step: string;
      note: string | null;
      stated_by_user_id: string | null;
      updated_at: Date | string | null;
    }>;

    const byStep = new Map<LeadStepOutcomeName, StepRead>();
    for (const step of LEAD_STEP_OUTCOMES) {
      byStep.set(step, {
        step,
        state: "pending",
        source: null,
        valueCents: null,
        note: null,
        statedByUserId: null,
        at: null,
      });
    }

    // A "never" first, so a later outcome for the same step overwrites it — the same precedence
    // the write path applies when it retracts one.
    for (const n of nevers) {
      const step = canonicalizeStepOutcome(n.step);
      if (!step) continue;
      byStep.set(step, {
        step,
        state: "never",
        source: "manual",
        valueCents: null,
        note: n.note,
        statedByUserId: n.stated_by_user_id,
        at: toIsoTimestamp(n.updated_at),
      });
    }

    // Rows arrive newest first, so the first one seen for a step is the one that answers.
    const claimed = new Set<LeadStepOutcomeName>();
    for (const o of outcomes) {
      const step = canonicalizeStepOutcome(o.event);
      if (!step || claimed.has(step)) continue;
      claimed.add(step);
      byStep.set(step, {
        step,
        state: "outcome",
        source: o.source === "manual" ? "manual" : "tracker",
        valueCents: o.value_cents,
        note: o.note,
        statedByUserId: o.stated_by_user_id,
        at: toIsoTimestamp(o.received_at),
      });
    }

    res.json({
      leadCampaignId: row.id,
      leadId: row.lead_id,
      campaignId: row.campaign_id,
      brandId,
      steps: LEAD_STEP_OUTCOMES.map((s) => byStep.get(s)!),
    });
  }),
);

/**
 * GET /internal/brands/:brandId/step-disqualifications
 *
 * INTERNAL (service-auth: x-api-key — the same tier as the conversion-count reads, NO Clerk).
 * The people a human has stated will NEVER reach a given step, per step, for the brand.
 *
 * Nothing here is an outcome and nothing counts it as one: this is the read that lets a consumer
 * separate a lead that is DEAD at a step from one still PENDING, which is what stops a
 * cost-per-acquisition denominator from waiting forever on somebody who is never coming.
 *
 * The identity returned is the lead's canonical (primary) email — the join key features-service
 * already holds from audience membership, and the SAME identity /converted-lead-emails returns —
 * lowercased and DISTINCT. A lead with no email contact method yields no join key and is excluded
 * from the email set; `counts` is over statements, so it does not depend on that join.
 *
 * Never 404 — a brand nobody has disqualified anyone for returns empty sets and zero counts.
 */
router.get(
  "/internal/brands/:brandId/step-disqualifications",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;

    const counts = Object.fromEntries(LEAD_STEP_OUTCOMES.map((s) => [s, 0])) as Record<
      LeadStepOutcomeName,
      number
    >;
    const byStep = Object.fromEntries(LEAD_STEP_OUTCOMES.map((s) => [s, [] as string[]])) as Record<
      LeadStepOutcomeName,
      string[]
    >;

    const countRows = (await db.execute(sql`
      SELECT step, count(DISTINCT lead_id)::int AS n
      FROM lead_step_disqualifications
      WHERE brand_id = ${brandId}
      GROUP BY step
    `)) as unknown as Array<{ step: string; n: number }>;
    for (const r of countRows) {
      const step = canonicalizeStepOutcome(r.step);
      if (step) counts[step] += r.n;
    }

    const emailRows = (await db.execute(sql`
      SELECT DISTINCT d.step, lower(canonical.value) AS email
      FROM lead_step_disqualifications d
      JOIN LATERAL (
        SELECT cm.value
        FROM lead_contact_methods cm
        WHERE cm.lead_id = d.lead_id AND cm.channel = 'email'
        ORDER BY cm.created_at ASC NULLS LAST, cm.value ASC
        LIMIT 1
      ) canonical ON true
      WHERE d.brand_id = ${brandId}
        AND canonical.value IS NOT NULL
    `)) as unknown as Array<{ step: string; email: string | null }>;
    for (const r of emailRows) {
      const step = canonicalizeStepOutcome(r.step);
      if (!step || !r.email) continue;
      byStep[step].push(r.email);
    }

    res.json({ counts, byStep });
  }),
);

export default router;
