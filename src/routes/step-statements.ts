import { Router, Request, Response, NextFunction } from "express";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { apiKeyAuth, requireOrgId, AuthenticatedRequest } from "../middleware/auth.js";
import { toIsoTimestamp } from "../lib/basic-leads.js";
import {
  LEAD_STEP_OUTCOMES,
  WEBSITE_VISIT,
  canonicalizeStepOutcome,
  manualOutcomeSignature,
  type LeadStepOutcomeName,
  type StatementSource,
  type StepState,
} from "../lib/step-statements.js";
import {
  MeasuredVisitLookupError,
  fetchMeasuredVisitEmails,
} from "../lib/measured-visits.js";
import {
  FunnelChainError,
  resolveCampaignChain,
  fetchOrgCampaignFunnelKeys,
  type ResolvedFunnelChain,
} from "../lib/campaign-funnel-client.js";
import {
  stepAndEarlier,
  stepAndLater,
  FUNNEL_STEP_CHAINS,
  type FunnelKey,
} from "../lib/funnel-chains.js";
import {
  resolveStepStates,
  type StatedNever,
  type StatedOutcome,
} from "../lib/step-chain-state.js";

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
  /** The lead's canonical (primary) email — the identity delivery evidence is keyed on. */
  email: string | null;
}

/**
 * The ONE membership row a caller names, or null. `org_id` is the entitlement boundary and sits
 * IN the predicate, so a row belonging to another org is indistinguishable from one that does
 * not exist — never a check afterwards.
 */
async function fetchLeadRow(orgId: string, id: string): Promise<LeadRow | null> {
  const rows = (await db.execute(sql`
    SELECT lc.id, lc.lead_id, lc.campaign_id, lc.brand_ids, lower(canonical.value) AS email
    FROM leads_campaigns lc
    LEFT JOIN LATERAL (
      SELECT cm.value
      FROM lead_contact_methods cm
      WHERE cm.lead_id = lc.lead_id AND cm.channel = 'email'
      ORDER BY cm.created_at ASC NULLS LAST, cm.value ASC
      LIMIT 1
    ) canonical ON true
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
 * Has the DELIVERY LAYER already measured this lead's visit (a click on the email we sent) for
 * this brand? email-gateway owns that evidence and nothing here changes it; this is a read.
 *
 * It is what keeps the panel coherent with the counts: a visit the delivery layer measured shows
 * as an outcome the tracker reported, so nobody is invited to state a fact the system already
 * holds, and stating "never happened" about a visit that demonstrably happened is refused.
 * A lead with no registered email can carry no delivery evidence — false, never a guess.
 */
async function visitAlreadyMeasured(
  row: LeadRow,
  brandId: string,
  orgId: string,
): Promise<boolean> {
  if (!row.email) return false;
  const measured = await fetchMeasuredVisitEmails(brandId, orgId, [row.email]);
  return measured.has(row.email);
}

/** A measured-visit lookup that could not be answered is a 502, never a silent "not measured". */
function respondMeasuredVisitFailure(error: unknown, res: Response): boolean {
  if (!(error instanceof MeasuredVisitLookupError)) return false;
  console.error(error.message);
  res.status(502).json({
    error:
      "email-gateway could not say whether this lead's website visit was already measured. " +
      "No answer is returned rather than one that could contradict the counts.",
  });
  return true;
}

/**
 * WHICH chain this lead is on. A funnel is a chain, and "before" / "after" mean nothing without
 * knowing which one: a campaign selling meetings off replies runs reply -> booked -> attended ->
 * paid, one selling off the website runs visit -> signup -> paid. The campaign is on the row and
 * campaign-service states its funnel, so it is read from there and never inferred.
 *
 * NO SILENT FALLBACK, and the failures are kept apart because they are different facts:
 *   502 — campaign-service could not answer. Transient.
 *   409 — the campaign states no funnel this service has a chain for (or campaign-service does not
 *         know the campaign). Nothing is broken; there is simply no order to read the steps in, and
 *         a made-up order would print a chain nobody stated.
 * Returns null having already answered.
 */
async function resolveChainOrRespond(
  row: LeadRow,
  req: AuthenticatedRequest,
  res: Response,
): Promise<ResolvedFunnelChain | null> {
  try {
    return await resolveCampaignChain(row.campaign_id, {
      orgId: req.orgId!,
      userId: req.userId ?? null,
      runId: req.runId ?? null,
      brandId: req.brandIds?.[0] ?? null,
    });
  } catch (error) {
    if (!(error instanceof FunnelChainError)) throw error;
    console.error(error.message);
    if (error.reason === "unavailable") {
      res.status(502).json({
        error:
          "campaign-service could not say which sales funnel this lead's campaign sells through, " +
          "so the order of its steps is unknown. No answer is returned rather than one built on a " +
          "chain nobody stated.",
        code: "campaign_service_unavailable",
      });
      return null;
    }
    res.status(409).json({
      error:
        error.reason === "unknown"
          ? "The campaign this lead belongs to is unknown to campaign-service, so its sales funnel " +
            "— and therefore the order of its steps — cannot be resolved."
          : "This lead's campaign states no sales funnel, so its steps have no order. Declare the " +
            "campaign's funnel and the chain resolves.",
      code: error.reason === "unknown" ? "campaign_unknown" : "funnel_unstated",
    });
    return null;
  }
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

    // A statement is only coherent against the chain it is made on, so the chain is resolved
    // BEFORE anything is written: a "never" constrains every LATER step, an outcome constrains
    // every EARLIER one, and neither rule can be enforced without knowing the order.
    const resolved = await resolveChainOrRespond(row, req, res);
    if (!resolved) return;
    const chain = resolved.chain;

    const nowIso = new Date().toISOString();
    const statedBy = req.userId ?? null;

    if (body.kind === "never") {
      // Everything this "never" would also make never: the step itself and every step AFTER it on
      // the chain. An outcome standing on ANY of them contradicts the statement — a lead that paid
      // cannot never have booked — so the refusal that already guarded the step itself guards the
      // whole forward slice, which is what stops the two directions disagreeing when statements
      // arrive in the other order.
      const blockedBy = stepAndLater(chain, step);

      // A visit the delivery layer already measured HAPPENED, whatever a person types about it.
      // Same refusal as an outcome already on the ledger, for the same reason.
      if (blockedBy.includes(WEBSITE_VISIT)) {
        let measured: boolean;
        try {
          measured = await visitAlreadyMeasured(row, brandId, req.orgId!);
        } catch (error) {
          if (respondMeasuredVisitFailure(error, res)) return;
          throw error;
        }
        if (measured) {
          res.status(409).json({
            error:
              step === WEBSITE_VISIT
                ? "website_visit was already measured for this lead (a click on the email we sent) " +
                  "— it cannot be stated as never"
                : `website_visit was already measured for this lead and comes after ${step} on this ` +
                  `campaign's funnel, so ${step} cannot be stated as never`,
            code: "step_already_happened",
          });
          return;
        }
      }

      // Exactly the outcomes the READ answers with for this lead: a hand-stated one is keyed to
      // the row it was stated on, a tracker-reported one knows only the brand. Asking a narrower
      // question here than the read asks is how the panel and the write path come to disagree.
      const existingOutcome = (await db.execute(sql`
        SELECT event
        FROM conversion_events
        WHERE brand_id = ${brandId}
          AND matched_lead_id = ${row.lead_id}
          AND attribution_status = 'attributed'
          AND (lead_campaign_id IS NULL OR lead_campaign_id = ${row.id})
          AND event = ANY(${sql.param(blockedBy)}::text[])
        LIMIT 1
      `)) as unknown as Array<{ event: string }>;
      if (existingOutcome.length > 0) {
        const happened = existingOutcome[0].event;
        res.status(409).json({
          error:
            happened === step
              ? `${step} already happened for this lead — it cannot be stated as never`
              : `${happened} already happened for this lead and comes after ${step} on this ` +
                `campaign's funnel, so ${step} cannot be stated as never`,
          code: "step_already_happened",
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
          -- Restating a "never" that was retracted by an outcome makes it live again: the person
          -- changed their mind again, and the row is the same statement, restated.
          retracted_at = NULL,
          retracted_by_step = NULL,
          retracted_by_user_id = NULL,
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
        funnelKey: resolved.funnelKey,
        chain: resolved.chain,
      });
      return;
    }

    // An outcome supersedes a "never" for the same step — the person did the thing after all — and,
    // along the chain, for every step BEFORE it too: a lead that paid necessarily got through the
    // steps that lead to paying, so a "never" standing on any of them is contradicted by the fact.
    //
    // The row is MARKED retracted, never deleted: what somebody actually stated is the thing that
    // makes this auditable, and it must survive being superseded. Every read filters it out, so
    // nothing counts it and nothing shows it as live.
    const supersedes = stepAndEarlier(chain, step);
    const retracted = (await db.execute(sql`
      UPDATE lead_step_disqualifications
      SET retracted_at = now(),
          retracted_by_step = ${step},
          retracted_by_user_id = ${statedBy},
          updated_at = now()
      WHERE lead_id = ${row.lead_id}
        AND campaign_id = ${row.campaign_id}
        AND step = ANY(${sql.param(supersedes)}::text[])
        AND retracted_at IS NULL
      RETURNING step
    `)) as unknown as Array<{ step: string }>;

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
      funnelKey: resolved.funnelKey,
      chain: resolved.chain,
      // Kept as a boolean for the callers that already read it; the steps say WHICH statements the
      // outcome superseded, including the earlier ones the chain reached.
      retractedNever: retracted.length > 0,
      retractedNeverSteps: retracted.map((r) => r.step),
    });
  }),
);

/**
 * GET /orgs/leads/:id/step-statements
 *
 * What is known about EVERY step of this lead's funnel, so a panel can state each one by hand and
 * read back what it stated — with the chain's two rules already applied, because a funnel is a
 * chain and no two surfaces may show a lead as dead at one step and alive at a later one.
 *
 * One entry per step of the outcome vocabulary, always all of them:
 *
 *   outcome — it happened. Either because somebody stated it (or the tracker reported it), or
 *             because a LATER step of this campaign's chain did: a lead that paid necessarily got
 *             through the steps that lead to paying.
 *   never   — it will not happen. Either stated, or implied by an EARLIER step of the chain being
 *             never: once a step is false, everything after it is false. Nothing counts either.
 *   pending — nobody spoke and neither rule reaches it. The honest "still on its way".
 *
 * `origin` is what tells a reader a step somebody STATED from one the chain IMPLIES, and an implied
 * step carries no author, no note and no date because nobody made that statement. `statedState`
 * keeps what a person really said readable even where the chain concluded otherwise, so a real
 * statement is never lost. Because implication is computed on READ from the live statements,
 * retracting or superseding one moves everything it implied with it, automatically.
 *
 * `chain` is this campaign's funnel, in order — per FUNNEL, never a universal step order. Steps
 * outside it (`inChain: false`) read from statements alone: no chain rule reaches them.
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

    const resolved = await resolveChainOrRespond(row, req, res);
    if (!resolved) return;

    // Outcomes credited to this person for this brand. A hand-stated one is keyed to the row it
    // was stated on; a tracker-reported one knows only the brand, so it is matched on the lead.
    const outcomeRows = (await db.execute(sql`
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

    // Retracted statements are excluded: they are kept for the record, not to be read as live.
    const neverRows = (await db.execute(sql`
      SELECT step, note, stated_by_user_id, updated_at
      FROM lead_step_disqualifications
      WHERE lead_id = ${row.lead_id}
        AND campaign_id = ${row.campaign_id}
        AND retracted_at IS NULL
    `)) as unknown as Array<{
      step: string;
      note: string | null;
      stated_by_user_id: string | null;
      updated_at: Date | string | null;
    }>;

    // Rows arrive newest first, so the first one seen for a step is the one that answers.
    const outcomes = new Map<LeadStepOutcomeName, StatedOutcome>();
    for (const o of outcomeRows) {
      const step = canonicalizeStepOutcome(o.event);
      if (!step || outcomes.has(step)) continue;
      outcomes.set(step, {
        source: o.source === "manual" ? "manual" : "tracker",
        valueCents: o.value_cents,
        note: o.note,
        statedByUserId: o.stated_by_user_id,
        at: toIsoTimestamp(o.received_at),
      });
    }

    const nevers = new Map<LeadStepOutcomeName, StatedNever>();
    for (const n of neverRows) {
      const step = canonicalizeStepOutcome(n.step);
      if (!step) continue;
      nevers.set(step, {
        note: n.note,
        statedByUserId: n.stated_by_user_id,
        at: toIsoTimestamp(n.updated_at),
      });
    }

    // The website visit is the one step that is ALSO measured automatically — a click on the email
    // we sent, owned by the delivery layer. A hand-stated visit is written like any other outcome
    // (and read above); a MEASURED one is not in this ledger at all, so it is read where it lives.
    // Without this the panel would offer to state a visit the system already knows about, and the
    // count (which suppresses the hand-stated duplicate) would disagree with what the panel shows.
    // A hand statement already on the row wins the display — it is the more specific fact, and it
    // carries the note and the date the person gave.
    if (!outcomes.has(WEBSITE_VISIT)) {
      let measured: boolean;
      try {
        measured = await visitAlreadyMeasured(row, brandId, req.orgId!);
      } catch (error) {
        if (respondMeasuredVisitFailure(error, res)) return;
        throw error;
      }
      if (measured) {
        outcomes.set(WEBSITE_VISIT, {
          source: "tracker",
          valueCents: null,
          note: null,
          statedByUserId: null,
          at: null,
        });
      }
    }

    res.json({
      leadCampaignId: row.id,
      leadId: row.lead_id,
      campaignId: row.campaign_id,
      brandId,
      funnelKey: resolved.funnelKey,
      chain: resolved.chain,
      steps: resolveStepStates({
        allSteps: LEAD_STEP_OUTCOMES,
        chain: resolved.chain,
        outcomes,
        nevers,
      }),
    });
  }),
);

/**
 * GET /internal/brands/:brandId/step-disqualifications[?implied=true]
 *
 * INTERNAL (service-auth: x-api-key — the same tier as the conversion-count reads, NO Clerk).
 * The people a human has stated will NEVER reach a given step, per step, for the brand.
 *
 * Nothing here is an outcome and nothing counts it as one: this is the read that lets a consumer
 * separate a lead that is DEAD at a step from one still PENDING, which is what stops a
 * cost-per-acquisition denominator from waiting forever on somebody who is never coming.
 *
 * `counts` / `byStep` are the STATEMENTS THEMSELVES — what a person actually said — and they are
 * byte-identical to what this read has always answered (retracted statements excluded, since a
 * superseded "never" was never a live one).
 *
 * `?implied=true` additionally applies the chain: a lead that will never book has, by the same
 * statement, never attended and never paid. That needs each lead's campaign funnel, so it is opt-in
 * — a consumer that does not ask pays for no campaign-service read and sees exactly what it saw
 * before. It answers three more fields, kept apart so a reader can always tell what somebody stated
 * from what the chain concluded:
 *
 *   impliedCounts / impliedByStep   — steps NOBODY stated, which a stated "never" earlier on the
 *                                     chain makes never.
 *   effectiveCounts / effectiveByStep — stated and implied together: the answer to "is this lead
 *                                     dead at this step?".
 *
 * A never contradicted by an outcome further down the chain is dropped from the implied and
 * effective sets (the lead demonstrably got there), never from the stated ones.
 *
 * The identity returned is the lead's canonical (primary) email — the join key features-service
 * already holds from audience membership, and the SAME identity /converted-lead-emails returns —
 * lowercased and DISTINCT. A lead with no email contact method yields no join key and is excluded
 * from the email sets; the counts are over statements, so they do not depend on that join.
 *
 * Never 404 — a brand nobody has disqualified anyone for returns empty sets and zero counts.
 */
router.get(
  "/internal/brands/:brandId/step-disqualifications",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;
    const wantsImplied = req.query.implied === "true";

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
        AND retracted_at IS NULL
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
        AND d.retracted_at IS NULL
        AND canonical.value IS NOT NULL
    `)) as unknown as Array<{ step: string; email: string | null }>;
    for (const r of emailRows) {
      const step = canonicalizeStepOutcome(r.step);
      if (!step || !r.email) continue;
      byStep[step].push(r.email);
    }

    if (!wantsImplied) {
      res.json({ counts, byStep });
      return;
    }

    // --- the chain view, opt-in ---

    const statementRows = (await db.execute(sql`
      SELECT d.lead_id, d.campaign_id, d.org_id, d.step, lower(canonical.value) AS email
      FROM lead_step_disqualifications d
      LEFT JOIN LATERAL (
        SELECT cm.value
        FROM lead_contact_methods cm
        WHERE cm.lead_id = d.lead_id AND cm.channel = 'email'
        ORDER BY cm.created_at ASC NULLS LAST, cm.value ASC
        LIMIT 1
      ) canonical ON true
      WHERE d.brand_id = ${brandId}
        AND d.retracted_at IS NULL
    `)) as unknown as Array<{
      lead_id: string;
      campaign_id: string;
      org_id: string;
      step: string;
      email: string | null;
    }>;

    const impliedCounts = Object.fromEntries(LEAD_STEP_OUTCOMES.map((s) => [s, 0])) as Record<
      LeadStepOutcomeName,
      number
    >;
    const impliedByStep = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, [] as string[]]),
    ) as Record<LeadStepOutcomeName, string[]>;
    const effectiveCounts = Object.fromEntries(LEAD_STEP_OUTCOMES.map((s) => [s, 0])) as Record<
      LeadStepOutcomeName,
      number
    >;
    const effectiveByStep = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, [] as string[]]),
    ) as Record<LeadStepOutcomeName, string[]>;

    if (statementRows.length === 0) {
      res.json({ counts, byStep, impliedCounts, impliedByStep, effectiveCounts, effectiveByStep });
      return;
    }

    // WHICH chain each of those leads is on. Read from campaign-service, per org, never inferred.
    const funnelByCampaign = new Map<string, FunnelKey | null>();
    try {
      for (const orgId of new Set(statementRows.map((r) => r.org_id))) {
        for (const [campaignId, funnelKey] of await fetchOrgCampaignFunnelKeys({ orgId })) {
          funnelByCampaign.set(campaignId, funnelKey);
        }
      }
    } catch (error) {
      if (!(error instanceof FunnelChainError)) throw error;
      console.error(error.message);
      res.status(502).json({
        error:
          "campaign-service could not say which sales funnels these leads' campaigns sell through, " +
          "so no chain can be applied. No answer is returned rather than one built on a chain " +
          "nobody stated.",
        code: "campaign_service_unavailable",
      });
      return;
    }

    const unstated = Array.from(
      new Set(
        statementRows
          .map((r) => r.campaign_id)
          .filter((campaignId) => !funnelByCampaign.get(campaignId)),
      ),
    );
    if (unstated.length > 0) {
      res.status(409).json({
        error:
          "Some of these leads belong to campaigns that state no sales funnel, so their steps have " +
          "no order and no chain can be applied. Declare those campaigns' funnels and the chain " +
          "resolves.",
        code: "funnel_unstated",
        campaignIds: unstated,
      });
      return;
    }

    // Outcomes contradict a "never" further up the chain, so they are read before anything is
    // concluded: a lead that demonstrably paid is not dead at the steps that lead to paying.
    const leadIds = Array.from(new Set(statementRows.map((r) => r.lead_id)));
    const outcomeRows = (await db.execute(sql`
      SELECT matched_lead_id, event
      FROM conversion_events
      WHERE brand_id = ${brandId}
        AND attribution_status = 'attributed'
        AND matched_lead_id = ANY(${sql.param(leadIds)}::uuid[])
    `)) as unknown as Array<{ matched_lead_id: string; event: string }>;

    const outcomesByLead = new Map<string, Map<LeadStepOutcomeName, StatedOutcome>>();
    for (const o of outcomeRows) {
      const step = canonicalizeStepOutcome(o.event);
      if (!step) continue;
      let m = outcomesByLead.get(o.matched_lead_id);
      if (!m) outcomesByLead.set(o.matched_lead_id, (m = new Map()));
      if (!m.has(step)) {
        m.set(step, { source: "manual", valueCents: null, note: null, statedByUserId: null, at: null });
      }
    }

    // One resolution per (lead, campaign): that pair is the chain a statement was made on.
    interface Group {
      leadId: string;
      campaignId: string;
      email: string | null;
      nevers: Map<LeadStepOutcomeName, StatedNever>;
    }
    const groups = new Map<string, Group>();
    for (const r of statementRows) {
      const step = canonicalizeStepOutcome(r.step);
      if (!step) continue;
      const key = `${r.lead_id}|${r.campaign_id}`;
      let group = groups.get(key);
      if (!group) {
        groups.set(
          key,
          (group = { leadId: r.lead_id, campaignId: r.campaign_id, email: r.email, nevers: new Map() }),
        );
      }
      group.nevers.set(step, { note: null, statedByUserId: null, at: null });
    }

    const impliedLeads = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, new Set<string>()]),
    ) as Record<LeadStepOutcomeName, Set<string>>;
    const effectiveLeads = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, new Set<string>()]),
    ) as Record<LeadStepOutcomeName, Set<string>>;
    const impliedEmails = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, new Set<string>()]),
    ) as Record<LeadStepOutcomeName, Set<string>>;
    const effectiveEmails = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((s) => [s, new Set<string>()]),
    ) as Record<LeadStepOutcomeName, Set<string>>;

    for (const group of groups.values()) {
      const funnelKey = funnelByCampaign.get(group.campaignId) as FunnelKey;
      const chain = FUNNEL_STEP_CHAINS[funnelKey];
      const states = resolveStepStates({
        allSteps: LEAD_STEP_OUTCOMES,
        chain,
        outcomes: outcomesByLead.get(group.leadId) ?? new Map(),
        nevers: group.nevers,
      });
      for (const s of states) {
        if (s.state !== "never") continue;
        effectiveLeads[s.step].add(group.leadId);
        if (group.email) effectiveEmails[s.step].add(group.email);
        if (s.origin === "implied") {
          impliedLeads[s.step].add(group.leadId);
          if (group.email) impliedEmails[s.step].add(group.email);
        }
      }
    }

    for (const step of LEAD_STEP_OUTCOMES) {
      impliedCounts[step] = impliedLeads[step].size;
      effectiveCounts[step] = effectiveLeads[step].size;
      impliedByStep[step] = Array.from(impliedEmails[step]);
      effectiveByStep[step] = Array.from(effectiveEmails[step]);
    }

    res.json({ counts, byStep, impliedCounts, impliedByStep, effectiveCounts, effectiveByStep });
  }),
);

export default router;
