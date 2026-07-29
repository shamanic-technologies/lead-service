/**
 * ONE-TIME served-lead email reconciliation.
 *
 * Why: until the email-owner-first identity fix, the serve path resolved a person
 * by provider person id FIRST. When a re-crawl minted a new provider id for a
 * human whose email was already registered against an older lead row, the serve
 * landed on a lead that could NEVER carry that email — the global
 * one-email-one-lead index (idx_lcm_channel_value) had already given it away.
 * The old code logged a warning and served the lead anyway.
 *
 * Consequence: the read path keys the email-gateway delivery overlay on the
 * lead's REGISTERED email, so those served rows can never resolve
 * contacted/sent/delivered. They are invisible in the dashboard funnel, absent
 * from outreach counts, and absent from features-service conversion attribution
 * — emails we paid for missing from every cost-per-outcome denominator.
 *
 * What: for every `status='served'` leads_campaigns row whose lead has NO email
 * contact method, read the email out of the lead's stored provider payload
 * (`leads.metadata->>'email'`, what the provider actually returned at serve
 * time) and repair it one of two ways:
 *
 *   A. email is FREE (no lead owns it)   -> register it on the row's current
 *                                           lead. lead_id is untouched.
 *   B. email is OWNED by another lead    -> re-point leads_campaigns.lead_id to
 *                                           the owning lead (that lead IS the
 *                                           person) and record the previous
 *                                           lead_id in repointed_from_lead_id.
 *
 * The row keeps every other column — campaign, brand_ids, audience_id, goal,
 * run ids, served_at — so its campaign/brand/audience attribution is preserved,
 * and the owning lead keeps all of its own history under other brands.
 *
 * Properties:
 *   - Dry-runnable   — `--dry-run` reports every bucket and writes nothing.
 *   - Idempotent     — a repaired row stops matching the orphan predicate (its
 *                      lead now has an email), so re-running converges to 0 and
 *                      never re-points a row twice.
 *   - Traceable      — case B records the previous lead_id in
 *                      leads_campaigns.repointed_from_lead_id; case A tags the
 *                      contact method `source='served-lead-email-repair'`.
 *   - Reversible     — UPDATE leads_campaigns
 *                        SET lead_id = repointed_from_lead_id,
 *                            repointed_from_lead_id = NULL
 *                      WHERE repointed_from_lead_id IS NOT NULL;
 *                      DELETE FROM lead_contact_methods
 *                      WHERE source = 'served-lead-email-repair';
 *   - Conflict-safe  — a row whose owning lead ALREADY has a membership row for
 *                      the same campaign would violate idx_lc_lead_campaign; it
 *                      is skipped and reported, never force-merged.
 *
 * Usage:
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/repair-served-lead-emails.ts --dry-run
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/repair-served-lead-emails.ts
 */

import { sql as leadSql } from "../src/db/index.js";

export const REPAIR_SOURCE = "served-lead-email-repair";

export function parseArgs(argv: string[]): { dryRun: boolean } {
  return { dryRun: argv.includes("--dry-run") };
}

export interface RepairPlan {
  /** served rows whose lead has no email contact method */
  orphans: number;
  /** ...of which the stored provider payload carries no email at all (unrepairable here) */
  noPayloadEmail: number;
  /** ...of which the payload email is free -> attach to the current lead (case A) */
  attachable: number;
  /** ...of which the payload email is owned by another lead -> re-point (case B) */
  repointable: number;
  /** ...of which re-pointing would violate (lead_id, campaign_id) -> skipped */
  conflicting: number;
}

/**
 * Served rows whose lead carries no email contact method, resolved against the
 * lead that owns the payload email (if any). One shared definition for the plan
 * and both write statements, so dry-run and apply always agree.
 */
const orphanCte = () => leadSql`
  orphan AS (
    SELECT lc.id AS lc_id,
           lc.lead_id,
           lc.campaign_id,
           nullif(l.metadata->>'email', '') AS payload_email
    FROM leads_campaigns lc
    JOIN leads l ON l.id = lc.lead_id
    WHERE lc.status = 'served'
      AND NOT EXISTS (
        SELECT 1 FROM lead_contact_methods m
        WHERE m.lead_id = lc.lead_id AND m.channel = 'email'
      )
  ),
  resolved AS (
    SELECT o.*,
           (SELECT m.lead_id FROM lead_contact_methods m
             WHERE m.channel = 'email' AND m.value = o.payload_email
             LIMIT 1) AS owner_lead_id
    FROM orphan o
  )`;

/** Count every bucket without writing. */
export async function buildPlan(): Promise<RepairPlan> {
  const rows = await leadSql<
    {
      orphans: string;
      no_payload_email: string;
      attachable: string;
      repointable: string;
      conflicting: string;
    }[]
  >`
    WITH ${orphanCte()}
    SELECT
      count(*)::text AS orphans,
      count(*) FILTER (WHERE payload_email IS NULL)::text AS no_payload_email,
      count(*) FILTER (WHERE payload_email IS NOT NULL AND owner_lead_id IS NULL)::text AS attachable,
      count(*) FILTER (
        WHERE owner_lead_id IS NOT NULL
          AND owner_lead_id <> lead_id
          AND NOT EXISTS (
            SELECT 1 FROM leads_campaigns x
            WHERE x.lead_id = resolved.owner_lead_id AND x.campaign_id = resolved.campaign_id
          )
      )::text AS repointable,
      count(*) FILTER (
        WHERE owner_lead_id IS NOT NULL
          AND owner_lead_id <> lead_id
          AND EXISTS (
            SELECT 1 FROM leads_campaigns x
            WHERE x.lead_id = resolved.owner_lead_id AND x.campaign_id = resolved.campaign_id
          )
      )::text AS conflicting
    FROM resolved`;

  const r = rows[0];
  return {
    orphans: Number(r?.orphans ?? "0"),
    noPayloadEmail: Number(r?.no_payload_email ?? "0"),
    attachable: Number(r?.attachable ?? "0"),
    repointable: Number(r?.repointable ?? "0"),
    conflicting: Number(r?.conflicting ?? "0"),
  };
}

/**
 * Case A — the payload email belongs to nobody: register it on the row's own
 * lead. Nothing is re-pointed; the lead simply gains the email it always had.
 */
export async function attachFreeEmails(): Promise<number> {
  const inserted = await leadSql<{ lead_id: string }[]>`
    WITH ${orphanCte()}
    INSERT INTO lead_contact_methods (lead_id, channel, value, status, source)
    SELECT DISTINCT ON (payload_email)
           lead_id, 'email'::text, payload_email, NULL::text, ${REPAIR_SOURCE}::text
    FROM resolved
    WHERE payload_email IS NOT NULL AND owner_lead_id IS NULL
    ORDER BY payload_email, lead_id
    ON CONFLICT DO NOTHING
    RETURNING lead_id`;
  return inserted.length;
}

/**
 * Case B — the payload email is owned by another lead: that lead IS the person,
 * so the membership row moves to it. Every other column is preserved, and the
 * previous lead_id is recorded for traceability/undo.
 */
export async function repointToEmailOwners(): Promise<number> {
  const updated = await leadSql<{ id: string }[]>`
    WITH ${orphanCte()}
    UPDATE leads_campaigns lc
    SET lead_id = r.owner_lead_id,
        repointed_from_lead_id = lc.lead_id,
        updated_at = now()
    FROM resolved r
    WHERE lc.id = r.lc_id
      AND r.owner_lead_id IS NOT NULL
      AND r.owner_lead_id <> r.lead_id
      AND NOT EXISTS (
        SELECT 1 FROM leads_campaigns x
        WHERE x.lead_id = r.owner_lead_id AND x.campaign_id = r.campaign_id
      )
    RETURNING lc.id`;
  return updated.length;
}

function logPlan(label: string, plan: RepairPlan): void {
  console.log(
    `[lead-service] ${label}: servedRowsWithoutEmail=${plan.orphans} ` +
      `attachable=${plan.attachable} repointable=${plan.repointable} ` +
      `conflicting=${plan.conflicting} noPayloadEmail=${plan.noPayloadEmail}`,
  );
}

async function main(): Promise<void> {
  const { dryRun } = parseArgs(process.argv.slice(2));
  console.log(`[lead-service] served-lead email repair start dryRun=${dryRun}`);

  try {
    const plan = await buildPlan();
    logPlan("before", plan);

    if (dryRun) {
      console.log("[lead-service] DRY RUN — no writes performed");
      return;
    }

    const repointed = await repointToEmailOwners();
    const attached = await attachFreeEmails();
    console.log(
      `[lead-service] repair done — repointed=${repointed} attachedFreeEmails=${attached}`,
    );

    const after = await buildPlan();
    logPlan("after", after);
    if (after.orphans > 0) {
      console.warn(
        `[lead-service] WARNING: ${after.orphans} served rows still have no registered email ` +
          `(conflicting=${after.conflicting} noPayloadEmail=${after.noPayloadEmail})`,
      );
    } else {
      console.log("[lead-service] verified: every served row now resolves an email");
    }
  } finally {
    await leadSql.end();
  }
}

// Only auto-run when invoked directly (not when imported by tests).
const invokedDirectly = process.argv[1]?.includes("repair-served-lead-emails");
if (invokedDirectly) {
  main().catch((err) => {
    console.error("[lead-service] served-lead email repair failed:", err);
    process.exit(1);
  });
}
