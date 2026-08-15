import { sql } from "../db/index.js";

// Shared scope for the two `GET /orgs/leads` code paths (slim `?view=basic` and the
// default full path). Mirrors the headers/query the route reads. `leads_campaigns`
// holds one row per (person × campaign-membership/serve), so a person engaged across
// N campaigns for a brand otherwise appears N times in a brand/org-scoped list.
export interface LeadListScope {
  orgId: string;
  brandId?: string;
  /** The campaign the caller asked for. Kept for logging/telemetry; the FILTER is campaignIds. */
  campaignId?: string;
  /**
   * The campaign IDENTITY's members — every stored campaign row the customer reads as the ONE
   * campaign they asked for (see campaign-identity.ts). Resolved by the route from campaign-service;
   * `[campaignId]` when the identity has a single member or could not be resolved. Absent means the
   * read is not campaign-scoped at all.
   */
  campaignIds?: string[];
  queryOrgId?: string;
  userId?: string;
  workflowSlug?: string;
}

/** The campaign ids a scope filters on, or null when the read is brand/org-scoped. */
export function campaignScopeIds(f: LeadListScope): string[] | null {
  if (f.campaignIds && f.campaignIds.length > 0) return f.campaignIds;
  return f.campaignId ? [f.campaignId] : null;
}

// Brand/org-scoped reads must be ONE row per PERSON, not one per campaign-membership.
// A campaign scope resolving to a SINGLE stored row is already ~1 row per person, so it stays flat
// — that also guarantees a genuine per-campaign membership row is never silently collapsed.
// A MULTI-member identity is the same shape as brand scope (one person can hold a membership row
// under several of the identity's stopped ancestors), so it collapses the same way: without this a
// campaign-scoped count would exceed the brand-scoped one for the same population.
export function shouldDedupeLeadList(f: LeadListScope): boolean {
  const ids = campaignScopeIds(f);
  return ids === null || ids.length > 1;
}

// Base relation aliased as `lc` for the list queries.
//
// For brand/org scope, collapse `leads_campaigns` to the single winning membership per
// `lead_id` via DISTINCT ON. The winner is the most-advanced lifecycle row (served >
// claimed > buffered > skipped) so the kept row fires the served-only delivery overlay
// whenever the person was served under ANY campaign; ties break on latest served_at,
// latest created_at, then stable id. The delivery overlay is keyed by EMAIL at brand
// scope and is identical across a person's rows, so whichever membership wins carries
// the person's full brand-level engagement (clicked/opened/replied OR-merged inherently).
//
// A campaign scope carrying SEVERAL stored rows (one campaign IDENTITY, its live row plus the
// stopped ancestors it kept switching workflows through) takes the same collapse: the same person
// can hold a membership row under several members, and the delivery overlay for that scope is
// keyed by email across the whole family, so the winning row carries the person's engagement there
// too. A single-row campaign scope stays flat, byte for byte as before.
//
// The DISTINCT ON must be GLOBAL (computed over the whole filtered set), so it lives in
// a subquery; the outer query then keyset-paginates / orders the DEDUPED relation by
// (created_at, id). Scope filters are applied here AND on the outer WHERE — duplicate
// predicates on the deduped relation are a harmless no-op, but they are REQUIRED on the
// outer query for the non-deduped (single-campaign) path.
export function leadCampaignBaseRelation(f: LeadListScope) {
  const campaignIds = campaignScopeIds(f);
  if (!shouldDedupeLeadList(f)) {
    return sql`leads_campaigns lc`;
  }
  return sql`(
    SELECT DISTINCT ON (lc0.lead_id) lc0.*
    FROM leads_campaigns lc0
    WHERE lc0.org_id = ${f.orgId}
      ${f.brandId ? sql`AND ${f.brandId} = ANY(lc0.brand_ids)` : sql``}
      ${campaignIds ? sql`AND lc0.campaign_id = ANY(${campaignIds})` : sql``}
      ${f.queryOrgId ? sql`AND lc0.org_id = ${f.queryOrgId}` : sql``}
      ${f.userId ? sql`AND lc0.user_id = ${f.userId}` : sql``}
      ${f.workflowSlug ? sql`AND lc0.workflow_slug = ${f.workflowSlug}` : sql``}
    ORDER BY lc0.lead_id,
      CASE lc0.status
        WHEN 'served' THEN 3
        WHEN 'claimed' THEN 2
        WHEN 'buffered' THEN 1
        ELSE 0
      END DESC,
      lc0.served_at DESC NULLS LAST,
      lc0.created_at DESC,
      lc0.id DESC
  ) lc`;
}
