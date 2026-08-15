/**
 * A campaign's IDENTITY is (org, brand, sales funnel, acquisition channel) — campaign-service's
 * own key (its `uniq_campaigns_org_brand_funnel_channel`, migration 0044). The WORKFLOW is not
 * part of it: selection re-picks a workflow every run, and campaign-service switches the workflow
 * of the campaign already alive on an identity instead of minting a second campaign.
 *
 * Before it did, so one brand grew dozens of stopped rows — one per workflow version — each
 * holding a slice of a history nobody could read as one campaign. Those rows stay: they carry real
 * serves keyed on their own campaign id in `leads_campaigns`. Nothing here rewrites or repoints any
 * of it; this module only decides which campaign ids answer to ONE customer-visible campaign, so a
 * campaign-scoped read totals the same population features-service already totals for the same
 * scope (its `src/lib/campaign-identity.ts` — this is the same grouping, byte for byte).
 *
 * ALL FOUR PARTS ARE READ FROM campaign-service, never re-derived. In particular the sales funnel
 * is never inferred from the campaign's goal: two funnels answer to the same goal
 * (`sales_meetings_from_conversation` and `sales_meetings_from_website` are both `meetingBooked`),
 * so a goal→funnel inference prints a chain the campaign never stated.
 */

/** A campaign row as campaign-service serves it, trimmed to the identity. */
export interface CampaignIdentityRow {
  id: string;
  orgId?: string | null;
  /** The identity's brand. Stored since campaign-service migration 0044; null on a row predating it. */
  brandId?: string | null;
  /** Legacy array the brand used to live in — read ONLY as a fallback for `brandId`. */
  brandIds?: string[] | null;
  /** The sales funnel the campaign states. NULL is a real state, not a gap to fill. */
  funnelKey?: string | null;
  /** Stored since migration 0044; null on a row predating it. */
  acquisitionChannel?: string | null;
  status?: string | null;
  createdAt?: string | null;
}

/**
 * A funnel a campaign never stated. Distinct from every real funnel key, so "this campaign sells
 * through nothing it declared" stays readable as its own state rather than collapsing onto a
 * stated funnel.
 *
 * Grouping the unstated ones together is campaign-service's OWN rule — its unique index keys on
 * `coalesce(funnel_key, '')` precisely so a brand cannot grow unlimited funnel-less campaigns on
 * one channel. Reading it any other way would disagree with the producer about what one campaign is.
 */
const FUNNEL_UNSTATED = " unstated-funnel";

/** One identity, and every campaign id that answers to it. */
export interface CampaignIdentity {
  key: string;
  campaignIds: string[];
}

/**
 * The resolved families for a brand/org, plus the lookup the reads need.
 *
 * A campaign we could not place — campaign-service does not know it, or the row predates migration
 * 0044 and states no brand / channel — is its OWN family of one. It is never folded onto another
 * identity on a guess, so the worst case is today's per-campaign-id behaviour.
 */
export interface CampaignFamilies {
  byCampaignId: Map<string, CampaignIdentity>;
  /** Every member of `campaignId`'s family, itself included. `[campaignId]` when unplaceable. */
  familyOf(campaignId: string): string[];
}

function brandOf(row: CampaignIdentityRow): string | null {
  return row.brandId ?? row.brandIds?.[0] ?? null;
}

/**
 * The identity key, or null when the row does not state enough of it to be pooled with anything.
 *
 * Null is deliberate: a row with no brand or no acquisition channel is one campaign-service could
 * not police either (its unique index skips exactly those), so pooling it here would invent an
 * identity the owner never asserted.
 */
export function identityKeyOf(row: CampaignIdentityRow): string | null {
  const brandId = brandOf(row);
  const channel = row.acquisitionChannel ?? null;
  if (!brandId || !channel) return null;
  const orgId = row.orgId ?? "";
  return `${orgId}|${brandId}|${row.funnelKey ?? FUNNEL_UNSTATED}|${channel}`;
}

/** Group campaign rows into identities. Pure — the network read lives in the client module. */
export function buildCampaignFamilies(rows: CampaignIdentityRow[]): CampaignFamilies {
  const byKey = new Map<string, CampaignIdentityRow[]>();
  const unplaceable: CampaignIdentityRow[] = [];

  for (const row of rows) {
    if (!row?.id) continue;
    const key = identityKeyOf(row);
    if (key === null) {
      unplaceable.push(row);
      continue;
    }
    const bucket = byKey.get(key);
    if (bucket) bucket.push(row);
    else byKey.set(key, [row]);
  }

  const byCampaignId = new Map<string, CampaignIdentity>();

  const register = (key: string, members: CampaignIdentityRow[]): void => {
    const campaignIds = Array.from(new Set(members.map((m) => m.id))).sort();
    const identity: CampaignIdentity = { key, campaignIds };
    for (const id of campaignIds) byCampaignId.set(id, identity);
  };

  for (const [key, members] of byKey) register(key, members);
  // Each unplaceable row is its own family of one, keyed on its own id so it can never collide
  // with a real identity key (which always carries three `|` separators and a channel).
  for (const row of unplaceable) register(`campaign:${row.id}`, [row]);

  return {
    byCampaignId,
    familyOf(campaignId: string): string[] {
      return byCampaignId.get(campaignId)?.campaignIds ?? [campaignId];
    },
  };
}
