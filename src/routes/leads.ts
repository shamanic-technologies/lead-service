import { Router } from "express";
import { type AuthenticatedRequest, apiKeyAuth, requireOrgId, getServiceContext } from "../middleware/auth.js";
import { sql } from "../db/index.js";
import {
  checkDeliveryStatus,
  type StatusResult,
  type DeliveryStatusItem,
  type ProviderStatus,
  type ScopedStatus,
  type GlobalStatus,
} from "../lib/email-gateway-client.js";
import { resolveCampaignFamily } from "../lib/campaign-identity-client.js";
import { traceEvent } from "../lib/trace-event.js";
import { buildFullLeadsBatch, type FullLead } from "../lib/lead-shape.js";
import { streamBasicLeadChunks, toIsoTimestamp, type BasicLeadRow } from "../lib/basic-leads.js";
import {
  campaignScopeIds,
  encodeLeadCursor,
  leadCampaignBaseRelation,
  leadStatusScope,
  parseLeadListPage,
  parseLeadStatusFilter,
  type LeadListCursor,
  type LeadListPage,
  type LeadListScope,
} from "../lib/lead-list-query.js";
import { resolveAudiencesForBrand, type AudienceCard, type AudienceResolveContext } from "../lib/audience-client.js";

const router = Router();

interface FlattenedStatus {
  contacted: boolean;
  sent: boolean;
  delivered: boolean;
  opened: boolean;
  clicked: boolean;
  bounced: boolean;
  unsubscribed: boolean;
  replied: boolean;
  replyClassification: "positive" | "negative" | "neutral" | null;
  sentCount: number;
  lastDeliveredAt: string | null;
  firstContactedAt: string | null;
  firstSentAt: string | null;
  firstDeliveredAt: string | null;
  firstOpenedAt: string | null;
  firstClickedAt: string | null;
  firstRepliedAt: string | null;
  firstBouncedAt: string | null;
  firstUnsubscribedAt: string | null;
  global: { bounced: boolean; unsubscribed: boolean };
}

/** First-occurrence (MIN) merge: earliest non-null ISO timestamp across providers. */
function earliestIso(a: string | null, b: string | null): string | null {
  if (!a) return b;
  if (!b) return a;
  return a <= b ? a : b;
}

function pickScoped(s: ScopedStatus | null | undefined) {
  return {
    contacted: !!s?.contacted,
    sent: !!s?.sent,
    delivered: !!s?.delivered,
    opened: !!s?.opened,
    clicked: !!s?.clicked,
    bounced: !!s?.bounced,
    unsubscribed: !!s?.unsubscribed,
    replied: !!s?.replied,
    replyClassification: s?.replyClassification ?? null,
    sentCount: s?.sentCount ?? 0,
    lastDeliveredAt: s?.lastDeliveredAt ?? null,
    firstContactedAt: s?.firstContactedAt ?? null,
    firstSentAt: s?.firstSentAt ?? null,
    firstDeliveredAt: s?.firstDeliveredAt ?? null,
    firstOpenedAt: s?.firstOpenedAt ?? null,
    firstClickedAt: s?.firstClickedAt ?? null,
    firstRepliedAt: s?.firstRepliedAt ?? null,
    firstBouncedAt: s?.firstBouncedAt ?? null,
    firstUnsubscribedAt: s?.firstUnsubscribedAt ?? null,
  };
}

function mergeGlobal(bc?: GlobalStatus | null, tx?: GlobalStatus | null) {
  return {
    bounced: !!(bc?.email?.bounced || tx?.email?.bounced),
    unsubscribed: !!(bc?.email?.unsubscribed || tx?.email?.unsubscribed),
  };
}

function mergeProviders(
  bcScope: ReturnType<typeof pickScoped>,
  txScope: ReturnType<typeof pickScoped>,
): Omit<FlattenedStatus, "global"> {
  return {
    contacted: bcScope.contacted || txScope.contacted,
    sent: bcScope.sent || txScope.sent,
    delivered: bcScope.delivered || txScope.delivered,
    opened: bcScope.opened || txScope.opened,
    clicked: bcScope.clicked || txScope.clicked,
    bounced: bcScope.bounced || txScope.bounced,
    unsubscribed: bcScope.unsubscribed || txScope.unsubscribed,
    replied: bcScope.replied || txScope.replied,
    replyClassification: bcScope.replyClassification ?? txScope.replyClassification ?? null,
    // Broadcast (Instantly) and transactional (Postmark) are disjoint sending
    // channels, so the total emails sent to this lead = sum across providers.
    sentCount: bcScope.sentCount + txScope.sentCount,
    lastDeliveredAt: bcScope.lastDeliveredAt ?? txScope.lastDeliveredAt ?? null,
    firstContactedAt: earliestIso(bcScope.firstContactedAt, txScope.firstContactedAt),
    firstSentAt: earliestIso(bcScope.firstSentAt, txScope.firstSentAt),
    firstDeliveredAt: earliestIso(bcScope.firstDeliveredAt, txScope.firstDeliveredAt),
    firstOpenedAt: earliestIso(bcScope.firstOpenedAt, txScope.firstOpenedAt),
    firstClickedAt: earliestIso(bcScope.firstClickedAt, txScope.firstClickedAt),
    firstRepliedAt: earliestIso(bcScope.firstRepliedAt, txScope.firstRepliedAt),
    firstBouncedAt: earliestIso(bcScope.firstBouncedAt, txScope.firstBouncedAt),
    firstUnsubscribedAt: earliestIso(bcScope.firstUnsubscribedAt, txScope.firstUnsubscribedAt),
  };
}

export function flattenCampaignStatus(result: StatusResult): FlattenedStatus {
  const bc = result.broadcast;
  const tx = result.transactional;
  const merged = mergeProviders(pickScoped(bc?.campaign), pickScoped(tx?.campaign));
  if (bc?.brand?.contacted || tx?.brand?.contacted) merged.contacted = true;
  return { ...merged, global: mergeGlobal(bc?.global, tx?.global) };
}

/**
 * Collapse one provider's per-campaign breakdown down to the members of ONE campaign identity.
 *
 * email-gateway keys its evidence on the campaign id that sent the email, so a person served under
 * a stopped ancestor of the identity has no evidence under the LIVE campaign id — asking in
 * campaign mode for that one id answers "never contacted" for a person the customer paid to
 * contact. Brand mode returns `byCampaign`, so the identity's own members are read from it and
 * nothing outside the identity is counted (a brand-scope answer would over-report a brand running
 * several identities). Booleans OR, `sentCount` sums (disjoint campaigns), `first*At` take the
 * earliest, `lastDeliveredAt` the latest, and the reply classification comes from the member that
 * replied most recently.
 */
function aggregateFamilyScope(
  provider: ProviderStatus | undefined,
  family: Set<string>,
): ScopedStatus | null {
  const byCampaign = provider?.byCampaign;
  if (!byCampaign) return null;

  const scopes = Object.entries(byCampaign)
    .filter(([campaignId]) => family.has(campaignId))
    .map(([, scope]) => scope)
    .filter((scope): scope is ScopedStatus => !!scope);
  if (scopes.length === 0) return null;

  const latestIso = (a: string | null, b: string | null): string | null => {
    if (!a) return b;
    if (!b) return a;
    return a >= b ? a : b;
  };

  let repliedAt: string | null = null;
  let replyClassification: ScopedStatus["replyClassification"] = null;
  for (const scope of scopes) {
    if (!scope.replyClassification) continue;
    if (repliedAt === null || (scope.firstRepliedAt ?? "") >= repliedAt) {
      repliedAt = scope.firstRepliedAt ?? "";
      replyClassification = scope.replyClassification;
    }
  }

  return scopes.reduce<ScopedStatus>(
    (acc, s) => ({
      contacted: acc.contacted || s.contacted,
      sent: acc.sent || s.sent,
      delivered: acc.delivered || s.delivered,
      opened: acc.opened || s.opened,
      clicked: acc.clicked || s.clicked,
      replied: acc.replied || s.replied,
      replyClassification,
      bounced: acc.bounced || s.bounced,
      unsubscribed: acc.unsubscribed || s.unsubscribed,
      sentCount: (acc.sentCount ?? 0) + (s.sentCount ?? 0),
      lastDeliveredAt: latestIso(acc.lastDeliveredAt, s.lastDeliveredAt),
      firstContactedAt: earliestIso(acc.firstContactedAt, s.firstContactedAt),
      firstSentAt: earliestIso(acc.firstSentAt, s.firstSentAt),
      firstDeliveredAt: earliestIso(acc.firstDeliveredAt, s.firstDeliveredAt),
      firstOpenedAt: earliestIso(acc.firstOpenedAt, s.firstOpenedAt),
      firstClickedAt: earliestIso(acc.firstClickedAt, s.firstClickedAt),
      firstRepliedAt: earliestIso(acc.firstRepliedAt, s.firstRepliedAt),
      firstBouncedAt: earliestIso(acc.firstBouncedAt, s.firstBouncedAt),
      firstUnsubscribedAt: earliestIso(acc.firstUnsubscribedAt, s.firstUnsubscribedAt),
    }),
    {
      contacted: false, sent: false, delivered: false, opened: false, clicked: false,
      replied: false, replyClassification, bounced: false, unsubscribed: false, sentCount: 0,
      lastDeliveredAt: null, firstContactedAt: null, firstSentAt: null, firstDeliveredAt: null,
      firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null,
      firstUnsubscribedAt: null,
    },
  );
}

/** The campaign-scope flatten for a campaign identity that spans several stored campaign rows. */
export function flattenFamilyStatus(result: StatusResult, family: Set<string>): FlattenedStatus {
  const bc = result.broadcast;
  const tx = result.transactional;
  const merged = mergeProviders(
    pickScoped(aggregateFamilyScope(bc, family)),
    pickScoped(aggregateFamilyScope(tx, family)),
  );
  // Same widening the single-campaign flatten applies: a person contacted anywhere for the brand
  // reads as contacted, so the campaign page never claims an untouched person we did reach.
  if (bc?.brand?.contacted || tx?.brand?.contacted) merged.contacted = true;
  return { ...merged, global: mergeGlobal(bc?.global, tx?.global) };
}

export function flattenBrandStatus(result: StatusResult): FlattenedStatus {
  const bc = result.broadcast;
  const tx = result.transactional;
  const merged = mergeProviders(pickScoped(bc?.brand), pickScoped(tx?.brand));
  return { ...merged, global: mergeGlobal(bc?.global, tx?.global) };
}

const DEFAULT_STATUS: FlattenedStatus = {
  contacted: false, sent: false, delivered: false, opened: false, clicked: false,
  bounced: false, unsubscribed: false, replied: false, replyClassification: null, sentCount: 0, lastDeliveredAt: null,
  firstContactedAt: null, firstSentAt: null, firstDeliveredAt: null, firstOpenedAt: null,
  firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null,
  global: { bounced: false, unsubscribed: false },
};

// A single brand can carry 50k+ leads_campaigns rows. Loading every row before
// streaming still OOMs even if hydration/JSON writes are chunked. Read, hydrate,
// overlay delivery status, and serialize one chunk at a time so peak memory is
// bounded by LEADS_STREAM_CHUNK_SIZE regardless of the brand's lead count.
// The wire shape is byte-identical to the old res.json({ leads }) — `{"leads":[...]}`.
const LEADS_STREAM_CHUNK_SIZE = Math.max(1, Number(process.env.LEADS_STREAM_CHUNK_SIZE) || 500);

// postgres.js returns timestamptz as Date OR string depending on the path; the cursor carries
// whichever came back and normalizes at encode time (see LeadListCursor).
type LeadCampaignCursor = LeadListCursor;

interface RawLeadCampaignRow {
  id: string;
  lead_id: string;
  campaign_id: string;
  org_id: string;
  user_id: string | null;
  brand_ids: string[];
  status: string;
  status_reason: string | null;
  status_details: string | null;
  parent_run_id: string | null;
  run_id: string | null;
  // postgres.js returns timestamptz as Date OR string depending on the path; normalize via toIsoTimestamp.
  served_at: Date | string | null;
  workflow_slug: string | null;
  feature_slug: string | null;
  goal: string | null;
  active_goal_id: string | null;
  brand_profile_id: string | null;
  audience_id: string | null;
  created_at: Date | string;
  lead_apollo_person_id: string | null;
}

interface LeadCampaignRow {
  id: string;
  leadId: string;
  campaignId: string;
  orgId: string;
  userId: string | null;
  brandIds: string[];
  status: string;
  statusReason: string | null;
  statusDetails: string | null;
  parentRunId: string | null;
  runId: string | null;
  servedAt: string | null;
  workflowSlug: string | null;
  featureSlug: string | null;
  goal: string | null;
  activeGoalId: string | null;
  brandProfileId: string | null;
  audienceId: string | null;
  createdAt: Date | string;
  leadApolloPersonId: string | null;
}

// Brand/org scope collapses leads_campaigns to one row per lead_id (see
// leadCampaignBaseRelation); campaign scope stays flat. Keyset-paginate the DEDUPED
// relation by (created_at, id) so dedup is GLOBAL, not per-chunk. Scope filters live
// both inside the dedup subquery (so the winner is chosen within scope) and on the
// outer WHERE (required for the non-deduped campaign path; a no-op for the dedup path).
/**
 * The ONE membership row a caller names, or null.
 *
 * Addressed by the row's own `id` — the identity every list row already carries as `id` — so the
 * caller needs nothing it did not get from the list. There is no dedup and no lifecycle filter
 * here: the caller is naming a row it has already been shown, not asking which of a person's rows
 * wins. `org_id` is the entitlement boundary and is part of the predicate rather than a check
 * afterwards, so a row belonging to another org is indistinguishable from one that does not exist.
 */
async function fetchLeadCampaignRowById(
  orgId: string,
  id: string,
): Promise<LeadCampaignRow | null> {
  const rows = await sql<RawLeadCampaignRow[]>`
    SELECT
      lc.id, lc.lead_id, lc.campaign_id, lc.org_id, lc.user_id, lc.brand_ids,
      lc.status, lc.status_reason, lc.status_details, lc.parent_run_id, lc.run_id,
      lc.served_at, lc.workflow_slug, lc.feature_slug, lc.goal, lc.active_goal_id,
      lc.brand_profile_id, lc.audience_id, lc.created_at,
      l.apollo_person_id AS lead_apollo_person_id
    FROM leads_campaigns lc
    LEFT JOIN leads l ON l.id = lc.lead_id
    WHERE lc.id = ${id} AND lc.org_id = ${orgId}
    LIMIT 1
  `;
  return mapLeadCampaignRows(rows)[0] ?? null;
}

/** Shared raw-row → camelCase mapping for both list and detail reads. */
function mapLeadCampaignRows(rows: RawLeadCampaignRow[]): LeadCampaignRow[] {
  return rows.map((r) => ({
    id: r.id,
    leadId: r.lead_id,
    campaignId: r.campaign_id,
    orgId: r.org_id,
    userId: r.user_id,
    brandIds: r.brand_ids,
    status: r.status,
    statusReason: r.status_reason,
    statusDetails: r.status_details,
    parentRunId: r.parent_run_id,
    runId: r.run_id,
    servedAt: toIsoTimestamp(r.served_at),
    workflowSlug: r.workflow_slug,
    featureSlug: r.feature_slug,
    goal: r.goal,
    activeGoalId: r.active_goal_id,
    brandProfileId: r.brand_profile_id,
    audienceId: r.audience_id,
    createdAt: r.created_at,
    leadApolloPersonId: r.lead_apollo_person_id,
  }));
}

async function fetchLeadCampaignChunk(
  scope: LeadListScope,
  cursor: LeadCampaignCursor | null,
  limit: number = LEADS_STREAM_CHUNK_SIZE,
  offset: number | null = null,
): Promise<LeadCampaignRow[]> {
  const rows = await sql<RawLeadCampaignRow[]>`
    SELECT
      lc.id, lc.lead_id, lc.campaign_id, lc.org_id, lc.user_id, lc.brand_ids,
      lc.status, lc.status_reason, lc.status_details, lc.parent_run_id, lc.run_id,
      lc.served_at, lc.workflow_slug, lc.feature_slug, lc.goal, lc.active_goal_id,
      lc.brand_profile_id, lc.audience_id, lc.created_at,
      l.apollo_person_id AS lead_apollo_person_id
    FROM ${leadCampaignBaseRelation(scope)}
    LEFT JOIN leads l ON l.id = lc.lead_id
    WHERE lc.org_id = ${scope.orgId}
      ${scope.brandId ? sql`AND ${scope.brandId} = ANY(lc.brand_ids)` : sql``}
      ${campaignScopeIds(scope) ? sql`AND lc.campaign_id = ANY(${campaignScopeIds(scope)!})` : sql``}
      ${leadStatusScope(scope) ? sql`AND lc.status = ANY(${leadStatusScope(scope)!})` : sql``}
      ${scope.queryOrgId ? sql`AND lc.org_id = ${scope.queryOrgId}` : sql``}
      ${scope.userId ? sql`AND lc.user_id = ${scope.userId}` : sql``}
      ${scope.workflowSlug ? sql`AND lc.workflow_slug = ${scope.workflowSlug}` : sql``}
      ${cursor ? sql`AND (lc.created_at, lc.id) > (${cursor.createdAt}, ${cursor.id})` : sql``}
    ORDER BY lc.created_at ASC, lc.id ASC
    LIMIT ${limit}
    ${offset == null || offset === 0 ? sql`` : sql`OFFSET ${offset}`}
  `;

  return mapLeadCampaignRows(rows);
}

// Resolve each lead's ACTIVE audience for its brand, server-to-server via
// human-service. Runs per chunk (bounded set), grouped by the brand the audience
// must be correct for: the explicitly-scoped brandId when present (the dashboard
// leads page always scopes by brand), else the row's primary brand. Both keys the
// lead carries — its tagged audienceId (~5% of rows) AND its email (historical
// coverage) — are forwarded; human-service owns the brand-correct pick. Fail-loud:
// a resolver failure rejects and aborts the request (never a silently-blank field).
interface AudienceRowDescriptor {
  leadId: string;
  email: string | null;
  audienceId: string | null;
  brandIds: string[];
}

async function buildAudienceMapForRows(
  descriptors: AudienceRowDescriptor[],
  scopeBrandId: string | undefined,
  ctx: AudienceResolveContext,
): Promise<Map<string, AudienceCard>> {
  // Group rows by the brand the audience must be correct for: the explicitly-
  // scoped brandId when present, else the row's primary brand. human-service
  // resolves by DISTINCT audienceId + email arrays, so per group we send the
  // deduped key sets and correlate the two returned maps back onto each lead.
  const groups = new Map<string, AudienceRowDescriptor[]>();
  for (const d of descriptors) {
    const brandId = scopeBrandId ?? d.brandIds[0];
    if (!brandId) continue;
    if (!groups.has(brandId)) groups.set(brandId, []);
    groups.get(brandId)!.push(d);
  }

  const merged = new Map<string, AudienceCard>();
  await Promise.all(
    Array.from(groups.entries()).map(async ([brandId, rows]) => {
      const audienceIds = Array.from(
        new Set(rows.map((r) => r.audienceId).filter((x): x is string => !!x)),
      );
      const emails = Array.from(
        new Set(rows.map((r) => r.email).filter((x): x is string => !!x)),
      );

      const { byAudienceId, byEmail } = await resolveAudiencesForBrand(
        brandId,
        { audienceIds, emails },
        ctx,
      );

      for (const r of rows) {
        // Prefer the tagged audience's card; fall back to the email membership
        // when the tag is absent / not this brand / retired (null).
        const byTag = r.audienceId ? byAudienceId[r.audienceId] : null;
        const byMail = r.email ? byEmail[r.email] : null;
        const card = byTag ?? byMail ?? null;
        if (card) merged.set(r.leadId, card);
      }
    }),
  );
  return merged;
}

/**
 * The campaign id to ask email-gateway for: the single stored row when the scope is one campaign,
 * and NOTHING for a multi-member identity — brand mode is what returns the per-campaign breakdown
 * the identity is then read out of (see aggregateFamilyScope).
 */
function statusCampaignId(campaignIds: string[] | null): string | undefined {
  return campaignIds?.length === 1 ? campaignIds[0] : undefined;
}

async function buildStatusMapForBasicRows(
  rows: BasicLeadRow[],
  campaignId: string | undefined,
  context: ReturnType<typeof getServiceContext>,
) {
  const statusMap = new Map<string, StatusResult>();
  const groups = new Map<string, { brandId: string; items: DeliveryStatusItem[] }>();

  for (const row of rows) {
    if (row.status !== "served") continue;
    const email = row.email?.value;
    if (!email) continue;
    const primaryBrandId = row.brandIds[0] ?? "unknown";
    if (!groups.has(primaryBrandId)) {
      groups.set(primaryBrandId, { brandId: primaryBrandId, items: [] });
    }
    groups.get(primaryBrandId)!.items.push({ email });
  }

  await Promise.all(
    Array.from(groups.values()).map(async (group) => {
      const response = await checkDeliveryStatus(group.brandId, campaignId, group.items, context);
      for (const result of response.results) statusMap.set(result.email, result);
    }),
  );

  return statusMap;
}

/**
 * One element of the list response, and the whole of the detail response.
 *
 * The two routes MUST emit the same object for the same row — a detail panel is rendered from
 * whichever of the two the consumer happened to read, so a field that exists on one and not the
 * other is a panel that changes shape depending on how it was loaded. Building it in one place is
 * what makes "the record carries everything the list carries" true by construction rather than by
 * review.
 */
function serializeLeadItem(
  row: LeadCampaignRow,
  fullLead: FullLead | null,
  email: { value: string; status: string | null } | null,
  audience: AudienceCard | null,
  deliveryStatus: FlattenedStatus,
) {
  return {
    id: row.id,
    leadId: row.leadId,
    namespace: "apollo",
    email: email?.value ?? "",
    apolloPersonId: row.leadApolloPersonId ?? null,
    parentRunId: row.parentRunId,
    runId: row.runId,
    brandIds: row.brandIds,
    campaignId: row.campaignId,
    orgId: row.orgId,
    userId: row.userId ?? null,
    workflowSlug: row.workflowSlug ?? null,
    featureSlug: row.featureSlug ?? null,
    goal: row.goal ?? null,
    activeGoalId: row.activeGoalId ?? null,
    brandProfileId: row.brandProfileId ?? null,
    audienceId: row.audienceId ?? null,
    audience: audience ?? null,
    servedAt: row.servedAt,
    status: row.status as "buffered" | "skipped" | "claimed" | "served",
    emailStatus: email?.status ?? null,
    lead: fullLead,
    statusReason: row.statusReason ?? null,
    statusDetails: row.statusDetails ?? null,
    ...deliveryStatus,
  };
}

/**
 * Where the caller resumes, or null when this response is the end of the population.
 *
 * A bounded read that came back FULL may have more behind it, so it carries the position of its
 * last row; the caller passes it back as `?cursor=` and continues from strictly after it. A read
 * that came back short (or was never bounded) has reached the end, and says so with null. A walk
 * that lands exactly on the last row gets one more request that returns zero rows and a null
 * cursor — the cost of not counting the whole population on every page.
 */
function nextCursorFor(
  page: LeadListPage,
  rowCount: number,
  last: LeadListCursor | null,
): string | null {
  if (page.limit === null || last === null) return null;
  if (rowCount < page.limit) return null;
  return encodeLeadCursor(last);
}

router.get("/orgs/leads", apiKeyAuth, requireOrgId, async (req: AuthenticatedRequest, res) => {
  let streamingStarted = false;
  try {
    if (req.runId) {
      traceEvent(req.runId, { service: "lead-service", event: "leads-query-start", detail: `orgId=${req.orgId}` }, req.headers).catch(() => {});
    }

    const { brandId, campaignId, orgId: queryOrgId, userId, workflowSlug } = req.query;
    const campaignIdStr = typeof campaignId === "string" ? campaignId : undefined;
    const brandIdStr = typeof brandId === "string" ? brandId : undefined;
    const queryOrgIdStr = typeof queryOrgId === "string" ? queryOrgId : undefined;
    const userIdStr = typeof userId === "string" ? userId : undefined;
    const workflowSlugStr = typeof workflowSlug === "string" ? workflowSlug : undefined;

    // Which lifecycle statuses this read answers for. Absent means the actionable population
    // (DEFAULT_LEAD_LIST_STATUSES — everything but `skipped`); `?status=all` or an explicit list
    // asks for more. A bad value is a 400 before any work starts, never a silent fallback.
    let statuses: readonly string[];
    let page: LeadListPage;
    try {
      statuses = parseLeadStatusFilter(req.query.status);
      // How much of that population to return, and where to start. Absent `limit` means the whole
      // thing — the read every caller got before bounds existed, and what the staff console still
      // asks for. Anything unreadable is a 400 before any work starts: a bound that is accepted
      // and dropped is the bug being fixed here, so nothing about paging fails quietly.
      page = parseLeadListPage(req.query);
    } catch (error) {
      return res.status(400).json({ error: error instanceof Error ? error.message : String(error) });
    }

    // A campaign as the customer knows it is an IDENTITY (org, brand, sales funnel, acquisition
    // channel), not one stored campaign row: campaign-service used to mint a new row on every
    // workflow switch, so one campaign lives in storage as many. A campaign-scoped read totals the
    // whole identity — the same population features-service already totals for the same scope, so
    // the money and the people on one campaign page describe the same thing. Resolution failing
    // falls back to the single requested row, loudly, never to a partial family or the brand.
    const campaignFamily = campaignIdStr
      ? await resolveCampaignFamily(campaignIdStr, {
          orgId: req.orgId!,
          userId: req.userId ?? null,
          runId: req.runId ?? null,
          brandId: brandIdStr ?? null,
        })
      : null;

    // One shared scope for both the slim (`?view=basic`) and full paths. Brand/org scope — and a
    // campaign identity spanning several rows — collapses to one row per lead_id downstream; a
    // single-row campaign scope stays flat.
    const scope: LeadListScope = {
      orgId: req.orgId!,
      brandId: brandIdStr,
      campaignId: campaignIdStr,
      campaignIds: campaignFamily ?? undefined,
      queryOrgId: queryOrgIdStr,
      userId: userIdStr,
      workflowSlug: workflowSlugStr,
      statuses,
    };

    const scopeCampaignIds = campaignScopeIds(scope);
    const statusCampaignIdStr = statusCampaignId(scopeCampaignIds);
    const familySet =
      scopeCampaignIds && scopeCampaignIds.length > 1 ? new Set(scopeCampaignIds) : null;

    const hasScopeForStatus = !!(campaignIdStr || brandIdStr);
    const flatten = familySet
      ? (result: StatusResult) => flattenFamilyStatus(result, familySet)
      : campaignIdStr
        ? flattenCampaignStatus
        : flattenBrandStatus;
    const context = getServiceContext(req);
    const audienceCtx: AudienceResolveContext = {
      orgId: req.orgId!,
      userId: req.userId ?? null,
      runId: req.runId ?? null,
    };
    // `?view=basic` => slim per-lead payload. Anything else (incl. absent) => full
    // FullLead, the existing default. No Zod default: a missing param is full.
    const slim = req.query.view === "basic";

    // Basic view: ONE flat query (current-employer org + primary email via LATERAL),
    // streamed in cursor chunks. This keeps the list shape compatible with api-service
    // while avoiding the "load a whole large brand before first byte" failure mode.
    if (slim) {
      res.setHeader("Content-Type", "application/json");
      res.write('{"leads":[');
      streamingStarted = true;

      let wroteFirstBasic = false;
      let rowCount = 0;
      let lastPosition: LeadListCursor | null = null;
      for await (const basicRows of streamBasicLeadChunks(scope, LEADS_STREAM_CHUNK_SIZE, page)) {
        rowCount += basicRows.length;
        const lastBasic = basicRows[basicRows.length - 1];
        if (lastBasic) lastPosition = { createdAt: lastBasic.createdAt, id: lastBasic.id };

        const statusMap = hasScopeForStatus
          ? await buildStatusMapForBasicRows(basicRows, statusCampaignIdStr, context)
          : new Map<string, StatusResult>();

        const audienceMap = await buildAudienceMapForRows(
          basicRows.map((r) => ({
            leadId: r.leadId,
            email: r.email?.value ?? null,
            audienceId: r.audienceId,
            brandIds: r.brandIds,
          })),
          brandIdStr,
          audienceCtx,
        );

        for (const r of basicRows) {
          const emailValue = r.email?.value ?? "";
          const emailStatus = r.email?.status ?? null;
          const statusResult = statusMap.get(emailValue);
          const deliveryStatus = hasScopeForStatus && r.status === "served"
            ? (statusResult ? flatten(statusResult) : DEFAULT_STATUS)
            : DEFAULT_STATUS;

          // The slim path's `lead` is the basic projection, not a FullLead — that is the whole
          // point of `?view=basic`, so it is passed through as-is rather than through the shared
          // FullLead-typed serializer.
          const leadOut = {
            id: r.id,
            leadId: r.leadId,
            namespace: "apollo",
            email: emailValue,
            apolloPersonId: r.leadApolloPersonId ?? null,
            parentRunId: r.parentRunId,
            runId: r.runId,
            brandIds: r.brandIds,
            campaignId: r.campaignId,
            orgId: r.orgId,
            userId: r.userId ?? null,
            workflowSlug: r.workflowSlug ?? null,
            featureSlug: r.featureSlug ?? null,
            goal: r.goal ?? null,
            activeGoalId: r.activeGoalId ?? null,
            brandProfileId: r.brandProfileId ?? null,
            audienceId: r.audienceId ?? null,
            audience: audienceMap.get(r.leadId) ?? null,
            servedAt: r.servedAt,
            status: r.status as "buffered" | "skipped" | "claimed" | "served",
            emailStatus,
            lead: r.lead,
            statusReason: r.statusReason ?? null,
            statusDetails: r.statusDetails ?? null,
            ...deliveryStatus,
          };

          res.write((wroteFirstBasic ? "," : "") + JSON.stringify(leadOut));
          wroteFirstBasic = true;
        }
      }

      res.write(`],"nextCursor":${JSON.stringify(nextCursorFor(page, rowCount, lastPosition))}}`);
      res.end();

      if (req.runId) {
        traceEvent(req.runId, { service: "lead-service", event: "leads-query-done", detail: `count=${rowCount}`, data: { count: rowCount } }, req.headers).catch(() => {});
      }
      return;
    }

    const primaryEmail = (lead: FullLead | undefined): { value: string; status: string | null } | null => {
      if (!lead) return null;
      const email = lead.contacts.find((c) => c.channel === "email");
      return email ? { value: email.value, status: email.status } : null;
    };

    // The DB query above is the last point a clean 500 can be sent. Everything below
    // writes to the socket; from here on, failures destroy the stream (headers are sent).
    res.setHeader("Content-Type", "application/json");
    res.write('{"leads":[');
    streamingStarted = true;

    let wroteFirst = false;
    let cursor: LeadCampaignCursor | null = page.cursor;
    // OFFSET positions the FIRST chunk only; the walk continues by keyset from there.
    let offset: number | null = page.offset;
    let remaining: number | null = page.limit;
    let rowCount = 0;
    while (remaining === null || remaining > 0) {
      const take = remaining === null ? LEADS_STREAM_CHUNK_SIZE : Math.min(LEADS_STREAM_CHUNK_SIZE, remaining);
      const chunkRows = await fetchLeadCampaignChunk(scope, cursor, take, offset);
      offset = null;
      if (chunkRows.length === 0) break;
      rowCount += chunkRows.length;
      if (remaining !== null) remaining -= chunkRows.length;
      const chunkLeadIds = Array.from(new Set(chunkRows.map((r) => r.leadId)));
      const fullLeadByLeadId = await buildFullLeadsBatch(chunkLeadIds);

      const audienceMap = await buildAudienceMapForRows(
        chunkRows.map((row) => ({
          leadId: row.leadId,
          email: primaryEmail(fullLeadByLeadId.get(row.leadId))?.value ?? null,
          audienceId: row.audienceId,
          brandIds: row.brandIds,
        })),
        brandIdStr,
        audienceCtx,
      );

      // Delivery-status overlay, scoped to this chunk's served rows only.
      const statusMap = new Map<string, StatusResult>();
      if (hasScopeForStatus) {
        const groups = new Map<string, { brandId: string; items: DeliveryStatusItem[] }>();
        for (const row of chunkRows) {
          if (row.status !== "served") continue;
          const email = primaryEmail(fullLeadByLeadId.get(row.leadId))?.value;
          if (!email) continue;
          const primaryBrandId = row.brandIds[0] ?? "unknown";
          if (!groups.has(primaryBrandId)) {
            groups.set(primaryBrandId, { brandId: primaryBrandId, items: [] });
          }
          groups.get(primaryBrandId)!.items.push({ email });
        }
        await Promise.all(
          Array.from(groups.values()).map(async (group) => {
            const response = await checkDeliveryStatus(group.brandId, statusCampaignIdStr, group.items, context);
            for (const result of response.results) statusMap.set(result.email, result);
          }),
        );
      }

      for (const row of chunkRows) {
        const fullLead = fullLeadByLeadId.get(row.leadId) ?? null;
        const email = primaryEmail(fullLead ?? undefined);
        const emailValue = email?.value ?? "";
        const statusResult = statusMap.get(emailValue);
        const deliveryStatus = hasScopeForStatus && row.status === "served"
          ? (statusResult ? flatten(statusResult) : DEFAULT_STATUS)
          : DEFAULT_STATUS;

        const leadOut = serializeLeadItem(
          row,
          fullLead,
          email,
          audienceMap.get(row.leadId) ?? null,
          deliveryStatus,
        );

        res.write((wroteFirst ? "," : "") + JSON.stringify(leadOut));
        wroteFirst = true;
      }

      const lastRow = chunkRows[chunkRows.length - 1];
      cursor = { createdAt: lastRow.createdAt, id: lastRow.id };
      if (chunkRows.length < take) break;
    }

    res.write(`],"nextCursor":${JSON.stringify(nextCursorFor(page, rowCount, cursor))}}`);
    res.end();

    if (req.runId) {
      traceEvent(req.runId, { service: "lead-service", event: "leads-query-done", detail: `count=${rowCount}`, data: { count: rowCount } }, req.headers).catch(() => {});
    }
  } catch (error) {
    console.error("[lead-service] Leads error:", error);
    if (streamingStarted || res.headersSent) {
      // Stream already open — can't send a 500 body. Destroy the socket so the caller
      // sees a truncated/aborted response and treats it as a failure (fail loud).
      res.destroy(error instanceof Error ? error : new Error(String(error)));
    } else {
      res.status(500).json({ error: "Internal server error" });
    }
  }
});

/** `leads_campaigns.id` is a uuid column, so anything else can only be a caller error, not a miss. */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * GET /orgs/leads/:id — the full record of ONE lead, on its own.
 *
 * A surface that shows a table plus a detail panel for the row somebody clicked has, until now,
 * had to read the FULL projection for the whole brand to make the panel work: ~57k rows and well
 * over 100 MB on one production brand, in a browser tab that polls it. This is the read that lets
 * such a caller take the slim list for the table and then ask for depth one row at a time.
 *
 * `:id` is the `id` of a list row — the `leads_campaigns` membership row — so the caller needs
 * nothing it did not already receive. The response is `{ leadDetail: … }`, one object, byte-equal
 * to the element the full list emits for the same row (both go through serializeLeadItem), so a
 * panel renders from it alone.
 *
 * `brandId` / `campaignId` are the SAME optional scoping the list takes, and they mean the same
 * thing here: which scope the delivery overlay answers for. A caller passes back whatever it
 * listed with and the engagement numbers in the panel match the row in the table. Neither one is
 * an entitlement check — `org_id` is, and it is in the lookup predicate.
 *
 * Deliberately NOT a filter on the list route: a consumer asking for one record should not be
 * constructing a list query, and `{ leadDetail }` says what it is where a one-element `{ leads: [] }`
 * would not.
 */
router.get("/orgs/leads/:id", apiKeyAuth, requireOrgId, async (req: AuthenticatedRequest, res) => {
  try {
    const id = req.params.id;
    if (!UUID_RE.test(id)) {
      return res.status(400).json({ error: "id must be the `id` of a lead row, a uuid" });
    }

    const row = await fetchLeadCampaignRowById(req.orgId!, id);
    if (!row) return res.status(404).json({ error: "Lead not found" });

    const { brandId, campaignId } = req.query;
    const brandIdStr = typeof brandId === "string" ? brandId : undefined;
    const campaignIdStr = typeof campaignId === "string" ? campaignId : undefined;

    // A brand scope the row is not part of names a lead this caller is not reading in that scope;
    // answer exactly as if it did not exist rather than leaking its existence.
    if (brandIdStr && !row.brandIds.includes(brandIdStr)) {
      return res.status(404).json({ error: "Lead not found" });
    }

    // Same campaign-identity resolution the list does, so a campaign-scoped panel reads the whole
    // identity's delivery evidence and not just the live campaign row's (see flattenFamilyStatus).
    const campaignFamily = campaignIdStr
      ? await resolveCampaignFamily(campaignIdStr, {
          orgId: req.orgId!,
          userId: req.userId ?? null,
          runId: req.runId ?? null,
          brandId: brandIdStr ?? null,
        })
      : null;
    const scopeCampaignIds = campaignFamily ?? (campaignIdStr ? [campaignIdStr] : null);
    const familySet =
      scopeCampaignIds && scopeCampaignIds.length > 1 ? new Set(scopeCampaignIds) : null;
    const flatten = familySet
      ? (result: StatusResult) => flattenFamilyStatus(result, familySet)
      : campaignIdStr
        ? flattenCampaignStatus
        : flattenBrandStatus;

    const fullLeadByLeadId = await buildFullLeadsBatch([row.leadId]);
    const fullLead = fullLeadByLeadId.get(row.leadId) ?? null;
    const emailContact = fullLead?.contacts.find((c) => c.channel === "email");
    const email = emailContact ? { value: emailContact.value, status: emailContact.status } : null;

    const audienceMap = await buildAudienceMapForRows(
      [{ leadId: row.leadId, email: email?.value ?? null, audienceId: row.audienceId, brandIds: row.brandIds }],
      brandIdStr,
      { orgId: req.orgId!, userId: req.userId ?? null, runId: req.runId ?? null },
    );

    // Same rule as the list: evidence is only fetched for a served row in a named scope, so an
    // unserved row (or an unscoped read) carries the same all-false overlay it does there.
    let deliveryStatus: FlattenedStatus = DEFAULT_STATUS;
    if ((brandIdStr || campaignIdStr) && row.status === "served" && email?.value) {
      const response = await checkDeliveryStatus(
        row.brandIds[0] ?? "unknown",
        statusCampaignId(scopeCampaignIds),
        [{ email: email.value }],
        getServiceContext(req),
      );
      const result = response.results.find((r) => r.email === email.value);
      deliveryStatus = result ? flatten(result) : DEFAULT_STATUS;
    }

    return res.json({
      leadDetail: serializeLeadItem(row, fullLead, email, audienceMap.get(row.leadId) ?? null, deliveryStatus),
    });
  } catch (error) {
    console.error("[lead-service] Lead detail error:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
