/**
 * WHICH funnel a lead is on: the one its CAMPAIGN states.
 *
 * campaign-service owns `funnelKey` on the campaign row, so it is read from there and never
 * inferred — not from the brand's declared funnels, not from a goal (two funnels answer to the
 * same goal, so a goal->funnel inference prints an order the campaign never stated), and not from
 * a sibling campaign.
 *
 * NO SILENT FALLBACK. A funnel that cannot be resolved is an error, and the three ways it fails
 * are kept apart because a caller acts on them differently:
 *
 *   unavailable — campaign-service could not answer. Transient; retrying later is the fix.
 *   unknown     — campaign-service does not know this campaign id at all.
 *   unstated    — the campaign exists and states no funnel, or states one this service has no
 *                 steps for. Nothing is broken and nothing is missing; there is simply no order to
 *                 read the steps in, and inventing one would print an order nobody stated.
 */
import { CAMPAIGN_SERVICE_URL, CAMPAIGN_SERVICE_API_KEY } from "../config.js";
import { fetchWithRetry } from "./fetch-retry.js";
import { stepsForFunnelKey, canonicalizeFunnelKey, type FunnelKey } from "./funnel-steps.js";
import type { LeadStepOutcomeName } from "./step-statements.js";

export type FunnelResolutionFailure = "unavailable" | "unknown" | "unstated";

export class FunnelResolutionError extends Error {
  constructor(
    readonly reason: FunnelResolutionFailure,
    message: string,
  ) {
    super(message);
    this.name = "FunnelResolutionError";
  }
}

export interface ResolvedFunnel {
  funnelKey: FunnelKey;
  funnelSteps: readonly LeadStepOutcomeName[];
}

export interface CampaignFunnelContext {
  orgId: string;
  userId?: string | null;
  runId?: string | null;
  brandId?: string | null;
}

function headersFor(ctx: CampaignFunnelContext): Record<string, string> {
  const headers: Record<string, string> = {
    "X-API-Key": CAMPAIGN_SERVICE_API_KEY,
    "x-org-id": ctx.orgId,
  };
  if (ctx.userId) headers["x-user-id"] = ctx.userId;
  if (ctx.runId) headers["x-run-id"] = ctx.runId;
  if (ctx.brandId) headers["x-brand-id"] = ctx.brandId;
  return headers;
}

/** The funnel a campaign states, or a typed failure. Never null-on-error, never a guess. */
export async function resolveCampaignFunnel(
  campaignId: string,
  ctx: CampaignFunnelContext,
): Promise<ResolvedFunnel> {
  let response: Response;
  try {
    response = await fetchWithRetry(`${CAMPAIGN_SERVICE_URL}/campaigns/${campaignId}`, {
      method: "GET",
      headers: headersFor(ctx),
      signal: AbortSignal.timeout(30_000),
    });
  } catch (error) {
    throw new FunnelResolutionError(
      "unavailable",
      `[campaign-funnel-client] campaign-service unreachable for campaign ${campaignId}: ${(error as Error).message}`,
    );
  }

  if (response.status === 404) {
    throw new FunnelResolutionError(
      "unknown",
      `[campaign-funnel-client] campaign-service does not know campaign ${campaignId}`,
    );
  }
  if (!response.ok) {
    const body = await response.text();
    throw new FunnelResolutionError(
      "unavailable",
      `[campaign-funnel-client] campaign-service /campaigns/${campaignId} failed (${response.status}): ${body}`,
    );
  }

  const data = (await response.json()) as { campaign?: { funnelKey?: string | null } };
  const funnelKey = canonicalizeFunnelKey(data.campaign?.funnelKey);
  const funnelSteps = stepsForFunnelKey(data.campaign?.funnelKey);
  if (!funnelKey || !funnelSteps) {
    throw new FunnelResolutionError(
      "unstated",
      `[campaign-funnel-client] campaign ${campaignId} states no sales funnel this service has ` +
        `steps for (funnelKey=${JSON.stringify(data.campaign?.funnelKey ?? null)})`,
    );
  }
  return { funnelKey, funnelSteps };
}

/**
 * Every campaign of one org and the funnel it states — one read, for the brand-level view that
 * must resolve many campaigns at once. A campaign stating no known funnel maps to null: the caller
 * decides what that means for its own answer, and there is still nothing to guess.
 */
export async function fetchOrgCampaignFunnelKeys(
  ctx: CampaignFunnelContext,
): Promise<Map<string, FunnelKey | null>> {
  let response: Response;
  try {
    response = await fetchWithRetry(`${CAMPAIGN_SERVICE_URL}/campaigns`, {
      method: "GET",
      headers: headersFor(ctx),
      signal: AbortSignal.timeout(30_000),
    });
  } catch (error) {
    throw new FunnelResolutionError(
      "unavailable",
      `[campaign-funnel-client] campaign-service unreachable for org ${ctx.orgId}: ${(error as Error).message}`,
    );
  }
  if (!response.ok) {
    const body = await response.text();
    throw new FunnelResolutionError(
      "unavailable",
      `[campaign-funnel-client] campaign-service /campaigns failed (${response.status}): ${body}`,
    );
  }
  const data = (await response.json()) as {
    campaigns?: Array<{ id?: string; funnelKey?: string | null }>;
  };
  if (!Array.isArray(data.campaigns)) {
    throw new FunnelResolutionError(
      "unavailable",
      "[campaign-funnel-client] campaign-service /campaigns returned no campaigns array",
    );
  }
  const byId = new Map<string, FunnelKey | null>();
  for (const row of data.campaigns) {
    if (!row?.id) continue;
    byId.set(row.id, canonicalizeFunnelKey(row.funnelKey));
  }
  return byId;
}
