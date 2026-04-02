import { OUTLETS_SERVICE_URL, OUTLETS_SERVICE_API_KEY } from "../config.js";

export interface OutletDetails {
  id: string;
  outletName: string;
  outletUrl: string;
  outletDomain: string;
  relevanceScore: number;
  whyRelevant: string;
  whyNotRelevant: string;
  overallRelevance: string;
  outletStatus: string;
  campaignId: string;
}

export interface BufferNextOutlet {
  outletId: string;
  outletName: string;
  outletUrl: string;
  outletDomain: string;
  campaignId: string;
  brandIds: string[];
  relevanceScore: number;
  whyRelevant: string;
  whyNotRelevant: string;
  overallRelevance: string | null;
  runId: string | null;
}

function buildHeaders(context?: {
  orgId?: string | null;
  userId?: string;
  runId?: string;
  campaignId?: string;
  brandId?: string;
  workflowSlug?: string;
  featureSlug?: string;
}): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": OUTLETS_SERVICE_API_KEY,
  };
  if (context?.orgId) headers["x-org-id"] = context.orgId;
  if (context?.userId) headers["x-user-id"] = context.userId;
  if (context?.runId) headers["x-run-id"] = context.runId;
  if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
  if (context?.brandId) headers["x-brand-id"] = context.brandId;
  if (context?.workflowSlug) headers["x-workflow-slug"] = context.workflowSlug;
  if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;
  return headers;
}

const RETRY_DELAYS_MS = [5_000, 15_000];

export async function fetchNextOutlet(context: {
  orgId: string;
  userId?: string;
  runId?: string;
  campaignId: string;
  brandId: string;
  workflowSlug?: string;
  featureSlug?: string;
  idempotencyKey?: string;
}): Promise<{ found: boolean; outlet?: BufferNextOutlet }> {
  const headers = buildHeaders(context);
  const body: Record<string, unknown> = {};
  if (context.idempotencyKey) body.idempotencyKey = context.idempotencyKey;

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    if (attempt > 0) {
      const delay = RETRY_DELAYS_MS[attempt - 1];
      console.log(`[outlet-client] Retrying buffer/next for campaign ${context.campaignId} in ${delay}ms (attempt ${attempt + 1}/${RETRY_DELAYS_MS.length + 1})`);
      await new Promise((r) => setTimeout(r, delay));
    }

    try {
      const response = await fetch(`${OUTLETS_SERVICE_URL}/buffer/next`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(300_000),
      });

      if (!response.ok) {
        const msg = `[outlet-client] buffer/next failed for campaign ${context.campaignId}: ${response.status}`;
        if (response.status >= 500) {
          lastError = new Error(msg);
          continue;
        }
        console.warn(msg);
        return { found: false };
      }

      const data = (await response.json()) as { outlets: BufferNextOutlet[] };
      const outlet = data.outlets?.[0];
      return { found: !!outlet, outlet };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      console.warn(`[outlet-client] buffer/next attempt ${attempt + 1} failed for campaign ${context.campaignId}:`, lastError.message);
      continue;
    }
  }

  console.error(`[outlet-client] buffer/next exhausted all ${RETRY_DELAYS_MS.length + 1} attempts for campaign ${context.campaignId}`);
  throw lastError!;
}

export async function fetchOutletsByCampaign(
  campaignId: string,
  orgId?: string | null,
  context?: { userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowSlug?: string; featureSlug?: string }
): Promise<OutletDetails[] | null> {
  try {
    const headers = buildHeaders({
      orgId,
      userId: context?.userId,
      runId: context?.runId,
      campaignId: context?.campaignId,
      brandId: context?.brandId,
      workflowSlug: context?.workflowSlug,
      featureSlug: context?.featureSlug,
    });

    const response = await fetch(
      `${OUTLETS_SERVICE_URL}/internal/outlets/by-campaign/${campaignId}`,
      { headers, signal: AbortSignal.timeout(300_000) }
    );

    if (!response.ok) {
      const msg = `[outlet-client] Failed to fetch outlets for campaign ${campaignId}: ${response.status}`;
      if (response.status >= 500) {
        throw new Error(msg);
      }
      console.warn(msg);
      return null;
    }

    const data = (await response.json()) as { outlets: OutletDetails[] };
    return data.outlets;
  } catch (error) {
    console.error("[outlet-client] Error fetching outlets:", error);
    throw error;
  }
}
