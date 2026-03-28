const OUTLETS_SERVICE_URL = process.env.OUTLETS_SERVICE_URL || "http://localhost:3010";
const OUTLETS_SERVICE_API_KEY = process.env.OUTLETS_SERVICE_API_KEY || "";

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
  brandId: string;
  relevanceScore: number;
  whyRelevant: string;
  whyNotRelevant: string;
  overallRelevance: string | null;
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
  try {
    const headers = buildHeaders(context);

    const body: Record<string, unknown> = {};
    if (context.idempotencyKey) body.idempotencyKey = context.idempotencyKey;

    const response = await fetch(`${OUTLETS_SERVICE_URL}/buffer/next`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      console.warn(`[outlet-client] buffer/next failed for campaign ${context.campaignId}: ${response.status}`);
      return { found: false };
    }

    const data = (await response.json()) as { found: boolean; outlet?: BufferNextOutlet };
    return data;
  } catch (error) {
    console.error("[outlet-client] Error fetching next outlet:", error);
    return { found: false };
  }
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
      { headers }
    );

    if (!response.ok) {
      console.warn(`[outlet-client] Failed to fetch outlets for campaign ${campaignId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { outlets: OutletDetails[] };
    return data.outlets;
  } catch (error) {
    console.error("[outlet-client] Error fetching outlets:", error);
    return null;
  }
}
