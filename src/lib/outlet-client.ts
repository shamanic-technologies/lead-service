const OUTLETS_SERVICE_URL = process.env.OUTLETS_SERVICE_URL || "http://localhost:3010";
const OUTLETS_SERVICE_API_KEY = process.env.OUTLETS_SERVICE_API_KEY || "";

export interface OutletDetails {
  id: string;
  outletName: string;
  outletUrl: string;
  outletDomain: string;
  relevanceScore: number;
  outletStatus: string;
  campaignId: string;
}

export async function discoverOutlets(params: {
  campaignId: string;
  brandId: string;
  featureInput?: Record<string, unknown>;
  workflowName?: string;
}, context?: {
  orgId?: string | null;
  userId?: string;
  runId?: string;
  featureSlug?: string;
}): Promise<OutletDetails[] | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": OUTLETS_SERVICE_API_KEY,
    };
    if (context?.orgId) headers["x-org-id"] = context.orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (params.campaignId) headers["x-campaign-id"] = params.campaignId;
    if (params.brandId) headers["x-brand-id"] = params.brandId;
    if (params.workflowName) headers["x-workflow-name"] = params.workflowName;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

    const response = await fetch(`${OUTLETS_SERVICE_URL}/outlets/discover`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        campaignId: params.campaignId,
        brandId: params.brandId,
        featureInput: params.featureInput,
        workflowName: params.workflowName,
      }),
    });

    if (response.status === 200) {
      // 200 = no outlets found
      console.log(`[outlet-client] Discover returned 0 outlets for campaign ${params.campaignId}`);
      return [];
    }

    if (!response.ok) {
      console.warn(`[outlet-client] Failed to discover outlets for campaign ${params.campaignId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { discoveredCount: number; outlets: OutletDetails[] };
    console.log(`[outlet-client] Discovered ${data.discoveredCount} outlets for campaign ${params.campaignId}`);
    return data.outlets;
  } catch (error) {
    console.error("[outlet-client] Error discovering outlets:", error);
    return null;
  }
}

export async function fetchOutletsByCampaign(
  campaignId: string,
  orgId?: string | null,
  context?: { userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowName?: string; featureSlug?: string }
): Promise<OutletDetails[] | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": OUTLETS_SERVICE_API_KEY,
    };
    if (orgId) headers["x-org-id"] = orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
    if (context?.brandId) headers["x-brand-id"] = context.brandId;
    if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

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
