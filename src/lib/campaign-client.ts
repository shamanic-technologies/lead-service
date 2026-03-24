const CAMPAIGN_SERVICE_URL = process.env.CAMPAIGN_SERVICE_URL || "http://localhost:3003";
const CAMPAIGN_SERVICE_API_KEY = process.env.CAMPAIGN_SERVICE_API_KEY || "";

export interface CampaignDetails {
  id: string;
  name: string;
  targetAudience: string | null;
  targetOutcome: string | null;
  valueForTarget: string | null;
}

export async function fetchCampaign(
  campaignId: string,
  orgId?: string | null,
  context?: { userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowName?: string; featureSlug?: string }
): Promise<CampaignDetails | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": CAMPAIGN_SERVICE_API_KEY,
    };
    if (orgId) headers["x-org-id"] = orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
    if (context?.brandId) headers["x-brand-id"] = context.brandId;
    if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

    const response = await fetch(`${CAMPAIGN_SERVICE_URL}/campaigns/${campaignId}`, {
      headers,
    });

    if (!response.ok) {
      console.warn(`[campaign-client] Failed to fetch campaign ${campaignId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { campaign: CampaignDetails };
    return data.campaign;
  } catch (error) {
    console.error("[campaign-client] Error fetching campaign:", error);
    return null;
  }
}
