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
  clerkOrgId?: string | null
): Promise<CampaignDetails | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": CAMPAIGN_SERVICE_API_KEY,
    };
    if (clerkOrgId) headers["x-clerk-org-id"] = clerkOrgId;

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
