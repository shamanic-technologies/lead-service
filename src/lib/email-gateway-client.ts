const EMAIL_GATEWAY_SERVICE_URL =
  process.env.EMAIL_GATEWAY_SERVICE_URL || "http://localhost:3009";
const EMAIL_GATEWAY_SERVICE_API_KEY =
  process.env.EMAIL_GATEWAY_SERVICE_API_KEY || "";

export interface DeliveryStatusItem {
  leadId: string;
  email: string;
}

export interface LeadDeliveryStatus {
  contacted: boolean;
  delivered: boolean;
  replied: boolean;
  lastDeliveredAt: string | null;
}

export interface EmailDeliveryStatus {
  contacted: boolean;
  delivered: boolean;
  bounced: boolean;
  unsubscribed: boolean;
  lastDeliveredAt: string | null;
}

export interface ScopedStatus {
  lead: LeadDeliveryStatus;
  email: EmailDeliveryStatus;
}

export interface GlobalStatus {
  email: EmailDeliveryStatus;
}

export interface ProviderStatus {
  campaign: ScopedStatus;
  brand: ScopedStatus;
  global: GlobalStatus;
}

export interface StatusResult {
  leadId: string;
  email: string;
  broadcast?: ProviderStatus;
  transactional?: ProviderStatus;
}

export interface DeliveryStatusResponse {
  results: StatusResult[];
}

export async function checkDeliveryStatus(
  brandId: string,
  campaignId: string | undefined,
  items: DeliveryStatusItem[],
  context?: { orgId?: string; userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowName?: string; featureSlug?: string }
): Promise<DeliveryStatusResponse | null> {
  try {
    const body: Record<string, unknown> = { brandId, items };
    if (campaignId) body.campaignId = campaignId;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": EMAIL_GATEWAY_SERVICE_API_KEY,
    };
    if (context?.orgId) headers["x-org-id"] = context.orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
    if (context?.brandId) headers["x-brand-id"] = context.brandId;
    if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

    const response = await fetch(`${EMAIL_GATEWAY_SERVICE_URL}/status`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const error = await response.text();
      console.error(
        `[email-gateway-client] Status check failed: ${response.status} - ${error}`
      );
      return null;
    }

    return (await response.json()) as DeliveryStatusResponse;
  } catch (error) {
    const isConnectionError =
      error instanceof TypeError && error.message === "fetch failed";
    if (isConnectionError) {
      console.warn(
        "[email-gateway-client] email-gateway unreachable, skipping delivery check"
      );
    } else {
      console.error("[email-gateway-client] Status check error:", error);
    }
    return null;
  }
}

/**
 * Check if a status result indicates the lead/email has already been contacted
 * via any provider (broadcast or transactional) at any scope (campaign, brand, or global).
 */
export function isContacted(result: StatusResult): boolean {
  const bc = result.broadcast;
  const tx = result.transactional;
  return !!(
    bc?.campaign.lead.contacted ||
    bc?.campaign.email.contacted ||
    bc?.brand.lead.contacted ||
    bc?.brand.email.contacted ||
    bc?.global.email.contacted ||
    tx?.campaign.lead.contacted ||
    tx?.campaign.email.contacted ||
    tx?.brand.lead.contacted ||
    tx?.brand.email.contacted ||
    tx?.global.email.contacted
  );
}
