const EMAIL_GATEWAY_SERVICE_URL =
  process.env.EMAIL_GATEWAY_SERVICE_URL || "http://localhost:3009";
const EMAIL_GATEWAY_SERVICE_API_KEY =
  process.env.EMAIL_GATEWAY_SERVICE_API_KEY || "";

export interface DeliveryStatusItem {
  leadId?: string;
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

export interface StatusScope {
  lead: LeadDeliveryStatus;
  email: EmailDeliveryStatus;
}

export interface ProviderStatus {
  campaign: StatusScope;
  global: StatusScope;
}

export interface StatusResult {
  leadId?: string;
  email: string;
  broadcast?: ProviderStatus;
  transactional?: ProviderStatus;
}

export interface DeliveryStatusResponse {
  results: StatusResult[];
}

export async function checkDeliveryStatus(
  campaignId: string,
  items: DeliveryStatusItem[]
): Promise<DeliveryStatusResponse | null> {
  try {
    const response = await fetch(`${EMAIL_GATEWAY_SERVICE_URL}/status`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": EMAIL_GATEWAY_SERVICE_API_KEY,
      },
      body: JSON.stringify({ campaignId, items }),
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
    console.error("[email-gateway-client] Status check error:", error);
    return null;
  }
}

/**
 * Check if a status result indicates the lead/email has been delivered
 * via any provider (broadcast or transactional) at any scope (campaign or global).
 */
export function isDelivered(result: StatusResult): boolean {
  const bc = result.broadcast;
  const tx = result.transactional;
  return !!(
    bc?.campaign.lead.contacted ||
    bc?.campaign.email.contacted ||
    bc?.global.lead.contacted ||
    bc?.global.email.contacted ||
    tx?.campaign.lead.contacted ||
    tx?.campaign.email.contacted ||
    tx?.global.lead.contacted ||
    tx?.global.email.contacted
  );
}
