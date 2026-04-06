import { EMAIL_GATEWAY_SERVICE_URL, EMAIL_GATEWAY_SERVICE_API_KEY } from "../config.js";

export interface DeliveryStatusItem {
  leadId: string;
  email: string;
}

export interface LeadDeliveryStatus {
  contacted: boolean;
  delivered: boolean;
  replied: boolean;
  replyClassification: "positive" | "negative" | "neutral" | null;
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

const BATCH_SIZE = 100;

async function checkDeliveryStatusBatch(
  brandId: string,
  campaignId: string | undefined,
  items: DeliveryStatusItem[],
  headers: Record<string, string>,
): Promise<DeliveryStatusResponse | null> {
  headers["x-brand-id"] = brandId;
  const body: Record<string, unknown> = { items };
  if (campaignId) body.campaignId = campaignId;

  const response = await fetch(`${EMAIL_GATEWAY_SERVICE_URL}/orgs/status`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(300_000),
  });

  if (!response.ok) {
    const error = await response.text();
    console.error(
      `[email-gateway-client] Status check failed: ${response.status} - ${error}`
    );
    return null;
  }

  return (await response.json()) as DeliveryStatusResponse;
}

export async function checkDeliveryStatus(
  brandId: string,
  campaignId: string | undefined,
  items: DeliveryStatusItem[],
  context?: { orgId?: string; userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowSlug?: string; featureSlug?: string }
): Promise<DeliveryStatusResponse | null> {
  if (items.length === 0) return { results: [] };

  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": EMAIL_GATEWAY_SERVICE_API_KEY,
    };
    if (context?.orgId) headers["x-org-id"] = context.orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
    if (context?.brandId) headers["x-brand-id"] = context.brandId;
    if (context?.workflowSlug) headers["x-workflow-slug"] = context.workflowSlug;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

    if (items.length <= BATCH_SIZE) {
      return await checkDeliveryStatusBatch(brandId, campaignId, items, headers);
    }

    const batches: DeliveryStatusItem[][] = [];
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
      batches.push(items.slice(i, i + BATCH_SIZE));
    }

    const batchResults = await Promise.all(
      batches.map((batch) => checkDeliveryStatusBatch(brandId, campaignId, batch, headers))
    );

    const allResults: StatusResult[] = [];
    for (const result of batchResults) {
      if (!result) return null;
      allResults.push(...result.results);
    }

    return { results: allResults };
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
