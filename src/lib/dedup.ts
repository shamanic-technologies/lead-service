import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";
import {
  checkDeliveryStatus,
  isContacted,
  type DeliveryStatusItem,
} from "./email-gateway-client.js";

/**
 * Check if items have already been contacted via email-gateway.
 * Returns a Map of email -> boolean (contacted or not).
 * Falls back to all-false if email-gateway is unreachable —
 * the downstream /send endpoint has its own idempotency.
 *
 * With multi-brand, checks against the first brand ID (email-gateway
 * body param expects a single brandId; the full CSV is forwarded via header).
 */
export async function checkContacted(
  brandIds: string[],
  campaignId: string,
  items: DeliveryStatusItem[],
  context?: { orgId?: string; userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowSlug?: string; featureSlug?: string }
): Promise<Map<string, boolean>> {
  const result = new Map<string, boolean>();

  const primaryBrandId = brandIds[0];
  if (!primaryBrandId) {
    // No brand IDs — can't check, assume not contacted
    for (const item of items) {
      result.set(item.email, false);
    }
    return result;
  }

  const statusResponse = await checkDeliveryStatus(primaryBrandId, campaignId, items, context);

  if (statusResponse) {
    for (const sr of statusResponse.results) {
      result.set(sr.email, isContacted(sr));
    }
  } else {
    console.warn(
      "[dedup] email-gateway unreachable, proceeding without contacted check"
    );
    for (const item of items) {
      result.set(item.email, false);
    }
  }

  return result;
}

/**
 * Record a served lead in the audit log (servedLeads table).
 * Now includes leadId for the global identity link.
 */
export async function markServed(params: {
  orgId: string;
  namespace: string;
  brandIds: string[];
  campaignId: string;
  email: string;
  leadId?: string | null;
  externalId?: string | null;
  metadata?: unknown;
  runId?: string | null;
  userId?: string | null;
  workflowSlug?: string | null;
  featureSlug?: string | null;
}): Promise<{ inserted: boolean }> {
  const result = await db
    .insert(servedLeads)
    .values({
      orgId: params.orgId,
      namespace: params.namespace,
      email: params.email,
      leadId: params.leadId ?? null,
      externalId: params.externalId ?? null,
      metadata: params.metadata ?? null,
      runId: params.runId ?? null,
      brandIds: params.brandIds,
      campaignId: params.campaignId,
      userId: params.userId ?? null,
      workflowSlug: params.workflowSlug ?? null,
      featureSlug: params.featureSlug ?? null,
    })
    .onConflictDoNothing()
    .returning();

  return { inserted: result.length > 0 };
}
