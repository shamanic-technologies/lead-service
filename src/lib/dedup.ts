import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";
import {
  checkDeliveryStatus,
  isDelivered,
  type DeliveryStatusItem,
} from "./email-gateway-client.js";

/**
 * Check if items have been delivered via email-gateway.
 * Returns a Map of email -> boolean (delivered or not).
 * Falls back to all-false if email-gateway is unreachable —
 * the downstream /send endpoint has its own idempotency.
 */
export async function checkDelivered(
  brandId: string,
  campaignId: string,
  items: DeliveryStatusItem[]
): Promise<Map<string, boolean>> {
  const result = new Map<string, boolean>();

  const statusResponse = await checkDeliveryStatus(brandId, campaignId, items);

  if (statusResponse) {
    for (const sr of statusResponse.results) {
      result.set(sr.email, isDelivered(sr));
    }
  } else {
    console.warn(
      "[dedup] email-gateway unreachable, proceeding without delivery check"
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
  organizationId: string;
  namespace: string;
  brandId: string;
  campaignId: string;
  email: string;
  leadId?: string | null;
  externalId?: string | null;
  metadata?: unknown;
  parentRunId?: string | null;
  runId?: string | null;
  orgId?: string | null;
  userId?: string | null;
}): Promise<{ inserted: boolean }> {
  const result = await db
    .insert(servedLeads)
    .values({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: params.email,
      leadId: params.leadId ?? null,
      externalId: params.externalId ?? null,
      metadata: params.metadata ?? null,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      brandId: params.brandId,
      campaignId: params.campaignId,
      orgId: params.orgId ?? null,
      userId: params.userId ?? null,
    })
    .onConflictDoNothing()
    .returning();

  return { inserted: result.length > 0 };
}
