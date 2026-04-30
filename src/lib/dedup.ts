import { db, sql as pgSql } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import {
  checkDeliveryStatus,
  checkEmailStatus,
  type DeliveryStatusItem,
  type EmailCheckResult,
} from "./email-gateway-client.js";

const RACE_WINDOW_MINUTES = 60;

/**
 * Check if a lead has already been served for any overlapping brand,
 * cross-campaign, using 3 axes: leadId, email, externalId.
 * Uses the && (array overlap) operator on brand_ids.
 */
export async function isAlreadyServedForBrand(params: {
  orgId: string;
  brandIds: string[];
  leadId?: string | null;
  email?: string | null;
  externalId?: string | null;
}): Promise<{ blocked: boolean; reason?: string }> {
  if (params.brandIds.length === 0) return { blocked: false };

  const brandIdsArray = `{${params.brandIds.join(",")}}`;

  // Build OR conditions for each available axis
  const conditions: string[] = [];
  const values: unknown[] = [params.orgId, brandIdsArray];
  let paramIdx = 3;

  if (params.leadId) {
    conditions.push(`lead_id = $${paramIdx}`);
    values.push(params.leadId);
    paramIdx++;
  }
  if (params.email) {
    conditions.push(`email = $${paramIdx}`);
    values.push(params.email);
    paramIdx++;
  }
  if (params.externalId) {
    conditions.push(`external_id = $${paramIdx}`);
    values.push(params.externalId);
    paramIdx++;
  }

  if (conditions.length === 0) return { blocked: false };

  const rows = await pgSql.unsafe(
    `SELECT lead_id, email, external_id FROM served_leads
     WHERE org_id = $1
       AND brand_ids && $2::text[]
       AND (${conditions.join(" OR ")})
     LIMIT 1`,
    values as string[],
  );

  if (rows.length > 0) {
    const match = rows[0];
    const axes: string[] = [];
    if (params.leadId && match.lead_id === params.leadId) axes.push("lead_id");
    if (params.email && match.email === params.email) axes.push("email");
    if (params.externalId && match.external_id === params.externalId) axes.push("external_id");
    return {
      blocked: true,
      reason: `already served for overlapping brand (matched on ${axes.join(", ") || "unknown axis"})`,
    };
  }

  return { blocked: false };
}

/**
 * Check if a lead is in the race window: claimed or served within the last hour
 * for any overlapping brand. Prevents concurrent pullNext calls across campaigns
 * from serving the same lead.
 */
export async function checkRaceWindow(params: {
  orgId: string;
  brandIds: string[];
  email: string;
  excludeBufferId: string;
}): Promise<boolean> {
  if (params.brandIds.length === 0) return false;

  const brandIdsArray = `{${params.brandIds.join(",")}}`;

  const rows = await pgSql.unsafe(
    `SELECT 1 FROM lead_buffer
     WHERE org_id = $1
       AND brand_ids && $2::text[]
       AND email = $3
       AND status IN ('claimed', 'served')
       AND created_at >= now() - interval '${RACE_WINDOW_MINUTES} minutes'
       AND id != $4
     LIMIT 1`,
    [params.orgId, brandIdsArray, params.email, params.excludeBufferId],
  );

  return rows.length > 0;
}

/**
 * Check if items have already been contacted via email-gateway.
 * Returns a Map of email -> EmailCheckResult.
 * Throws if email-gateway is unreachable — fail loud, no silent fallback.
 */
export async function checkContacted(
  brandIds: string[],
  campaignId: string,
  items: DeliveryStatusItem[],
  context?: { orgId?: string; userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowSlug?: string; featureSlug?: string }
): Promise<Map<string, EmailCheckResult>> {
  const result = new Map<string, EmailCheckResult>();

  const primaryBrandId = brandIds[0];
  if (!primaryBrandId) {
    throw new Error("[dedup] No brand IDs provided — cannot check delivery status");
  }

  const statusResponse = await checkDeliveryStatus(primaryBrandId, campaignId, items, context);

  if (!statusResponse) {
    throw new Error("[dedup] email-gateway unreachable — refusing to serve without delivery check");
  }

  for (const sr of statusResponse.results) {
    result.set(sr.email, checkEmailStatus(sr));
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
  parentRunId?: string | null;
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
      parentRunId: params.parentRunId ?? null,
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
