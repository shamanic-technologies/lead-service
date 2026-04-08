import { Router } from "express";
import { eq, and, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, apiKeyAuth, requireOrgId, getServiceContext } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";
import {
  checkDeliveryStatus,
  type StatusResult,
  type DeliveryStatusItem,
} from "../lib/email-gateway-client.js";

const router = Router();

export function extractEnrichment(metadata: unknown): Record<string, unknown> | null {
  if (!metadata || typeof metadata !== "object") return null;
  const m = metadata as Record<string, unknown>;
  // Only return enrichment if at least one person field exists
  if (!m.firstName && !m.lastName && !m.email) return null;
  // Pass through all fields from Apollo — no filtering
  return { ...m };
}

/**
 * Flatten status using campaign scope (when campaignId is provided).
 */
export function flattenCampaignStatus(result: StatusResult) {
  const bc = result.broadcast;
  const tx = result.transactional;

  const contacted = !!(
    bc?.campaign?.contacted ||
    bc?.brand?.contacted ||
    bc?.global?.email?.contacted ||
    tx?.campaign?.contacted ||
    tx?.brand?.contacted ||
    tx?.global?.email?.contacted
  );

  const delivered = !!(
    bc?.campaign?.delivered ||
    tx?.campaign?.delivered
  );

  const bounced = !!(
    bc?.campaign?.bounced ||
    tx?.campaign?.bounced
  );

  const replied = !!(
    bc?.campaign?.replied ||
    tx?.campaign?.replied
  );

  const replyClassification =
    bc?.campaign?.replyClassification ??
    tx?.campaign?.replyClassification ??
    null;

  const lastDeliveredAt =
    bc?.campaign?.lastDeliveredAt ??
    tx?.campaign?.lastDeliveredAt ??
    null;

  return { contacted, delivered, bounced, replied, replyClassification, lastDeliveredAt };
}

/**
 * Flatten status using brand scope (when no campaignId — cross-campaign view).
 */
export function flattenBrandStatus(result: StatusResult) {
  const bc = result.broadcast;
  const tx = result.transactional;

  const contacted = !!(
    bc?.brand?.contacted ||
    bc?.global?.email?.contacted ||
    tx?.brand?.contacted ||
    tx?.global?.email?.contacted
  );

  const delivered = !!(
    bc?.brand?.delivered ||
    tx?.brand?.delivered
  );

  const bounced = !!(
    bc?.brand?.bounced ||
    tx?.brand?.bounced
  );

  const replied = !!(
    bc?.brand?.replied ||
    tx?.brand?.replied
  );

  const replyClassification =
    bc?.brand?.replyClassification ??
    tx?.brand?.replyClassification ??
    null;

  const lastDeliveredAt =
    bc?.brand?.lastDeliveredAt ??
    tx?.brand?.lastDeliveredAt ??
    null;

  return { contacted, delivered, bounced, replied, replyClassification, lastDeliveredAt };
}

const DEFAULT_STATUS = { contacted: false, delivered: false, bounced: false, replied: false, replyClassification: null, lastDeliveredAt: null };

router.get("/orgs/leads", apiKeyAuth, requireOrgId, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId, orgId, userId } = req.query;

    // Build filter conditions
    const conditions: SQL[] = [eq(servedLeads.orgId, req.orgId!)];

    if (brandId && typeof brandId === "string") {
      conditions.push(sql`${brandId} = ANY(${servedLeads.brandIds})`);
    }
    if (campaignId && typeof campaignId === "string") {
      conditions.push(eq(servedLeads.campaignId, campaignId));
    }
    if (orgId && typeof orgId === "string") {
      conditions.push(eq(servedLeads.orgId, orgId));
    }
    if (userId && typeof userId === "string") {
      conditions.push(eq(servedLeads.userId, userId));
    }

    // Get served leads
    const rows = await db.query.servedLeads.findMany({
      where: and(...conditions),
    });

    // Fetch delivery status from email-gateway
    const campaignIdStr = typeof campaignId === "string" ? campaignId : undefined;
    const brandIdStr = typeof brandId === "string" ? brandId : undefined;
    const hasScopeForStatus = !!(campaignIdStr || brandIdStr);
    const flatten = campaignIdStr ? flattenCampaignStatus : flattenBrandStatus;
    const context = getServiceContext(req);

    let statusMap = new Map<string, StatusResult>();

    if (hasScopeForStatus) {
      // Group by first brandId since email-gateway scopes status per brand
      const groups = new Map<string, { brandId: string; items: DeliveryStatusItem[] }>();
      for (const row of rows) {
        if (!row.leadId) continue;
        const primaryBrandId = row.brandIds[0] ?? "unknown";
        if (!groups.has(primaryBrandId)) {
          groups.set(primaryBrandId, { brandId: primaryBrandId, items: [] });
        }
        groups.get(primaryBrandId)!.items.push({ email: row.email });
      }

      await Promise.all(
        Array.from(groups.values()).map(async (group) => {
          const response = await checkDeliveryStatus(
            group.brandId,
            campaignIdStr,
            group.items,
            context,
          );
          if (!response) return;
          for (const result of response.results) {
            statusMap.set(result.email, result);
          }
        }),
      );
    }

    const enrichedLeads = rows.map((lead) => {
      const statusResult = statusMap.get(lead.email);
      const status = hasScopeForStatus
        ? (statusResult ? flatten(statusResult) : DEFAULT_STATUS)
        : DEFAULT_STATUS;

      return {
        ...lead,
        leadId: lead.leadId ?? null,
        enrichment: extractEnrichment(lead.metadata),
        ...status,
      };
    });

    res.json({ leads: enrichedLeads });
  } catch (error) {
    console.error("[lead-service] Leads error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
