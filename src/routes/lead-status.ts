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

/**
 * Flatten status using campaign scope (when campaignId is provided).
 */
function flattenCampaignStatus(result: StatusResult) {
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
function flattenBrandStatus(result: StatusResult) {
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

const DEFAULT_FLAT = { contacted: false, delivered: false, bounced: false, replied: false, replyClassification: null, lastDeliveredAt: null };

router.get("/orgs/leads/status", apiKeyAuth, requireOrgId, async (req: AuthenticatedRequest, res) => {
  try {
    const campaignId = typeof req.query.campaignId === "string" ? req.query.campaignId : undefined;
    const brandId = typeof req.query.brandId === "string" ? req.query.brandId : undefined;

    // brandId is required when no campaignId (cross-campaign needs a brand scope)
    if (!campaignId && !brandId) {
      res.status(400).json({ error: "Either campaignId or brandId query parameter is required" });
      return;
    }

    const orgId = req.orgId!;
    const conditions: SQL[] = [eq(servedLeads.orgId, orgId)];

    if (campaignId) {
      conditions.push(eq(servedLeads.campaignId, campaignId));
    }
    if (brandId) {
      conditions.push(sql`${brandId} = ANY(${servedLeads.brandIds})`);
    }
    const rows = await db
      .select({
        leadId: servedLeads.leadId,
        email: servedLeads.email,
        brandIds: servedLeads.brandIds,
        metadata: servedLeads.metadata,
      })
      .from(servedLeads)
      .where(and(...conditions));

    if (rows.length === 0) {
      res.json({ statuses: [] });
      return;
    }

    const context = getServiceContext(req);
    const flatten = campaignId ? flattenCampaignStatus : flattenBrandStatus;

    // Group by first brandId since email-gateway scopes status per brand
    const groups = new Map<string, { brandId: string; items: DeliveryStatusItem[] }>();
    for (const row of rows) {
      if (!row.leadId) continue;
      const primaryBrandId = row.brandIds[0] ?? "unknown";
      if (!groups.has(primaryBrandId)) {
        groups.set(primaryBrandId, { brandId: primaryBrandId, items: [] });
      }
      groups.get(primaryBrandId)!.items.push({ leadId: row.leadId, email: row.email });
    }

    const statusMap = new Map<string, StatusResult>();

    await Promise.all(
      Array.from(groups.values()).map(async (group) => {
        const response = await checkDeliveryStatus(
          group.brandId,
          campaignId,
          group.items,
          context,
        );
        if (!response) return;
        for (const result of response.results) {
          statusMap.set(result.email, result);
        }
      }),
    );

    // Deduplicate by email (cross-campaign can have same lead in multiple campaigns)
    const seen = new Set<string>();
    const statuses = [];

    for (const row of rows) {
      if (!row.leadId) continue;
      if (seen.has(row.email)) continue;
      seen.add(row.email);

      const result = statusMap.get(row.email);
      const flat = result ? flatten(result) : DEFAULT_FLAT;

      statuses.push({
        leadId: row.leadId,
        email: row.email,
        ...flat,
      });
    }

    res.json({ statuses });
  } catch (error) {
    console.error("[lead-service] Lead status error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export { flattenCampaignStatus, flattenBrandStatus };
export default router;
