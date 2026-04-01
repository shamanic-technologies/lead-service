import { Router } from "express";
import { eq, and, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate, getServiceContext } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";
import {
  checkDeliveryStatus,
  type StatusResult,
  type DeliveryStatusItem,
} from "../lib/email-gateway-client.js";
import {
  fetchQualificationsByOrg,
  classifyReply,
} from "../lib/reply-qualification-client.js";

const router = Router();

function flattenBrandStatus(result: StatusResult) {
  const bc = result.broadcast;
  const tx = result.transactional;

  // Use brand-scoped status for cross-campaign view
  const contacted = !!(
    bc?.brand.lead.contacted ||
    bc?.brand.email.contacted ||
    bc?.global.email.contacted ||
    tx?.brand.lead.contacted ||
    tx?.brand.email.contacted ||
    tx?.global.email.contacted
  );

  const delivered = !!(
    bc?.brand.email.delivered ||
    tx?.brand.email.delivered
  );

  const bounced = !!(
    bc?.brand.email.bounced ||
    tx?.brand.email.bounced
  );

  const replied = !!(
    bc?.brand.lead.replied ||
    tx?.brand.lead.replied
  );

  const lastDeliveredAt =
    bc?.brand.email.lastDeliveredAt ??
    tx?.brand.email.lastDeliveredAt ??
    null;

  return { contacted, delivered, bounced, replied, lastDeliveredAt };
}

router.get("/leads/outreach-status", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const brandId = typeof req.query.brandId === "string" ? req.query.brandId : undefined;
    if (!brandId) {
      res.status(400).json({ error: "brandId query parameter is required" });
      return;
    }

    const orgId = req.orgId!;

    // Query all served leads for this org+brand, cross-campaign
    const conditions: SQL[] = [
      eq(servedLeads.orgId, orgId),
      sql`${brandId} = ANY(${servedLeads.brandIds})`,
    ];

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

    // Fetch delivery status from email-gateway (brand-scoped, no campaignId)
    const items: DeliveryStatusItem[] = rows
      .filter((row) => row.leadId)
      .map((row) => ({ leadId: row.leadId!, email: row.email }));

    const [deliveryResponse, qualificationsMap] = await Promise.all([
      checkDeliveryStatus(brandId, undefined, items, context),
      fetchQualificationsByOrg(orgId, {
        runId: context.runId,
        brandId: context.brandId,
        campaignId: context.campaignId,
        workflowSlug: context.workflowSlug,
        featureSlug: context.featureSlug,
      }),
    ]);

    const statusMap = new Map<string, StatusResult>();
    if (deliveryResponse) {
      for (const result of deliveryResponse.results) {
        statusMap.set(result.email, result);
      }
    }

    // Deduplicate by email (a lead can appear in multiple campaigns)
    const seen = new Set<string>();
    const statuses = [];

    for (const row of rows) {
      if (!row.leadId) continue;
      if (seen.has(row.email)) continue;
      seen.add(row.email);

      const meta = row.metadata as Record<string, unknown> | null;
      const journalistId = (meta?.journalistId as string) ?? null;
      const outletId = (meta?.outletId as string) ?? null;

      const result = statusMap.get(row.email);
      const flat = result
        ? flattenBrandStatus(result)
        : { contacted: false, delivered: false, bounced: false, replied: false, lastDeliveredAt: null };

      const qualification = qualificationsMap.get(row.email);
      const replyClassification = qualification
        ? classifyReply(qualification.classification)
        : null;

      statuses.push({
        leadId: row.leadId,
        email: row.email,
        journalistId,
        outletId,
        ...flat,
        replyClassification,
      });
    }

    res.json({ statuses });
  } catch (error) {
    console.error("[lead-service] Outreach status error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export { flattenBrandStatus };
export default router;
