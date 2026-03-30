import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate, getServiceContext } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";
import {
  checkDeliveryStatus,
  type StatusResult,
  type DeliveryStatusItem,
} from "../lib/email-gateway-client.js";

const router = Router();

function flattenStatus(result: StatusResult) {
  const bc = result.broadcast;
  const tx = result.transactional;

  const contacted = !!(
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

  const delivered = !!(
    bc?.campaign.email.delivered ||
    tx?.campaign.email.delivered
  );

  const bounced = !!(
    bc?.campaign.email.bounced ||
    tx?.campaign.email.bounced
  );

  const replied = !!(
    bc?.campaign.lead.replied ||
    tx?.campaign.lead.replied
  );

  const lastDeliveredAt =
    bc?.campaign.email.lastDeliveredAt ??
    tx?.campaign.email.lastDeliveredAt ??
    null;

  return { contacted, delivered, bounced, replied, lastDeliveredAt };
}

router.get("/leads/status", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const campaignId = typeof req.query.campaignId === "string" ? req.query.campaignId : undefined;
    if (!campaignId) {
      res.status(400).json({ error: "campaignId query parameter is required" });
      return;
    }

    const conditions: SQL[] = [
      eq(servedLeads.orgId, req.orgId!),
      eq(servedLeads.campaignId, campaignId),
    ];

    const brandId = typeof req.query.brandId === "string" ? req.query.brandId : undefined;
    if (brandId) {
      conditions.push(eq(servedLeads.brandId, brandId));
    }

    const rows = await db
      .select({
        leadId: servedLeads.leadId,
        email: servedLeads.email,
        brandId: servedLeads.brandId,
      })
      .from(servedLeads)
      .where(and(...conditions));

    if (rows.length === 0) {
      res.json({ statuses: [] });
      return;
    }

    // Group by brandId since email-gateway scopes status per brand/campaign
    const groups = new Map<string, { brandId: string; items: DeliveryStatusItem[] }>();
    for (const row of rows) {
      if (!row.leadId) continue;
      const key = row.brandId;
      if (!groups.has(key)) {
        groups.set(key, { brandId: row.brandId, items: [] });
      }
      groups.get(key)!.items.push({ leadId: row.leadId, email: row.email });
    }

    const context = getServiceContext(req);
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

    const statuses = rows
      .filter((row) => row.leadId)
      .map((row) => {
        const result = statusMap.get(row.email);
        const flat = result
          ? flattenStatus(result)
          : { contacted: false, delivered: false, bounced: false, replied: false, lastDeliveredAt: null };

        return {
          leadId: row.leadId!,
          email: row.email,
          ...flat,
        };
      });

    res.json({ statuses });
  } catch (error) {
    console.error("[lead-service] Lead status error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export { flattenStatus };
export default router;
