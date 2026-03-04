import { Router } from "express";
import { eq, and, count, inArray, or, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";

const router = Router();

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId, orgId, userId, runIds } = req.query;
    const str = (v: unknown): string | undefined => typeof v === "string" ? v : undefined;
    const brandIdStr = str(brandId);
    const campaignIdStr = str(campaignId);
    const orgIdStr = str(orgId);
    const userIdStr = str(userId);
    const runIdList = typeof runIds === "string" ? runIds.split(",").filter(Boolean) : [];

    const servedConditions: SQL[] = [eq(servedLeads.orgId, req.orgId!)];
    const bufferConditions: SQL[] = [eq(leadBuffer.orgId, req.orgId!)];

    if (brandIdStr) {
      servedConditions.push(eq(servedLeads.brandId, brandIdStr));
      bufferConditions.push(eq(leadBuffer.brandId, brandIdStr));
    }
    if (campaignIdStr) {
      servedConditions.push(eq(servedLeads.campaignId, campaignIdStr));
      bufferConditions.push(eq(leadBuffer.campaignId, campaignIdStr));
    }
    if (orgIdStr) {
      servedConditions.push(eq(servedLeads.orgId, orgIdStr));
      bufferConditions.push(eq(leadBuffer.orgId, orgIdStr));
    }
    if (userIdStr) {
      servedConditions.push(eq(servedLeads.userId, userIdStr));
      bufferConditions.push(eq(leadBuffer.userId, userIdStr));
    }
    if (runIdList.length > 0) {
      servedConditions.push(
        or(
          inArray(servedLeads.parentRunId, runIdList),
          inArray(servedLeads.runId, runIdList)
        )!
      );
      bufferConditions.push(inArray(leadBuffer.pushRunId, runIdList));
    }

    const apolloFilters: Record<string, unknown> = {};
    if (brandIdStr) apolloFilters.brandId = brandIdStr;
    if (campaignIdStr) apolloFilters.campaignId = campaignIdStr;
    if (runIdList.length > 0) apolloFilters.runIds = runIdList;

    const [servedResult, bufferRows, apollo] = await Promise.all([
      db.select({ count: count() }).from(servedLeads).where(and(...servedConditions)).then(([r]) => r),
      db.select({ status: leadBuffer.status, count: count() }).from(leadBuffer).where(and(...bufferConditions)).groupBy(leadBuffer.status),
      fetchApolloStats(apolloFilters as Parameters<typeof fetchApolloStats>[0], orgIdStr ?? req.orgId, { userId: req.userId, runId: req.runId }),
    ]);

    const bufferByStatus = Object.fromEntries(bufferRows.map((r) => [r.status, r.count]));

    res.json({
      served: servedResult?.count ?? 0,
      buffered: bufferByStatus["buffered"] ?? 0,
      skipped: bufferByStatus["skipped"] ?? 0,
      apollo,
    });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
