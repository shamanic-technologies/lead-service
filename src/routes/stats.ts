import { Router } from "express";
import { eq, and, count, inArray, or, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer, organizations } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";

const router = Router();

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId, clerkOrgId, clerkUserId, appId, runIds } = req.query;
    const str = (v: unknown): string | undefined => typeof v === "string" ? v : undefined;
    const brandIdStr = str(brandId);
    const campaignIdStr = str(campaignId);
    const clerkOrgIdStr = str(clerkOrgId);
    const clerkUserIdStr = str(clerkUserId);
    const appIdStr = str(appId);
    const runIdList = typeof runIds === "string" ? runIds.split(",").filter(Boolean) : [];

    const servedConditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];
    const bufferConditions: SQL[] = [eq(leadBuffer.organizationId, req.organizationId!)];

    if (brandIdStr) {
      servedConditions.push(eq(servedLeads.brandId, brandIdStr));
      bufferConditions.push(eq(leadBuffer.brandId, brandIdStr));
    }
    if (campaignIdStr) {
      servedConditions.push(eq(servedLeads.campaignId, campaignIdStr));
      bufferConditions.push(eq(leadBuffer.campaignId, campaignIdStr));
    }
    if (clerkOrgIdStr) {
      servedConditions.push(eq(servedLeads.clerkOrgId, clerkOrgIdStr));
      bufferConditions.push(eq(leadBuffer.clerkOrgId, clerkOrgIdStr));
    }
    if (clerkUserIdStr) {
      servedConditions.push(eq(servedLeads.clerkUserId, clerkUserIdStr));
      bufferConditions.push(eq(leadBuffer.clerkUserId, clerkUserIdStr));
    }
    if (appIdStr) {
      const orgs = await db.query.organizations.findMany({
        where: eq(organizations.appId, appIdStr),
      });
      if (orgs.length > 0) {
        const orgIds = orgs.map((o) => o.id);
        servedConditions.push(inArray(servedLeads.organizationId, orgIds));
        bufferConditions.push(inArray(leadBuffer.organizationId, orgIds));
      } else {
        const emptyApollo = { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 };
        return res.json({ served: 0, buffered: 0, skipped: 0, apollo: emptyApollo });
      }
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
    if (appIdStr) apolloFilters.appId = appIdStr;
    if (runIdList.length > 0) apolloFilters.runIds = runIdList;

    const [servedResult, bufferRows, apollo] = await Promise.all([
      db.select({ count: count() }).from(servedLeads).where(and(...servedConditions)).then(([r]) => r),
      db.select({ status: leadBuffer.status, count: count() }).from(leadBuffer).where(and(...bufferConditions)).groupBy(leadBuffer.status),
      fetchApolloStats(apolloFilters as Parameters<typeof fetchApolloStats>[0], clerkOrgIdStr ?? req.externalOrgId),
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
