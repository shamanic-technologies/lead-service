import { Router } from "express";
import { eq, and, count, inArray, or, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer, organizations } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";

const router = Router();

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
  /*
    #swagger.summary = 'Get lead stats by status'
    #swagger.description = 'Returns counts of leads by status: served (delivered with verified email), buffered (awaiting enrichment), and skipped (no email found).'
    #swagger.parameters['x-app-id'] = { in: 'header', required: true, type: 'string', description: 'Identifies the calling application, e.g. mcpfactory' }
    #swagger.parameters['x-org-id'] = { in: 'header', required: true, type: 'string', description: 'External organization ID, e.g. Clerk org ID' }
    #swagger.parameters['brandId'] = { in: 'query', type: 'string', required: false }
    #swagger.parameters['campaignId'] = { in: 'query', type: 'string', required: false }
    #swagger.responses[200] = {
      description: 'Lead stats by status with Apollo search/enrichment metrics',
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["served", "buffered", "skipped", "apollo"],
            properties: {
              served: { type: "integer", description: "Leads with verified email, delivered to campaign" },
              buffered: { type: "integer", description: "Leads awaiting email enrichment" },
              skipped: { type: "integer", description: "Leads where no email was found" },
              apollo: {
                type: "object",
                properties: {
                  enrichedLeadsCount: { type: "integer", description: "Number of enriched lead records" },
                  searchCount: { type: "integer", description: "Number of search operations performed" },
                  fetchedPeopleCount: { type: "integer", description: "Sum of people returned from searches" },
                  totalMatchingPeople: { type: "integer", description: "Sum of total matching people in Apollo" }
                }
              }
            }
          }
        }
      }
    }
  */
  try {
    const { brandId, campaignId } = req.query;
    const brandIdStr = typeof brandId === "string" ? brandId : undefined;
    const campaignIdStr = typeof campaignId === "string" ? campaignId : undefined;

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

    const [servedResult, bufferRows, apollo] = await Promise.all([
      db.select({ count: count() }).from(servedLeads).where(and(...servedConditions)).then(([r]) => r),
      db.select({ status: leadBuffer.status, count: count() }).from(leadBuffer).where(and(...bufferConditions)).groupBy(leadBuffer.status),
      fetchApolloStats({ brandId: brandIdStr, campaignId: campaignIdStr }, req.externalOrgId),
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

router.post("/stats", async (req, res) => {
  /*
    #swagger.summary = 'Get lead stats by status (internal)'
    #swagger.description = 'Service-to-service endpoint. Returns counts of leads by status: served (delivered with verified email), buffered (awaiting enrichment), and skipped (no email found).'
    #swagger.requestBody = {
      required: true,
      content: {
        "application/json": {
          schema: {
            type: "object",
            properties: {
              runIds: { type: "array", items: { type: "string" } },
              appId: { type: "string" },
              brandId: { type: "string" },
              campaignId: { type: "string" },
              clerkOrgId: { type: "string" }
            }
          }
        }
      }
    }
    #swagger.responses[200] = {
      description: 'Lead stats by status with Apollo search/enrichment metrics',
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["served", "buffered", "skipped", "apollo"],
            properties: {
              served: { type: "integer", description: "Leads with verified email, delivered to campaign" },
              buffered: { type: "integer", description: "Leads awaiting email enrichment" },
              skipped: { type: "integer", description: "Leads where no email was found" },
              apollo: {
                type: "object",
                properties: {
                  enrichedLeadsCount: { type: "integer", description: "Number of enriched lead records" },
                  searchCount: { type: "integer", description: "Number of search operations performed" },
                  fetchedPeopleCount: { type: "integer", description: "Sum of people returned from searches" },
                  totalMatchingPeople: { type: "integer", description: "Sum of total matching people in Apollo" }
                }
              }
            }
          }
        }
      }
    }
  */
  try {
    const { runIds, appId, brandId, campaignId, clerkOrgId } = req.body ?? {};

    const servedConditions: SQL[] = [];
    const bufferConditions: SQL[] = [];

    if (runIds && Array.isArray(runIds) && runIds.length > 0) {
      servedConditions.push(
        or(
          inArray(servedLeads.parentRunId, runIds),
          inArray(servedLeads.runId, runIds)
        )!
      );
      bufferConditions.push(inArray(leadBuffer.pushRunId, runIds));
    }
    if (brandId) {
      servedConditions.push(eq(servedLeads.brandId, brandId));
      bufferConditions.push(eq(leadBuffer.brandId, brandId));
    }
    if (campaignId) {
      servedConditions.push(eq(servedLeads.campaignId, campaignId));
      bufferConditions.push(eq(leadBuffer.campaignId, campaignId));
    }
    if (clerkOrgId) {
      servedConditions.push(eq(servedLeads.clerkOrgId, clerkOrgId));
      bufferConditions.push(eq(leadBuffer.clerkOrgId, clerkOrgId));
    }
    if (appId) {
      const orgs = await db.query.organizations.findMany({
        where: eq(organizations.appId, appId),
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

    const servedWhere = servedConditions.length > 0 ? and(...servedConditions) : undefined;
    const bufferWhere = bufferConditions.length > 0 ? and(...bufferConditions) : undefined;

    const apolloFilters: Record<string, unknown> = {};
    if (runIds) apolloFilters.runIds = runIds;
    if (appId) apolloFilters.appId = appId;
    if (brandId) apolloFilters.brandId = brandId;
    if (campaignId) apolloFilters.campaignId = campaignId;

    const [servedResult, bufferRows, apollo] = await Promise.all([
      db.select({ count: count() }).from(servedLeads).where(servedWhere).then(([r]) => r),
      db.select({ status: leadBuffer.status, count: count() }).from(leadBuffer).where(bufferWhere).groupBy(leadBuffer.status),
      fetchApolloStats(apolloFilters as Parameters<typeof fetchApolloStats>[0], clerkOrgId),
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
