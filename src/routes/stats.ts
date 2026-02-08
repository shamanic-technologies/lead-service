import { Router } from "express";
import { eq, and, count, inArray, or, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer, organizations } from "../db/schema.js";

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
      description: 'Lead stats by status',
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["served", "buffered", "skipped"],
            properties: {
              served: { type: "integer", description: "Leads with verified email, delivered to campaign" },
              buffered: { type: "integer", description: "Leads awaiting email enrichment" },
              skipped: { type: "integer", description: "Leads where no email was found" }
            }
          }
        }
      }
    }
  */
  try {
    const { brandId, campaignId } = req.query;

    const servedConditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];
    const bufferConditions: SQL[] = [eq(leadBuffer.organizationId, req.organizationId!)];

    if (brandId && typeof brandId === "string") {
      servedConditions.push(eq(servedLeads.brandId, brandId));
      bufferConditions.push(eq(leadBuffer.brandId, brandId));
    }
    if (campaignId && typeof campaignId === "string") {
      servedConditions.push(eq(servedLeads.campaignId, campaignId));
      bufferConditions.push(eq(leadBuffer.campaignId, campaignId));
    }

    const [servedResult] = await db
      .select({ count: count() })
      .from(servedLeads)
      .where(and(...servedConditions));

    const bufferRows = await db
      .select({ status: leadBuffer.status, count: count() })
      .from(leadBuffer)
      .where(and(...bufferConditions))
      .groupBy(leadBuffer.status);

    const bufferByStatus = Object.fromEntries(bufferRows.map((r) => [r.status, r.count]));

    res.json({
      served: servedResult?.count ?? 0,
      buffered: bufferByStatus["buffered"] ?? 0,
      skipped: bufferByStatus["skipped"] ?? 0,
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
      description: 'Lead stats by status',
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["served", "buffered", "skipped"],
            properties: {
              served: { type: "integer", description: "Leads with verified email, delivered to campaign" },
              buffered: { type: "integer", description: "Leads awaiting email enrichment" },
              skipped: { type: "integer", description: "Leads where no email was found" }
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
        return res.json({ served: 0, buffered: 0, skipped: 0 });
      }
    }

    const servedWhere = servedConditions.length > 0 ? and(...servedConditions) : undefined;
    const bufferWhere = bufferConditions.length > 0 ? and(...bufferConditions) : undefined;

    const [servedResult] = await db
      .select({ count: count() })
      .from(servedLeads)
      .where(servedWhere);

    const bufferRows = await db
      .select({ status: leadBuffer.status, count: count() })
      .from(leadBuffer)
      .where(bufferWhere)
      .groupBy(leadBuffer.status);

    const bufferByStatus = Object.fromEntries(bufferRows.map((r) => [r.status, r.count]));

    res.json({
      served: servedResult?.count ?? 0,
      buffered: bufferByStatus["buffered"] ?? 0,
      skipped: bufferByStatus["skipped"] ?? 0,
    });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
