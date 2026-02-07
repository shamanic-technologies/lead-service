import { Router } from "express";
import { eq, and, count, inArray, or, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, organizations } from "../db/schema.js";

const router = Router();

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
  /*
    #swagger.summary = 'Get served lead count'
    #swagger.parameters['x-app-id'] = { in: 'header', required: true, type: 'string', description: 'Identifies the calling application, e.g. mcpfactory' }
    #swagger.parameters['x-org-id'] = { in: 'header', required: true, type: 'string', description: 'External organization ID, e.g. Clerk org ID' }
    #swagger.parameters['brandId'] = { in: 'query', type: 'string', required: false }
    #swagger.parameters['campaignId'] = { in: 'query', type: 'string', required: false }
  */
  try {
    const { brandId, campaignId } = req.query;

    const conditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];

    if (brandId && typeof brandId === "string") {
      conditions.push(eq(servedLeads.brandId, brandId));
    }
    if (campaignId && typeof campaignId === "string") {
      conditions.push(eq(servedLeads.campaignId, campaignId));
    }

    const [result] = await db
      .select({ totalServed: count() })
      .from(servedLeads)
      .where(and(...conditions));

    const totalServed = result?.totalServed ?? 0;
    res.json({ totalServed });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/stats", async (req, res) => {
  /*
    #swagger.summary = 'Get served lead count (internal)'
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
  */
  try {
    const { runIds, appId, brandId, campaignId, clerkOrgId } = req.body ?? {};

    const conditions: SQL[] = [];

    if (runIds && Array.isArray(runIds) && runIds.length > 0) {
      conditions.push(
        or(
          inArray(servedLeads.parentRunId, runIds),
          inArray(servedLeads.runId, runIds)
        )!
      );
    }
    if (brandId) {
      conditions.push(eq(servedLeads.brandId, brandId));
    }
    if (campaignId) {
      conditions.push(eq(servedLeads.campaignId, campaignId));
    }
    if (clerkOrgId) {
      conditions.push(eq(servedLeads.clerkOrgId, clerkOrgId));
    }
    if (appId) {
      const orgs = await db.query.organizations.findMany({
        where: eq(organizations.appId, appId),
      });
      if (orgs.length > 0) {
        conditions.push(
          inArray(servedLeads.organizationId, orgs.map((o) => o.id))
        );
      } else {
        return res.json({ totalServed: 0 });
      }
    }

    const where = conditions.length > 0 ? and(...conditions) : undefined;

    const [result] = await db
      .select({ totalServed: count() })
      .from(servedLeads)
      .where(where);

    const totalServed = result?.totalServed ?? 0;
    res.json({ totalServed });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
