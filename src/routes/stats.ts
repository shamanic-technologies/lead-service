import { Router } from "express";
import { eq, and, count, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

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

export default router;
