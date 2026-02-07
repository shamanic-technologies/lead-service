import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, enrichments } from "../db/schema.js";

const router = Router();

router.get("/leads", authenticate, async (req: AuthenticatedRequest, res) => {
  /*
    #swagger.summary = 'List served leads with enrichment data'
    #swagger.parameters['x-app-id'] = { in: 'header', required: true, type: 'string', description: 'Identifies the calling application, e.g. mcpfactory' }
    #swagger.parameters['x-org-id'] = { in: 'header', required: true, type: 'string', description: 'External organization ID, e.g. Clerk org ID' }
    #swagger.parameters['brandId'] = { in: 'query', type: 'string', required: false }
    #swagger.parameters['clerkOrgId'] = { in: 'query', type: 'string', required: false }
    #swagger.parameters['clerkUserId'] = { in: 'query', type: 'string', required: false }
  */
  try {
    const { brandId, clerkOrgId, clerkUserId } = req.query;

    // Build filter conditions
    const conditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];

    if (brandId && typeof brandId === "string") {
      conditions.push(eq(servedLeads.brandId, brandId));
    }
    if (clerkOrgId && typeof clerkOrgId === "string") {
      conditions.push(eq(servedLeads.clerkOrgId, clerkOrgId));
    }
    if (clerkUserId && typeof clerkUserId === "string") {
      conditions.push(eq(servedLeads.clerkUserId, clerkUserId));
    }

    // Get served leads
    const leads = await db.query.servedLeads.findMany({
      where: and(...conditions),
    });

    // Get enrichment data for all leads
    const emails = leads.map((l) => l.email.toLowerCase());
    const enrichmentData = emails.length > 0
      ? await db.query.enrichments.findMany({
          where: (table, { inArray }) => inArray(table.email, emails),
        })
      : [];

    // Create email -> enrichment map
    const enrichmentMap = new Map(
      enrichmentData.map((e) => [e.email.toLowerCase(), e])
    );

    // Join leads with enrichments
    const enrichedLeads = leads.map((lead) => ({
      ...lead,
      enrichment: enrichmentMap.get(lead.email.toLowerCase()) ?? null,
    }));

    res.json({ leads: enrichedLeads });
  } catch (error) {
    console.error("[leads] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
