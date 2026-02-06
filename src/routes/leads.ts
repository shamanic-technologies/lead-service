import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, enrichments } from "../db/schema.js";

const router = Router();

router.get("/leads", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, clerkOrgId, clerkUserId } = req.query;

    const filters = {
      organizationId: req.organizationId,
      brandId: brandId || null,
      clerkOrgId: clerkOrgId || null,
      clerkUserId: clerkUserId || null,
    };
    console.log("[Lead Service][leads] GET /leads called with filters:", JSON.stringify(filters));

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

    console.log(`[Lead Service][leads] Found ${leads.length} served leads for org=${req.organizationId}`);
    if (leads.length === 0) {
      console.log("[Lead Service][leads] 0 leads returned. Possible causes: no leads served yet for this org, or filters too restrictive. Filters applied:", JSON.stringify(filters));
    }

    // Get enrichment data for all leads
    const emails = leads.map((l) => l.email.toLowerCase());
    const enrichmentData = emails.length > 0
      ? await db.query.enrichments.findMany({
          where: (table, { inArray }) => inArray(table.email, emails),
        })
      : [];

    if (leads.length > 0) {
      console.log(`[Lead Service][leads] Found ${enrichmentData.length}/${leads.length} enrichments`);
    }

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
    console.error("[Lead Service][leads] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
