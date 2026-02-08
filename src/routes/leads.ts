import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, enrichments } from "../db/schema.js";

const router = Router();

router.get("/leads", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId, clerkOrgId, clerkUserId } = req.query;

    // Build filter conditions
    const conditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];

    if (brandId && typeof brandId === "string") {
      conditions.push(eq(servedLeads.brandId, brandId));
    }
    if (campaignId && typeof campaignId === "string") {
      conditions.push(eq(servedLeads.campaignId, campaignId));
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
