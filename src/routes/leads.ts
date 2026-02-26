import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

const router = Router();

export function extractEnrichment(metadata: unknown): Record<string, unknown> | null {
  if (!metadata || typeof metadata !== "object") return null;
  const m = metadata as Record<string, unknown>;
  // Only return enrichment if at least one person field exists
  if (!m.firstName && !m.lastName && !m.email) return null;
  // Pass through all fields from Apollo — no filtering
  return { ...m };
}

router.get("/leads", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId, orgId, userId } = req.query;

    // Build filter conditions
    const conditions: SQL[] = [eq(servedLeads.organizationId, req.organizationId!)];

    if (brandId && typeof brandId === "string") {
      conditions.push(eq(servedLeads.brandId, brandId));
    }
    if (campaignId && typeof campaignId === "string") {
      conditions.push(eq(servedLeads.campaignId, campaignId));
    }
    if (orgId && typeof orgId === "string") {
      conditions.push(eq(servedLeads.orgId, orgId));
    }
    if (userId && typeof userId === "string") {
      conditions.push(eq(servedLeads.userId, userId));
    }

    // Get served leads
    const leads = await db.query.servedLeads.findMany({
      where: and(...conditions),
    });

    // Extract enrichment from metadata (Apollo data is stored in servedLeads.metadata)
    const enrichedLeads = leads.map((lead) => ({
      ...lead,
      leadId: lead.leadId ?? null,
      enrichment: extractEnrichment(lead.metadata),
    }));

    res.json({ leads: enrichedLeads });
  } catch (error) {
    console.error("[leads] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
