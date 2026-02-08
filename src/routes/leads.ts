import { Router } from "express";
import { eq, and, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

const router = Router();

function extractEnrichment(metadata: unknown): Record<string, unknown> | null {
  if (!metadata || typeof metadata !== "object") return null;
  const m = metadata as Record<string, unknown>;
  // Only return enrichment if at least one person field exists
  if (!m.firstName && !m.lastName && !m.email) return null;
  return {
    firstName: m.firstName ?? null,
    lastName: m.lastName ?? null,
    title: m.title ?? null,
    linkedinUrl: m.linkedinUrl ?? m.linkedin_url ?? null,
    organizationName: m.organizationName ?? m.organization_name ?? null,
    organizationDomain: m.organizationDomain ?? m.organization_domain ?? null,
    organizationIndustry: m.organizationIndustry ?? m.organization_industry ?? null,
    organizationSize: m.organizationSize ?? m.organization_size ?? null,
  };
}

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

    // Extract enrichment from metadata (Apollo data is stored in servedLeads.metadata)
    const enrichedLeads = leads.map((lead) => ({
      ...lead,
      enrichment: extractEnrichment(lead.metadata),
    }));

    res.json({ leads: enrichedLeads });
  } catch (error) {
    console.error("[leads] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
