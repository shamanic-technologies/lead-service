import { Router } from "express";
import { eq, and, count, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

const router = Router();

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { brandId, campaignId } = req.query;

    console.log(`[stats] GET /stats called for org=${req.organizationId} brandId=${brandId || "all"} campaignId=${campaignId || "all"}`);

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
    console.log(`[stats] org=${req.organizationId} brandId=${brandId || "all"} campaignId=${campaignId || "all"} totalServed=${totalServed}`);

    res.json({ totalServed });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
