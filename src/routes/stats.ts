import { Router } from "express";
import { eq, and, count } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

const router = Router();

router.get("/stats/:namespace", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { namespace } = req.params;

    console.log(`[stats] GET /stats/${namespace} called for org=${req.organizationId}`);

    const [result] = await db
      .select({ totalServed: count() })
      .from(servedLeads)
      .where(
        and(
          eq(servedLeads.organizationId, req.organizationId!),
          eq(servedLeads.brandId, namespace)
        )
      );

    const totalServed = result?.totalServed ?? 0;
    console.log(`[stats] brand=${namespace} org=${req.organizationId} totalServed=${totalServed}`);

    res.json({ totalServed });
  } catch (error) {
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
