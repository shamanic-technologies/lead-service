import { Router } from "express";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { getEnrichment } from "../lib/enrichment.js";

const router = Router();

router.post("/enrich", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({ error: "email required" });
    }

    const enrichment = await getEnrichment(email);

    if (!enrichment) {
      return res.status(404).json({ error: "Could not enrich email" });
    }

    res.json(enrichment);
  } catch (error) {
    console.error("[enrich] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
