import { Router } from "express";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { pushLeads, pullNext } from "../lib/buffer.js";
import { ensureOrganization, createRun, updateRun } from "../lib/runs-client.js";

const router = Router();

router.post("/buffer/push", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { campaignId, brandId, parentRunId, clerkOrgId, clerkUserId, leads } = req.body;

    console.log(`[buffer/push] Called for org=${req.organizationId} campaignId=${campaignId || "none"} brandId=${brandId || "none"} leads=${Array.isArray(leads) ? leads.length : "invalid"} clerkOrgId=${clerkOrgId || "none"}`);

    if (!campaignId || !brandId || !Array.isArray(leads)) {
      console.log("[buffer/push] Rejected: missing campaignId, brandId, or leads[]");
      return res.status(400).json({ error: "campaignId, brandId, and leads[] required" });
    }

    // Create child run for traceability
    let pushRunId: string | null = null;
    if (parentRunId && req.externalOrgId) {
      try {
        const runsOrgId = await ensureOrganization(req.externalOrgId);
        const childRun = await createRun({
          organizationId: runsOrgId,
          serviceName: "lead-service",
          taskName: "buffer-push",
          parentRunId,
        });
        pushRunId = childRun.id;
      } catch (err) {
        console.error("[buffer/push] Failed to create run:", err);
      }
    }

    const result = await pushLeads({
      organizationId: req.organizationId!,
      campaignId,
      brandId,
      pushRunId,
      clerkOrgId: clerkOrgId ?? null,
      clerkUserId: clerkUserId ?? null,
      leads,
    });

    console.log(`[buffer/push] Result: buffered=${result.buffered} skippedAlreadyServed=${result.skippedAlreadyServed}`);

    if (pushRunId) {
      try {
        await updateRun(pushRunId, "completed");
      } catch (err) {
        console.error("[buffer/push] Failed to update run:", err);
      }
    }

    res.json(result);
  } catch (error) {
    console.error("[buffer/push] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/buffer/next", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { campaignId, brandId, parentRunId, searchParams, clerkOrgId, clerkUserId } = req.body;

    console.log(`[buffer/next] Called for org=${req.organizationId} campaignId=${campaignId || "none"} brandId=${brandId || "none"} hasSearchParams=${!!searchParams} clerkOrgId=${clerkOrgId || "none"}`);

    if (!campaignId || !brandId) {
      console.log("[buffer/next] Rejected: missing campaignId or brandId");
      return res.status(400).json({ error: "campaignId and brandId required" });
    }

    // Create child run for traceability
    let serveRunId: string | null = null;
    if (parentRunId && req.externalOrgId) {
      try {
        const runsOrgId = await ensureOrganization(req.externalOrgId);
        const childRun = await createRun({
          organizationId: runsOrgId,
          serviceName: "lead-service",
          taskName: "lead-serve",
          parentRunId,
        });
        serveRunId = childRun.id;
      } catch (err) {
        console.error("[buffer/next] Failed to create run:", err);
      }
    }

    const result = await pullNext({
      organizationId: req.organizationId!,
      campaignId,
      brandId,
      parentRunId: parentRunId ?? null,
      runId: serveRunId,
      searchParams: searchParams ?? undefined,
      clerkOrgId: clerkOrgId ?? null,
      clerkUserId: clerkUserId ?? null,
    });

    console.log(`[buffer/next] Result: found=${result.found} email=${result.lead?.email || "none"}`);

    if (serveRunId) {
      try {
        await updateRun(serveRunId, "completed");
      } catch (err) {
        console.error("[buffer/next] Failed to update run:", err);
      }
    }

    res.json(result);
  } catch (error) {
    console.error("[buffer/next] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
