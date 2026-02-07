import { Router } from "express";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { pushLeads, pullNext } from "../lib/buffer.js";
import { ensureOrganization, createRun, updateRun } from "../lib/runs-client.js";

const router = Router();

router.post("/buffer/push", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { campaignId, brandId, parentRunId, clerkUserId, leads } = req.body;
    const clerkOrgId = req.externalOrgId ?? null;

    if (!campaignId || !brandId || !Array.isArray(leads)) {
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
      clerkOrgId,
      clerkUserId: clerkUserId ?? null,
      leads,
    });

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
    const { campaignId, brandId, parentRunId, searchParams, clerkUserId } = req.body;
    const clerkOrgId = req.externalOrgId ?? null;

    if (!campaignId || !brandId) {
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
      clerkOrgId,
      clerkUserId: clerkUserId ?? null,
    });

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
