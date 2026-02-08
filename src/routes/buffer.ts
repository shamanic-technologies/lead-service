import { Router } from "express";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { pushLeads, pullNext } from "../lib/buffer.js";
import { createRun, updateRun } from "../lib/runs-client.js";
import { BufferPushRequestSchema, BufferNextRequestSchema } from "../schemas.js";

const router = Router();

router.post("/buffer/push", authenticate, async (req: AuthenticatedRequest, res) => {
  const parsed = BufferPushRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid request", details: parsed.error.flatten() });
  }

  try {
    const { campaignId, brandId, parentRunId, clerkUserId, leads } = parsed.data;
    const clerkOrgId = req.externalOrgId ?? null;

    // Create child run for traceability
    const childRun = await createRun({
      clerkOrgId: req.externalOrgId!,
      appId: req.appId || "mcpfactory",
      serviceName: "lead-service",
      taskName: "buffer-push",
      parentRunId,
      clerkUserId,
      brandId,
      campaignId,
    });
    const pushRunId = childRun.id;

    const result = await pushLeads({
      organizationId: req.organizationId!,
      campaignId,
      brandId,
      pushRunId,
      clerkOrgId,
      clerkUserId: clerkUserId ?? null,
      leads,
    });

    try {
      await updateRun(pushRunId, "completed");
    } catch (err) {
      console.error("[buffer/push] Failed to update run:", err);
    }

    res.json(result);
  } catch (error) {
    console.error("[buffer/push] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/buffer/next", authenticate, async (req: AuthenticatedRequest, res) => {
  const parsed = BufferNextRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid request", details: parsed.error.flatten() });
  }

  try {
    const { campaignId, brandId, parentRunId, searchParams, clerkUserId } = parsed.data;
    const clerkOrgId = req.externalOrgId ?? null;

    // Create child run for traceability
    const childRun = await createRun({
      clerkOrgId: req.externalOrgId!,
      appId: req.appId || "mcpfactory",
      serviceName: "lead-service",
      taskName: "lead-serve",
      parentRunId,
      clerkUserId,
      brandId,
      campaignId,
    });
    const serveRunId = childRun.id;

    const result = await pullNext({
      organizationId: req.organizationId!,
      campaignId,
      brandId,
      parentRunId,
      runId: serveRunId,
      searchParams: searchParams ?? undefined,
      clerkOrgId,
      clerkUserId: clerkUserId ?? null,
      appId: req.appId,
    });

    try {
      await updateRun(serveRunId, "completed");
    } catch (err) {
      console.error("[buffer/next] Failed to update run:", err);
    }

    res.json(result);
  } catch (error) {
    console.error("[buffer/next] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
