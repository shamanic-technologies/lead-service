import { Router } from "express";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { pushLeads, pullNext } from "../lib/buffer.js";
import { ensureOrganization, createRun, updateRun } from "../lib/runs-client.js";

const router = Router();

router.post("/buffer/push", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { namespace, parentRunId, brandId, clerkOrgId, clerkUserId, leads } = req.body;

    console.log(`[Lead Service][buffer/push] Called for org=${req.organizationId} namespace=${namespace} leads=${Array.isArray(leads) ? leads.length : "invalid"} brandId=${brandId || "none"} clerkOrgId=${clerkOrgId || "none"}`);

    if (!namespace || !Array.isArray(leads)) {
      console.log("[Lead Service][buffer/push] Rejected: missing namespace or leads[]");
      return res.status(400).json({ error: "namespace and leads[] required" });
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
        console.error("[Lead Service][buffer/push] Failed to create run:", err);
      }
    }

    const result = await pushLeads({
      organizationId: req.organizationId!,
      namespace,
      pushRunId,
      brandId: brandId ?? null,
      clerkOrgId: clerkOrgId ?? null,
      clerkUserId: clerkUserId ?? null,
      leads,
    });

    console.log(`[Lead Service][buffer/push] Result: buffered=${result.buffered} skippedAlreadyServed=${result.skippedAlreadyServed}`);

    if (pushRunId) {
      try {
        await updateRun(pushRunId, "completed");
      } catch (err) {
        console.error("[Lead Service][buffer/push] Failed to update run:", err);
      }
    }

    res.json(result);
  } catch (error) {
    console.error("[Lead Service][buffer/push] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/buffer/next", authenticate, async (req: AuthenticatedRequest, res) => {
  try {
    const { namespace, parentRunId, searchParams, brandId, clerkOrgId, clerkUserId } = req.body;

    console.log(`[Lead Service][buffer/next] Called for org=${req.organizationId} namespace=${namespace} hasSearchParams=${!!searchParams} brandId=${brandId || "none"} clerkOrgId=${clerkOrgId || "none"}`);

    if (!namespace) {
      console.log("[Lead Service][buffer/next] Rejected: missing namespace");
      return res.status(400).json({ error: "namespace required" });
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
        console.error("[Lead Service][buffer/next] Failed to create run:", err);
      }
    }

    const result = await pullNext({
      organizationId: req.organizationId!,
      namespace,
      parentRunId: parentRunId ?? null,
      runId: serveRunId,
      searchParams: searchParams ?? undefined,
      brandId: brandId ?? null,
      clerkOrgId: clerkOrgId ?? null,
      clerkUserId: clerkUserId ?? null,
    });

    console.log(`[Lead Service][buffer/next] Result: found=${result.found} email=${result.lead?.email || "none"}`);

    if (serveRunId) {
      try {
        await updateRun(serveRunId, "completed");
      } catch (err) {
        console.error("[Lead Service][buffer/next] Failed to update run:", err);
      }
    }

    res.json(result);
  } catch (error) {
    console.error("[Lead Service][buffer/next] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
