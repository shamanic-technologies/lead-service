import { Router } from "express";
import { eq, lt } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { pullNext } from "../lib/buffer.js";
import { createRun, updateRun } from "../lib/runs-client.js";
import { BufferNextRequestSchema } from "../schemas.js";
import { db } from "../db/index.js";
import { idempotencyCache } from "../db/schema.js";

const router = Router();

const IDEMPOTENCY_TTL_DAYS = 60;

function pruneExpiredIdempotencyCache(): void {
  const cutoff = new Date(Date.now() - IDEMPOTENCY_TTL_DAYS * 24 * 60 * 60 * 1000);
  db.delete(idempotencyCache)
    .where(lt(idempotencyCache.createdAt, cutoff))
    .then((result) => {
      if (result.length > 0) {
        console.log(`[lead-service] Pruned ${result.length} expired idempotency cache entries`);
      }
    })
    .catch((err) => {
      console.warn("[lead-service] Failed to prune expired idempotency cache:", err);
    });
}

router.post("/buffer/next", authenticate, async (req: AuthenticatedRequest, res) => {
  const parsed = BufferNextRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid request", details: parsed.error.flatten() });
  }

  const campaignId = req.campaignId;
  const brandId = req.brandId;

  if (!campaignId || !brandId) {
    return res.status(400).json({ error: "x-campaign-id and x-brand-id headers required" });
  }

  const { sourceType } = parsed.data;
  const workflowSlug = req.workflowSlug;
  const runId = req.runId!;

  const runMeta = {
    orgId: req.orgId,
    userId: req.userId,
    campaignId,
    brandId,
    workflowSlug,
    featureSlug: req.featureSlug,
  };

  // Idempotency on x-run-id: if this run already got a lead, return the cached response
  const cached = await db.query.idempotencyCache.findFirst({
    where: eq(idempotencyCache.idempotencyKey, runId),
  });
  if (cached) {
    console.log(`[lead-service] Idempotency hit for runId=${runId}`);
    return res.json(cached.response);
  }

  // Create child run for traceability (x-run-id from caller becomes our parentRunId)
  const childRun = await createRun({
    orgId: req.orgId!,
    serviceName: "lead-service",
    taskName: "lead-serve",
    parentRunId: runId,
    userId: req.userId,
    brandId,
    campaignId,
    workflowSlug,
    featureSlug: req.featureSlug,
  });
  const serveRunId = childRun.id;

  try {
    const result = await pullNext({
      orgId: req.orgId!,
      campaignId,
      brandId,
      runId: serveRunId,
      userId: req.userId ?? null,
      workflowSlug,
      featureSlug: req.featureSlug,
      sourceType,
    });

    // Cache response keyed by caller's runId for idempotency
    if (Math.random() < 0.01) pruneExpiredIdempotencyCache();
    try {
      await db.insert(idempotencyCache).values({
        idempotencyKey: runId,
        orgId: req.orgId!,
        response: result,
      });
    } catch (err) {
      // Ignore duplicate key errors (race condition between concurrent retries)
      console.warn("[lead-service] Failed to cache idempotency response:", err);
    }

    const runStatus = result.found ? "completed" : "failed";
    await updateRun(serveRunId, runStatus, runMeta);

    res.json(result);
  } catch (error) {
    console.error("[lead-service] buffer/next error:", error);
    try {
      await updateRun(serveRunId, "failed", runMeta);
    } catch (runErr) {
      console.error("[lead-service] Failed to close run after error:", runErr);
    }
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
