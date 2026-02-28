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
        console.log(`[idempotency] Pruned ${result.length} expired cache entries`);
      }
    })
    .catch((err) => {
      console.warn("[idempotency] Failed to prune expired cache:", err);
    });
}

router.post("/buffer/next", authenticate, async (req: AuthenticatedRequest, res) => {
  const parsed = BufferNextRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Invalid request", details: parsed.error.flatten() });
  }

  try {
    const { campaignId, brandId, parentRunId, keySource, searchParams, userId, workflowName, idempotencyKey } = parsed.data;
    const orgId = req.externalOrgId ?? null;

    // Idempotency: return cached response if this key was already processed
    if (idempotencyKey) {
      const cached = await db.query.idempotencyCache.findFirst({
        where: eq(idempotencyCache.idempotencyKey, idempotencyKey),
      });
      if (cached) {
        console.log(`[buffer/next] Idempotency hit for key=${idempotencyKey}`);
        return res.json(cached.response);
      }
    }

    // Create child run for traceability
    const childRun = await createRun({
      orgId: req.externalOrgId!,
      appId: req.appId,
      serviceName: "lead-service",
      taskName: "lead-serve",
      parentRunId,
      userId,
      brandId,
      campaignId,
      workflowName,
    });
    const serveRunId = childRun.id;

    const result = await pullNext({
      organizationId: req.organizationId!,
      campaignId,
      brandId,
      parentRunId,
      runId: serveRunId,
      keySource,
      searchParams: searchParams ?? undefined,
      orgId,
      userId: userId ?? null,
      appId: req.appId,
      workflowName,
    });

    // Cache the response for idempotency + probabilistic TTL cleanup (~1% of requests)
    if (idempotencyKey) {
      if (Math.random() < 0.01) pruneExpiredIdempotencyCache();
      try {
        await db.insert(idempotencyCache).values({
          idempotencyKey,
          organizationId: req.organizationId!,
          response: result,
        });
      } catch (err) {
        // Ignore duplicate key errors (race condition between concurrent retries)
        console.warn("[buffer/next] Failed to cache idempotency response:", err);
      }
    }

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
