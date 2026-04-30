import { Router } from "express";
import { z } from "zod";
import { eq, and, sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { apiKeyAuth } from "../middleware/auth.js";
import { traceEvent } from "../lib/trace-event.js";

const router = Router();

const TransferBrandBodySchema = z.object({
  sourceBrandId: z.string().uuid(),
  sourceOrgId: z.string().uuid(),
  targetOrgId: z.string().uuid(),
  targetBrandId: z.string().uuid().optional(),
});

router.post("/internal/transfer-brand", apiKeyAuth, async (req, res) => {
  const parsed = TransferBrandBodySchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const { sourceBrandId, sourceOrgId, targetOrgId, targetBrandId } = parsed.data;

  const runId = req.headers["x-run-id"] as string | undefined;
  if (runId) traceEvent(runId, { service: "lead-service", event: "transfer-brand-start", detail: `sourceBrandId=${sourceBrandId}, sourceOrgId=${sourceOrgId}, targetOrgId=${targetOrgId}` }, req.headers).catch(() => {});

  console.log(
    `[lead-service] Transfer brand ${sourceBrandId} from org ${sourceOrgId} to org ${targetOrgId}` +
      (targetBrandId ? ` (rewrite to ${targetBrandId})` : "")
  );

  // Solo-brand condition: brand_ids array has exactly one element and it equals sourceBrandId
  const soloBrandCondition = sql`array_length(brand_ids, 1) = 1 AND brand_ids[1] = ${sourceBrandId}`;

  // Step 1: Move org — UPDATE SET org_id = targetOrgId WHERE brand_id = sourceBrandId AND org_id = sourceOrgId
  const servedStep1 = await db
    .update(servedLeads)
    .set({ orgId: targetOrgId })
    .where(
      and(eq(servedLeads.orgId, sourceOrgId), soloBrandCondition)
    )
    .returning({ id: servedLeads.id });

  const bufferStep1 = await db
    .update(leadBuffer)
    .set({ orgId: targetOrgId })
    .where(
      and(
        eq(leadBuffer.orgId, sourceOrgId),
        sql`brand_ids IS NOT NULL`,
        soloBrandCondition
      )
    )
    .returning({ id: leadBuffer.id });

  // Step 2: Rewrite brand (if targetBrandId present) — no org_id filter, matches all rows with sourceBrandId
  if (targetBrandId) {
    const rewriteSet = { brandIds: sql`ARRAY[${targetBrandId}]::text[]` };

    await db
      .update(servedLeads)
      .set(rewriteSet)
      .where(soloBrandCondition);

    await db
      .update(leadBuffer)
      .set(rewriteSet)
      .where(and(sql`brand_ids IS NOT NULL`, soloBrandCondition));
  }

  const updatedTables = [
    { tableName: "served_leads", count: servedStep1.length },
    { tableName: "lead_buffer", count: bufferStep1.length },
  ];

  console.log(
    `[lead-service] Transfer complete: ${JSON.stringify(updatedTables)}`
  );

  if (runId) traceEvent(runId, { service: "lead-service", event: "transfer-brand-done", detail: `updated: ${JSON.stringify(updatedTables)}`, data: { updatedTables } }, req.headers).catch(() => {});

  res.json({ updatedTables });
});

export default router;
