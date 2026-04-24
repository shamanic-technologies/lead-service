import { Router } from "express";
import { z } from "zod";
import { eq, and, sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { apiKeyAuth } from "../middleware/auth.js";

const router = Router();

const TransferBrandBodySchema = z.object({
  brandId: z.string().uuid(),
  sourceOrgId: z.string().uuid(),
  targetOrgId: z.string().uuid(),
});

router.post("/internal/transfer-brand", apiKeyAuth, async (req, res) => {
  const parsed = TransferBrandBodySchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.message });
    return;
  }

  const { brandId, sourceOrgId, targetOrgId } = parsed.data;

  console.log(
    `[lead-service] Transfer brand ${brandId} from org ${sourceOrgId} to org ${targetOrgId}`
  );

  // Solo-brand condition: brand_ids array has exactly one element and it equals brandId
  const soloBrandCondition = sql`array_length(brand_ids, 1) = 1 AND brand_ids[1] = ${brandId}`;

  // Update served_leads
  const servedResult = await db
    .update(servedLeads)
    .set({ orgId: targetOrgId })
    .where(
      and(eq(servedLeads.orgId, sourceOrgId), soloBrandCondition)
    )
    .returning({ id: servedLeads.id });

  // Update lead_buffer (brand_ids is nullable, so also check it's not null)
  const bufferResult = await db
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

  const updatedTables = [
    { tableName: "served_leads", count: servedResult.length },
    { tableName: "lead_buffer", count: bufferResult.length },
  ];

  console.log(
    `[lead-service] Transfer complete: ${JSON.stringify(updatedTables)}`
  );

  res.json({ updatedTables });
});

export default router;
