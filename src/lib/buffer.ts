import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import { leadBuffer } from "../db/schema.js";
import { isServed, markServed } from "./dedup.js";

export async function pushLeads(params: {
  organizationId: string;
  namespace: string;
  pushRunId?: string | null;
  brandId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
  leads: Array<{
    email: string;
    externalId?: string | null;
    data?: unknown;
  }>;
}): Promise<{ buffered: number; skippedAlreadyServed: number }> {
  let buffered = 0;
  let skippedAlreadyServed = 0;

  for (const lead of params.leads) {
    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      lead.email
    );

    if (alreadyServed) {
      skippedAlreadyServed++;
      continue;
    }

    await db.insert(leadBuffer).values({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: params.pushRunId ?? null,
      brandId: params.brandId ?? null,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    });
    buffered++;
  }

  return { buffered, skippedAlreadyServed };
}

export async function pullNext(params: {
  organizationId: string;
  namespace: string;
  parentRunId?: string | null;
  runId?: string | null;
}): Promise<{
  found: boolean;
  lead?: {
    email: string;
    externalId: string | null;
    data: unknown;
    brandId: string | null;
    clerkOrgId: string | null;
    clerkUserId: string | null;
  };
}> {
  while (true) {
    const row = await db.query.leadBuffer.findFirst({
      where: and(
        eq(leadBuffer.organizationId, params.organizationId),
        eq(leadBuffer.namespace, params.namespace),
        eq(leadBuffer.status, "buffered")
      ),
    });

    if (!row) {
      return { found: false };
    }

    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      row.email
    );

    if (alreadyServed) {
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    await markServed({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: row.email,
      externalId: row.externalId,
      metadata: row.data,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      brandId: row.brandId,
      clerkOrgId: row.clerkOrgId,
      clerkUserId: row.clerkUserId,
    });

    await db
      .update(leadBuffer)
      .set({ status: "served" })
      .where(eq(leadBuffer.id, row.id));

    return {
      found: true,
      lead: {
        email: row.email,
        externalId: row.externalId,
        data: row.data,
        brandId: row.brandId,
        clerkOrgId: row.clerkOrgId,
        clerkUserId: row.clerkUserId,
      },
    };
  }
}
