import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import { servedLeads } from "../db/schema.js";

export async function isServed(
  organizationId: string,
  namespace: string,
  email: string
): Promise<boolean> {
  const existing = await db.query.servedLeads.findFirst({
    where: and(
      eq(servedLeads.organizationId, organizationId),
      eq(servedLeads.namespace, namespace),
      eq(servedLeads.email, email)
    ),
  });
  return !!existing;
}

export async function markServed(params: {
  organizationId: string;
  namespace: string;
  email: string;
  externalId?: string | null;
  metadata?: unknown;
  parentRunId?: string | null;
  runId?: string | null;
  brandId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
}): Promise<{ inserted: boolean }> {
  const result = await db
    .insert(servedLeads)
    .values({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: params.email,
      externalId: params.externalId ?? null,
      metadata: params.metadata ?? null,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      brandId: params.brandId ?? null,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    })
    .onConflictDoNothing()
    .returning();

  return { inserted: result.length > 0 };
}
