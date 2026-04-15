import { db, sql } from "../../src/db/index.js";
import { servedLeads, leadBuffer, cursors, idempotencyCache } from "../../src/db/schema.js";
import { eq } from "drizzle-orm";

export const TEST_API_KEY = "test-api-key";
export const TEST_ORG_ID = "test-org-uuid-integration";
export const TEST_USER_ID = "test-user-uuid-integration";
export const TEST_RUN_ID = "test-run-uuid-integration";

export async function cleanupTestData(): Promise<void> {
  await db.delete(idempotencyCache).where(eq(idempotencyCache.orgId, TEST_ORG_ID));
  await db.delete(cursors).where(eq(cursors.orgId, TEST_ORG_ID));
  await db.delete(leadBuffer).where(eq(leadBuffer.orgId, TEST_ORG_ID));
  // Delete served_leads first (FK → leads), then lead_emails, then leads
  await db.delete(servedLeads).where(eq(servedLeads.orgId, TEST_ORG_ID));
  await sql`DELETE FROM lead_emails WHERE lead_id IN (
    SELECT l.id FROM leads l
    JOIN lead_emails le ON le.lead_id = l.id
    WHERE le.email LIKE '%@example.com'
  )`;
  await sql`DELETE FROM leads WHERE id IN (
    SELECT l.id FROM leads l
    LEFT JOIN served_leads sl ON sl.lead_id = l.id
    WHERE sl.id IS NULL
  )`;
}

export async function closeDb(): Promise<void> {
  await sql.end();
}

export function getAuthHeaders(extra?: { campaignId?: string; brandId?: string; runId?: string }) {
  const headers: Record<string, string> = {
    "x-api-key": TEST_API_KEY,
    "x-org-id": TEST_ORG_ID,
    "x-user-id": TEST_USER_ID,
    "x-run-id": extra?.runId ?? TEST_RUN_ID,
  };
  if (extra?.campaignId) headers["x-campaign-id"] = extra.campaignId;
  if (extra?.brandId) headers["x-brand-id"] = extra.brandId;
  return headers;
}

/** Insert leads directly into leadBuffer for testing (replaces POST /buffer/push). */
export async function seedBuffer(params: {
  campaignId: string;
  brandId: string;
  leads: Array<{ email: string; externalId?: string; data?: unknown }>;
}): Promise<void> {
  for (const lead of params.leads) {
    await db.insert(leadBuffer).values({
      namespace: "apollo",
      campaignId: params.campaignId,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: null,
      brandIds: [params.brandId],
      orgId: TEST_ORG_ID,
      userId: null,
    });
  }
}
