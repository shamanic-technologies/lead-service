import { db, sql } from "../../src/db/index.js";
import { servedLeads, leadBuffer, cursors, idempotencyCache } from "../../src/db/schema.js";
import { eq } from "drizzle-orm";

export const TEST_API_KEY = "test-api-key-12345";
export const TEST_ORG_ID = "test-org-uuid-integration";
export const TEST_USER_ID = "test-user-uuid-integration";
export const TEST_RUN_ID = "test-run-uuid-integration";

export async function cleanupTestData(): Promise<void> {
  await db.delete(idempotencyCache).where(eq(idempotencyCache.orgId, TEST_ORG_ID));
  await db.delete(cursors).where(eq(cursors.orgId, TEST_ORG_ID));
  await db.delete(leadBuffer).where(eq(leadBuffer.orgId, TEST_ORG_ID));
  await db.delete(servedLeads).where(eq(servedLeads.orgId, TEST_ORG_ID));
  // Clean up global tables in FK-safe order: lead_emails → leads
  await sql`DELETE FROM lead_emails WHERE lead_id IN (SELECT id FROM leads)`;
  await sql`DELETE FROM leads`;
}

export async function closeDb(): Promise<void> {
  await sql.end();
}

export function getAuthHeaders() {
  return {
    "x-api-key": TEST_API_KEY,
    "x-org-id": TEST_ORG_ID,
    "x-user-id": TEST_USER_ID,
    "x-run-id": TEST_RUN_ID,
  };
}

/** Insert leads directly into leadBuffer for testing (replaces POST /buffer/push). */
export async function seedBuffer(params: {
  campaignId: string;
  brandId: string;
  leads: Array<{ email: string; externalId?: string; data?: unknown }>;
}): Promise<void> {
  for (const lead of params.leads) {
    await db.insert(leadBuffer).values({
      namespace: params.campaignId,
      campaignId: params.campaignId,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: null,
      brandId: params.brandId,
      orgId: TEST_ORG_ID,
      userId: null,
    });
  }
}
