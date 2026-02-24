import { db, sql } from "../../src/db/index.js";
import { organizations, servedLeads, leadBuffer, cursors, idempotencyCache, leadEmails, leads } from "../../src/db/schema.js";
import { eq, and } from "drizzle-orm";

export const TEST_API_KEY = "test-api-key-12345";
export const TEST_APP_ID = "test-app";
export const TEST_ORG_ID = "test-org-integration";

let testOrgUuid: string | null = null;

export async function setupTestOrg(): Promise<string> {
  // Clean up stale test data from previous runs (by appId/externalId, not in-memory uuid)
  const existing = await db
    .select({ id: organizations.id })
    .from(organizations)
    .where(
      and(
        eq(organizations.appId, TEST_APP_ID),
        eq(organizations.externalId, TEST_ORG_ID)
      )
    );

  if (existing.length > 0) {
    testOrgUuid = existing[0].id;
    await cleanupTestData();
  }

  // Create test organization
  const [org] = await db
    .insert(organizations)
    .values({ appId: TEST_APP_ID, externalId: TEST_ORG_ID })
    .returning();

  testOrgUuid = org.id;
  return org.id;
}

export async function cleanupTestData(): Promise<void> {
  if (testOrgUuid) {
    await db.delete(idempotencyCache).where(eq(idempotencyCache.organizationId, testOrgUuid));
    await db.delete(cursors).where(eq(cursors.organizationId, testOrgUuid));
    await db.delete(leadBuffer).where(eq(leadBuffer.organizationId, testOrgUuid));
    await db.delete(servedLeads).where(eq(servedLeads.organizationId, testOrgUuid));
    // Clean up leadEmails and leads (global tables, clean all test-created rows)
    await sql`DELETE FROM lead_emails WHERE lead_id IN (SELECT id FROM leads)`;
    await sql`DELETE FROM leads`;
    await db.delete(organizations).where(eq(organizations.id, testOrgUuid));
    testOrgUuid = null;
  }
}

export async function closeDb(): Promise<void> {
  await sql.end();
}

export function getAuthHeaders() {
  return {
    "x-api-key": TEST_API_KEY,
    "x-app-id": TEST_APP_ID,
    "x-org-id": TEST_ORG_ID,
  };
}

/** Insert leads directly into leadBuffer for testing (replaces POST /buffer/push). */
export async function seedBuffer(params: {
  campaignId: string;
  brandId: string;
  leads: Array<{ email: string; externalId?: string; data?: unknown }>;
}): Promise<void> {
  if (!testOrgUuid) throw new Error("setupTestOrg() must be called first");
  for (const lead of params.leads) {
    await db.insert(leadBuffer).values({
      organizationId: testOrgUuid,
      namespace: params.campaignId,
      campaignId: params.campaignId,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: null,
      brandId: params.brandId,
      clerkOrgId: TEST_ORG_ID,
      clerkUserId: null,
    });
  }
}
