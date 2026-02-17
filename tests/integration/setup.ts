import { db, sql } from "../../src/db/index.js";
import { organizations, servedLeads, leadBuffer, cursors, idempotencyCache } from "../../src/db/schema.js";
import { eq } from "drizzle-orm";

export const TEST_API_KEY = "test-api-key-12345";
export const TEST_APP_ID = "test-app";
export const TEST_ORG_ID = "test-org-integration";

let testOrgUuid: string | null = null;

export async function setupTestOrg(): Promise<string> {
  // Clean up any existing test org
  await cleanupTestData();

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
