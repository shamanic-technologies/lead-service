-- Add org_id to tables that only had organization_id FK
ALTER TABLE "idempotency_cache" ADD COLUMN "org_id" text;--> statement-breakpoint
ALTER TABLE "cursors" ADD COLUMN "org_id" text;--> statement-breakpoint

-- Backfill org_id from organizations.external_id
UPDATE "idempotency_cache" ic SET "org_id" = o."external_id"
  FROM "organizations" o WHERE ic."organization_id" = o."id";--> statement-breakpoint
UPDATE "cursors" c SET "org_id" = o."external_id"
  FROM "organizations" o WHERE c."organization_id" = o."id";--> statement-breakpoint

-- Delete orphaned rows (organization_id that doesn't exist in organizations)
DELETE FROM "idempotency_cache" WHERE "org_id" IS NULL;--> statement-breakpoint
DELETE FROM "cursors" WHERE "org_id" IS NULL;--> statement-breakpoint

-- Make org_id NOT NULL on all dependent tables
ALTER TABLE "idempotency_cache" ALTER COLUMN "org_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "cursors" ALTER COLUMN "org_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "served_leads" ALTER COLUMN "org_id" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "lead_buffer" ALTER COLUMN "org_id" SET NOT NULL;--> statement-breakpoint

-- Drop old indexes that reference organization_id
DROP INDEX IF EXISTS "idx_served_org_brand_email";--> statement-breakpoint
DROP INDEX IF EXISTS "idx_served_org";--> statement-breakpoint
DROP INDEX IF EXISTS "idx_buffer_org_ns_status";--> statement-breakpoint
DROP INDEX IF EXISTS "idx_cursors_org_ns";--> statement-breakpoint

-- Drop organization_id FK columns from all dependent tables
ALTER TABLE "served_leads" DROP COLUMN "organization_id";--> statement-breakpoint
ALTER TABLE "lead_buffer" DROP COLUMN "organization_id";--> statement-breakpoint
ALTER TABLE "idempotency_cache" DROP COLUMN "organization_id";--> statement-breakpoint
ALTER TABLE "cursors" DROP COLUMN "organization_id";--> statement-breakpoint

-- Recreate indexes using org_id
CREATE UNIQUE INDEX "idx_served_org_brand_email" ON "served_leads" USING btree ("org_id","brand_id","email");--> statement-breakpoint
CREATE INDEX "idx_buffer_org_ns_status" ON "lead_buffer" USING btree ("org_id","namespace","status");--> statement-breakpoint
CREATE UNIQUE INDEX "idx_cursors_org_ns" ON "cursors" USING btree ("org_id","namespace");--> statement-breakpoint

-- Drop org/user mapping tables (users has FK to organizations, drop first)
DROP TABLE "users";--> statement-breakpoint
DROP TABLE "organizations";
