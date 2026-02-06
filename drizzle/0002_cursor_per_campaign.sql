-- Add campaign_id to lead_buffer
ALTER TABLE "lead_buffer" ADD COLUMN "campaign_id" text;--> statement-breakpoint
UPDATE "lead_buffer" SET "campaign_id" = "namespace" WHERE "campaign_id" IS NULL;--> statement-breakpoint
ALTER TABLE "lead_buffer" ALTER COLUMN "campaign_id" SET NOT NULL;--> statement-breakpoint

-- Add campaign_id to served_leads
ALTER TABLE "served_leads" ADD COLUMN "campaign_id" text;--> statement-breakpoint
UPDATE "served_leads" SET "campaign_id" = "namespace" WHERE "campaign_id" IS NULL;--> statement-breakpoint
ALTER TABLE "served_leads" ALTER COLUMN "campaign_id" SET NOT NULL;--> statement-breakpoint

-- Make brand_id NOT NULL on served_leads (backfill from namespace for existing rows)
UPDATE "served_leads" SET "brand_id" = "namespace" WHERE "brand_id" IS NULL;--> statement-breakpoint
ALTER TABLE "served_leads" ALTER COLUMN "brand_id" SET NOT NULL;--> statement-breakpoint

-- Replace dedup unique index: (orgId, namespace, email) → (orgId, brandId, email)
DROP INDEX IF EXISTS "idx_served_org_ns_email";--> statement-breakpoint
CREATE UNIQUE INDEX "idx_served_org_brand_email" ON "served_leads" USING btree ("organization_id", "brand_id", "email");--> statement-breakpoint

-- Add campaign index on served_leads
CREATE INDEX "idx_served_campaign" ON "served_leads" USING btree ("campaign_id");
