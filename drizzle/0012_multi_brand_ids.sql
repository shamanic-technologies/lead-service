-- Migrate served_leads: brand_id TEXT -> brand_ids TEXT[]
ALTER TABLE "served_leads" ADD COLUMN "brand_ids" text[];
UPDATE "served_leads" SET "brand_ids" = ARRAY["brand_id"];
ALTER TABLE "served_leads" ALTER COLUMN "brand_ids" SET NOT NULL;
ALTER TABLE "served_leads" DROP COLUMN "brand_id";

-- Drop old unique index and create new one scoped by campaign
DROP INDEX IF EXISTS "idx_served_org_brand_email";
CREATE UNIQUE INDEX "idx_served_org_campaign_email" ON "served_leads" ("org_id", "campaign_id", "email");

-- Drop old brand index and create GIN index on brand_ids
DROP INDEX IF EXISTS "idx_served_brand";
CREATE INDEX "idx_served_brand_ids" ON "served_leads" USING gin ("brand_ids");

-- Migrate lead_buffer: brand_id TEXT -> brand_ids TEXT[]
ALTER TABLE "lead_buffer" ADD COLUMN "brand_ids" text[];
UPDATE "lead_buffer" SET "brand_ids" = CASE WHEN "brand_id" IS NOT NULL THEN ARRAY["brand_id"] ELSE NULL END;
ALTER TABLE "lead_buffer" DROP COLUMN "brand_id";
