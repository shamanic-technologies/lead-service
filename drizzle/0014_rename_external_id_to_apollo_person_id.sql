-- Rename external_id → apollo_person_id in all tables (idempotent)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'leads' AND column_name = 'external_id') THEN
    ALTER TABLE "leads" RENAME COLUMN "external_id" TO "apollo_person_id";
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'served_leads' AND column_name = 'external_id') THEN
    ALTER TABLE "served_leads" RENAME COLUMN "external_id" TO "apollo_person_id";
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'lead_buffer' AND column_name = 'external_id') THEN
    ALTER TABLE "lead_buffer" RENAME COLUMN "external_id" TO "apollo_person_id";
  END IF;
END $$;--> statement-breakpoint
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'enrichments' AND column_name = 'external_id') THEN
    ALTER TABLE "enrichments" RENAME COLUMN "external_id" TO "apollo_person_id";
  END IF;
END $$;--> statement-breakpoint

-- Add email_status column to enrichments (nullable, no default)
ALTER TABLE "enrichments" ADD COLUMN IF NOT EXISTS "email_status" text;--> statement-breakpoint

-- Rename indexes to match new column name
ALTER INDEX IF EXISTS "idx_leads_external_id" RENAME TO "idx_leads_apollo_person_id";--> statement-breakpoint
ALTER INDEX IF EXISTS "idx_enrichments_external_id" RENAME TO "idx_enrichments_apollo_person_id";--> statement-breakpoint

-- Recreate buffer index with new column name (idempotent)
DROP INDEX IF EXISTS "idx_buffer_org_campaign_extid";--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_buffer_org_campaign_extid" ON "lead_buffer" USING btree ("org_id","campaign_id","apollo_person_id");
