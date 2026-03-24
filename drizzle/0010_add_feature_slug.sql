ALTER TABLE "served_leads" ADD COLUMN IF NOT EXISTS "feature_slug" text;--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "feature_slug" text;
