CREATE TABLE IF NOT EXISTS "enrichments" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"email" text NOT NULL,
	"apollo_person_id" text,
	"first_name" text,
	"last_name" text,
	"title" text,
	"linkedin_url" text,
	"organization_name" text,
	"organization_domain" text,
	"organization_industry" text,
	"organization_size" text,
	"response_raw" jsonb,
	"enriched_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "enrichments_email_unique" UNIQUE("email")
);
--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "brand_id" text;--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "clerk_org_id" text;--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "clerk_user_id" text;--> statement-breakpoint
ALTER TABLE "served_leads" ADD COLUMN IF NOT EXISTS "brand_id" text;--> statement-breakpoint
ALTER TABLE "served_leads" ADD COLUMN IF NOT EXISTS "clerk_org_id" text;--> statement-breakpoint
ALTER TABLE "served_leads" ADD COLUMN IF NOT EXISTS "clerk_user_id" text;--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_enrichments_email" ON "enrichments" USING btree ("email");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_served_brand" ON "served_leads" USING btree ("brand_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_served_clerk_org" ON "served_leads" USING btree ("clerk_org_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_served_clerk_user" ON "served_leads" USING btree ("clerk_user_id");