CREATE TABLE IF NOT EXISTS "idempotency_cache" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"idempotency_key" text NOT NULL,
	"organization_id" uuid NOT NULL,
	"response" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
DROP INDEX IF EXISTS "idx_served_org_ns_email";--> statement-breakpoint
ALTER TABLE "served_leads" ALTER COLUMN "brand_id" SET NOT NULL;--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "idempotency_cache" ADD CONSTRAINT "idempotency_cache_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_idempotency_key" ON "idempotency_cache" USING btree ("idempotency_key");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_served_org_brand_email" ON "served_leads" USING btree ("organization_id","brand_id","email");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_served_campaign" ON "served_leads" USING btree ("campaign_id");
