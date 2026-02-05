CREATE TABLE IF NOT EXISTS "cursors" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"namespace" text NOT NULL,
	"state" jsonb,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "lead_buffer" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"namespace" text NOT NULL,
	"email" text NOT NULL,
	"external_id" text,
	"data" jsonb,
	"status" text DEFAULT 'buffered' NOT NULL,
	"push_run_id" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "organizations" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"app_id" text NOT NULL,
	"external_id" text NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "served_leads" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"namespace" text NOT NULL,
	"email" text NOT NULL,
	"external_id" text,
	"metadata" jsonb,
	"parent_run_id" text,
	"run_id" text,
	"served_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "users" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"external_id" text NOT NULL,
	"organization_id" uuid NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "cursors" ADD CONSTRAINT "cursors_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "lead_buffer" ADD CONSTRAINT "lead_buffer_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "served_leads" ADD CONSTRAINT "served_leads_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
DO $$ BEGIN
 ALTER TABLE "users" ADD CONSTRAINT "users_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;
EXCEPTION
 WHEN duplicate_object THEN null;
END $$;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_cursors_org_ns" ON "cursors" USING btree ("organization_id","namespace");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_buffer_org_ns_status" ON "lead_buffer" USING btree ("organization_id","namespace","status");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_orgs_app_external" ON "organizations" USING btree ("app_id","external_id");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_served_org_ns_email" ON "served_leads" USING btree ("organization_id","namespace","email");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_served_org" ON "served_leads" USING btree ("organization_id");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_org_external" ON "users" USING btree ("organization_id","external_id");