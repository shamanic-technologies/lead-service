ALTER TABLE "lead_buffer" RENAME COLUMN "clerk_org_id" TO "org_id";--> statement-breakpoint
ALTER TABLE "lead_buffer" RENAME COLUMN "clerk_user_id" TO "user_id";--> statement-breakpoint
ALTER TABLE "served_leads" RENAME COLUMN "clerk_org_id" TO "org_id";--> statement-breakpoint
ALTER TABLE "served_leads" RENAME COLUMN "clerk_user_id" TO "user_id";--> statement-breakpoint
DROP INDEX IF EXISTS "idx_served_clerk_org";--> statement-breakpoint
DROP INDEX IF EXISTS "idx_served_clerk_user";--> statement-breakpoint
CREATE INDEX "idx_served_org_id" ON "served_leads" USING btree ("org_id");--> statement-breakpoint
CREATE INDEX "idx_served_user_id" ON "served_leads" USING btree ("user_id");
