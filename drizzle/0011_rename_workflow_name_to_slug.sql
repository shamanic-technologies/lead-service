ALTER TABLE "served_leads" RENAME COLUMN "workflow_name" TO "workflow_slug";--> statement-breakpoint
ALTER TABLE "lead_buffer" RENAME COLUMN "workflow_name" TO "workflow_slug";
