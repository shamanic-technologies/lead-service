ALTER TABLE "served_leads" ADD COLUMN "workflow_name" text;--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN "workflow_name" text;
