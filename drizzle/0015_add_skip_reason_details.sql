-- Add skip_reason and skip_details columns to lead_buffer for observability
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "skip_reason" text;--> statement-breakpoint
ALTER TABLE "lead_buffer" ADD COLUMN IF NOT EXISTS "skip_details" text;
