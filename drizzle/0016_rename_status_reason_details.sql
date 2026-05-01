-- Rename skip_reason/skip_details to status_reason/status_details (applies to all statuses, not just skipped)
ALTER TABLE "lead_buffer" RENAME COLUMN "skip_reason" TO "status_reason";--> statement-breakpoint
ALTER TABLE "lead_buffer" RENAME COLUMN "skip_details" TO "status_details";
