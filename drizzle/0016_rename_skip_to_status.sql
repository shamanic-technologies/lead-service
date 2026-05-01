-- Rename skip_reason/skip_details to status_reason/status_details (applies to all status transitions, not just skips)
-- Idempotent: only renames if the old column still exists
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'lead_buffer' AND column_name = 'skip_reason') THEN
    ALTER TABLE "lead_buffer" RENAME COLUMN "skip_reason" TO "status_reason";
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'lead_buffer' AND column_name = 'skip_details') THEN
    ALTER TABLE "lead_buffer" RENAME COLUMN "skip_details" TO "status_details";
  END IF;
END $$;
