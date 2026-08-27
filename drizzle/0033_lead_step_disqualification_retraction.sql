-- 0033: a "never" is RETRACTED, never deleted.
--
-- A funnel is ORDERED: stating that a lead PAID says it also got through the steps that lead to
-- paying, so a "never" standing on any of those steps is contradicted by the fact. The same-step
-- version of that rule already shipped, and it resolved the contradiction by DELETING the
-- disqualification row — which destroys the record of what a person actually stated, and the record
-- of who said what, and when, is the thing that makes any of this auditable.
--
-- So a contradicted "never" is now marked retracted and kept. Every read filters `retracted_at IS
-- NULL`, so nothing counts it and nothing shows it as live; the row survives so the history can be
-- read back. Restating the same "never" clears the mark (the person changed their mind again).
--
-- `retracted_by_step` records WHICH outcome retracted it — the same step for the same-step rule,
-- an EARLIER step's row retracted by a LATER outcome for the funnel-order rule.
--
-- Idempotent: every statement guards itself, so a partially-applied state is a no-op.

ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "retracted_at" timestamp with time zone;
ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "retracted_by_step" text;
ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "retracted_by_user_id" text;

-- The live-statement read: who is dead at this step for this brand, retracted rows excluded.
CREATE INDEX IF NOT EXISTS "idx_lsd_brand_step_live"
  ON "lead_step_disqualifications" ("brand_id", "step")
  WHERE "retracted_at" IS NULL;
