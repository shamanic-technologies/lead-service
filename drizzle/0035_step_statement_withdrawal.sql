-- 0035: a statement a PERSON made can be TAKEN BACK.
--
-- Somebody states a step by hand on the wrong lead, on the wrong step, or off a reply they
-- misread. Until now the only correction on offer was stating the opposite thing — which is itself
-- a false statement, and one that keeps counting: the outcome stays on the ledger and the cost the
-- customer stated for it stays in their spend, so every cost of acquisition and every return
-- downstream carries money nobody spent for an outcome nobody had.
--
-- So a statement carries a WITHDRAWAL. It is not a third kind of statement — nothing new to count,
-- nothing for a consumer to learn — it is the absence of one: every read that already filters
-- retracted statements filters withdrawn ones the same way, and the step falls back to whatever the
-- remaining statements imply, computed on read exactly as before.
--
-- NOTHING IS DELETED. What somebody actually stated, and the fact that they later withdrew it, both
-- stay readable: this is a correction, not an erasure — the same posture retraction already takes.
-- The two are different facts and are stored apart: `retracted_at` means an outcome superseded the
-- statement (the person changed their mind and bought), `withdrawn_at` means the person says they
-- never should have stated it at all.
--
-- Restating a withdrawn statement clears the mark — the write path's existing upsert is the same
-- statement, made again — so withdrawal never bricks a lead row.
--
-- Idempotent: a partially-applied state is a no-op.

ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "withdrawn_at" timestamp with time zone;
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "withdrawn_by_user_id" text;

ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "withdrawn_at" timestamp with time zone;
ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "withdrawn_by_user_id" text;

-- Every outcome read is "live rows for this brand", so that is what the index answers.
CREATE INDEX IF NOT EXISTS "idx_ce_brand_event_live"
  ON "conversion_events" ("brand_id", "event")
  WHERE "withdrawn_at" IS NULL;

CREATE INDEX IF NOT EXISTS "idx_lsd_brand_step_unwithdrawn"
  ON "lead_step_disqualifications" ("brand_id", "step")
  WHERE "retracted_at" IS NULL AND "withdrawn_at" IS NULL;
