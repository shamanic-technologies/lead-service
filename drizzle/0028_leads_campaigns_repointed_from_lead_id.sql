-- 0028: Add leads_campaigns.repointed_from_lead_id (served-lead email repair audit).
--
-- Rows served before the email-owner-first identity fix were attributed to a lead
-- that could never carry the person's email — the global one-email-one-lead index
-- (idx_lcm_channel_value) had already given that email to a DIFFERENT lead. Those
-- served rows can never resolve contacted/sent/delivered, because the read path
-- keys the email-gateway delivery overlay on the lead's REGISTERED email.
--
-- scripts/repair-served-lead-emails.ts re-points such a row's lead_id to the lead
-- that owns the email and records the previous lead_id here, so the repair is
-- traceable and reversible:
--   UPDATE leads_campaigns
--      SET lead_id = repointed_from_lead_id, repointed_from_lead_id = NULL
--    WHERE repointed_from_lead_id IS NOT NULL;
--
-- NULL on every row the repair never touched. The serve path never writes it.
-- Guarded so a partially-applied state is a no-op.
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "repointed_from_lead_id" uuid;

CREATE INDEX IF NOT EXISTS "idx_lc_repointed_from"
  ON "leads_campaigns" ("repointed_from_lead_id")
  WHERE "repointed_from_lead_id" IS NOT NULL;
