-- 0029: requeued_serves — audit + undo ledger for the one-time recovery of leads
-- that were served, paid for, and then never contacted.
--
-- Between 2026-08-07 01:00 UTC and 2026-08-08, instantly-service failed at
-- campaign creation for any lead carrying a real IANA timezone (the vendor 400s
-- any value outside its closed enum). The failure landed AFTER lead-service had
-- served the lead and after its email had been generated and paid for, so from
-- here it looks like a completed serve: leads_campaigns carries status='served'
-- and idx_lc_lead_campaign then makes a fresh lifecycle row for the same
-- (lead, campaign) impossible — the serve path's ON CONFLICT DO NOTHING would
-- silently keep the stale row, with the old run ids and served_at, if the person
-- were ever re-served to the campaign that lost them.
--
-- scripts/requeue-uncontacted-serves.ts therefore ARCHIVES the whole
-- leads_campaigns row here (verbatim, as jsonb) and DELETES it, so the person is
-- un-served for that campaign and a genuine re-serve records cleanly.
--
-- Reversal (restores every archived row exactly as it was, then empties the
-- ledger for that reason):
--   INSERT INTO leads_campaigns
--   SELECT * FROM jsonb_populate_record(NULL::leads_campaigns, row_snapshot)
--     FROM requeued_serves
--    WHERE reason = 'instantly-timezone-send-failure'
--   ON CONFLICT DO NOTHING;
--   DELETE FROM requeued_serves WHERE reason = 'instantly-timezone-send-failure';
--
-- The serve path never writes this table; it exists only for the repair's audit
-- trail. Guarded so a partially-applied state is a no-op.
CREATE TABLE IF NOT EXISTS "requeued_serves" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "lead_campaign_id" uuid NOT NULL,
  "lead_id" uuid NOT NULL,
  "campaign_id" text NOT NULL,
  "org_id" text NOT NULL,
  "brand_ids" text[] NOT NULL,
  "email" text NOT NULL,
  "reason" text NOT NULL,
  "row_snapshot" jsonb NOT NULL,
  "requeued_at" timestamp with time zone NOT NULL DEFAULT now()
);

-- One archive row per (person, campaign, reason): re-running the repair can
-- never double-archive, and the ledger is the authoritative "what did we undo".
CREATE UNIQUE INDEX IF NOT EXISTS "idx_requeued_serves_lead_campaign_reason"
  ON "requeued_serves" ("lead_id", "campaign_id", "reason");

CREATE INDEX IF NOT EXISTS "idx_requeued_serves_reason" ON "requeued_serves" ("reason");
CREATE INDEX IF NOT EXISTS "idx_requeued_serves_brand_ids"
  ON "requeued_serves" USING gin ("brand_ids");
