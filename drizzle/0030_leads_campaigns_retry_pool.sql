-- 0030: Paid-pool retry state on leads_campaigns (src/lib/retry-pool.ts).
--
-- A person served by this service has already been paid for (apollo/apify enrichment)
-- and is suppressed for three months in human-service the moment the serve happens. So
-- a failure AFTER the serve — email generation, the vendor push, anything — strands a
-- prospect the brand can no longer reach: 133 of them on one campaign on 2026-08-25.
-- The pool re-serves those people before buying new ones. These three columns are its
-- state; `status` keeps its meaning (a served row stays 'served', so the delivery
-- overlay and the read paths' winner ordering are untouched).
--
--   sent_at          terminal — an email went out, the row leaves the pool for good and
--                    is never re-queried against email-gateway. This is what keeps the
--                    per-pull gateway call small whatever the campaign's age.
--   retry_claimed_at the claim lease. The conditional UPDATE that writes it is what
--                    stops two concurrent pulls handing the same person to two runs,
--                    and it doubles as the queue position so a repeatedly-failing
--                    person sorts to the back instead of blocking the campaign's head.
--   retry_count      how many times this paid serve has been handed out again.
--
-- Guarded so a partially-applied state is a no-op.
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "sent_at" timestamp with time zone;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "retry_claimed_at" timestamp with time zone;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "retry_count" integer DEFAULT 0 NOT NULL;

-- The pool's only read: this campaign's non-terminal serves, oldest first.
CREATE INDEX IF NOT EXISTS "idx_lc_retry_pool"
  ON "leads_campaigns" ("org_id", "campaign_id", "retry_claimed_at")
  WHERE "status" = 'served' AND "sent_at" IS NULL;
