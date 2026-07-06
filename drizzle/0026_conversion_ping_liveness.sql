-- 0026: Conversion tracker liveness ping.
--
-- Net-new, additive: one new nullable column on brand_conversion_tokens, zero change
-- to any existing column or table. Lets the client's on-page tag fire a { event: "ping" }
-- heartbeat so the dashboard can DERIVE "the tracker is alive on my site" BEFORE any
-- real conversion arrives (mirrors Meta Pixel "Last Received" / Google tag "Recording").
--
-- A ping is NOT a conversion: it updates last_ping_at only, never inserts a
-- conversion_events row, never runs attribution, never counts toward eventTypesSeen.
--
-- Idempotent: guarded so a partially-applied state is a no-op.

ALTER TABLE "brand_conversion_tokens"
  ADD COLUMN IF NOT EXISTS "last_ping_at" timestamp with time zone;
