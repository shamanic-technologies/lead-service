-- 0027: Rename the terminal "customer paid" conversion event from "purchase" to "sale".
--
-- distribute renamed the terminal conversion signal (the "a customer paid" event, which
-- carries a revenue value) from "purchase" to "sale". Ingest now accepts BOTH the canonical
-- "sale" spelling AND the legacy "purchase" spelling (normalized to "sale" at write), so
-- already-configured client integrations keep firing. This migration folds any historical
-- rows still stored under "purchase" to the canonical "sale" so every read path is canonical.
--
-- Idempotent: a re-run is a no-op once no "purchase" rows remain. Reversible: the inverse is
-- UPDATE ... SET event = 'purchase' WHERE event = 'sale'. dedupe_signature values on the
-- folded rows are left verbatim (opaque + still unique); only the event label changes.

UPDATE "conversion_events" SET "event" = 'sale' WHERE "event" = 'purchase';
