-- 0025: Conversion tracking (beta) — publishable write-tokens + conversion events.
--
-- Net-new, additive: two new tables, zero change to any existing table. Lets a
-- client's website report "person P converted (signup | meeting_booked)" and lets
-- us attribute that event back to a lead we emailed for that brand, with an honest
-- confidence tier (deterministic / strong / probabilistic / unmatched) so a weak
-- match never silently credits revenue.
--
-- brand_conversion_tokens: one publishable write-key per brand. The token is embedded
-- in a client-side pixel, so it is NOT a secret — stored plaintext. It can only write
-- conversion events for its one brand and can never read. Rotation invalidates the old
-- token (public lookup by token then finds nothing → 401).
--
-- conversion_events: one row per reported conversion, storing every identity field
-- received plus the full attribution decision for audit.
--
-- All statements are guarded so a partially-applied state is a no-op.

CREATE TABLE IF NOT EXISTS "brand_conversion_tokens" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "brand_id" text NOT NULL,
  "org_id" text NOT NULL,
  "token" text NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  "rotated_at" timestamp with time zone
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_bct_brand_id" ON "brand_conversion_tokens" ("brand_id");
CREATE UNIQUE INDEX IF NOT EXISTS "idx_bct_token" ON "brand_conversion_tokens" ("token");

CREATE TABLE IF NOT EXISTS "conversion_events" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "brand_id" text NOT NULL,
  "org_id" text NOT NULL,
  "event" text NOT NULL,
  "email" text,
  "phone" text,
  "first_name" text,
  "last_name" text,
  "company_url" text,
  "dedupe_key" text,
  "dedupe_signature" text,
  "value_cents" integer,
  "matched_lead_id" uuid,
  "match_method" text,
  "match_confidence" text NOT NULL,
  "attribution_status" text NOT NULL,
  "candidate_count" integer DEFAULT 0 NOT NULL,
  "received_at" timestamp with time zone DEFAULT now() NOT NULL,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'conversion_events_matched_lead_id_leads_id_fk'
  ) THEN
    ALTER TABLE "conversion_events"
      ADD CONSTRAINT "conversion_events_matched_lead_id_leads_id_fk"
      FOREIGN KEY ("matched_lead_id") REFERENCES "leads" ("id") ON DELETE SET NULL;
  END IF;
END $$;

-- Effective dedupe uniqueness: unique per (brand, signature) only when a signature
-- exists. Rows with no dedupe basis (null signature) always insert.
CREATE UNIQUE INDEX IF NOT EXISTS "idx_ce_brand_dedupe_signature"
  ON "conversion_events" ("brand_id", "dedupe_signature")
  WHERE "dedupe_signature" IS NOT NULL;

CREATE INDEX IF NOT EXISTS "idx_ce_brand_event" ON "conversion_events" ("brand_id", "event");
CREATE INDEX IF NOT EXISTS "idx_ce_matched_lead" ON "conversion_events" ("matched_lead_id");
