-- 0031: Step outcomes a HUMAN states about one lead, and their negative twin.
--
-- The website tracker sees roughly one conversion in ten (26 of 29 events ever received are
-- 'unmatched', typically because the person signed up with an address we never emailed), and it
-- cannot see a meeting somebody took or a deal closed on a call at all. Those facts have lived as
-- typed notes in whichever tool the person was looking at, where nothing that computes the funnel,
-- the ROI or the cost per acquisition can read them.
--
-- Two shapes, deliberately stored apart, because they are not the same kind of fact:
--
--  1. An OUTCOME ("this happened") is a conversion_events row like any other, so every consumer
--     that already counts that ledger counts it with no change. `source` is what keeps a
--     hand-stated outcome distinguishable from a tracker-reported one after the fact, and
--     `campaign_id` is what makes it attributable to the campaign it was stated on rather than
--     only to the brand. `received_at` carries WHEN the outcome happened (the caller may state a
--     past date), so the by-day series places it on the right day.
--
--  2. A NEVER ("this will not happen") is NOT an outcome, so it is deliberately absent from that
--     ledger — nothing counts it, by construction rather than by a filter somebody must remember.
--     It lives here so a consumer can tell a lead that is DEAD at a step from one still PENDING.
--
-- Guarded so a partially-applied state is a no-op.

-- Defaulting existing rows to 'tracker' is the truth: every row written before this migration came
-- from the public ingest.
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "source" text DEFAULT 'tracker' NOT NULL;
-- The campaign the statement was made on. NULL for a tracker event: a website pixel knows the
-- brand and nothing about which campaign reached the person.
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "campaign_id" text;
-- The leads_campaigns row a human named, kept so a statement can be read back for that exact row.
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "lead_campaign_id" uuid;
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "stated_by_user_id" text;
ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "note" text;

CREATE INDEX IF NOT EXISTS "idx_ce_brand_source" ON "conversion_events" ("brand_id", "source");
CREATE INDEX IF NOT EXISTS "idx_ce_lead_campaign" ON "conversion_events" ("lead_campaign_id");

CREATE TABLE IF NOT EXISTS "lead_step_disqualifications" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
  "lead_id" uuid NOT NULL REFERENCES "leads"("id") ON DELETE CASCADE,
  "lead_campaign_id" uuid NOT NULL,
  "campaign_id" text NOT NULL,
  "brand_id" text NOT NULL,
  "org_id" text NOT NULL,
  "step" text NOT NULL,
  "note" text,
  "stated_by_user_id" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);

-- One statement per person per campaign per step: restating corrects, never accumulates.
CREATE UNIQUE INDEX IF NOT EXISTS "idx_lsd_lead_campaign_step"
  ON "lead_step_disqualifications" ("lead_id", "campaign_id", "step");
-- The consumer read: who is dead at this step for this brand.
CREATE INDEX IF NOT EXISTS "idx_lsd_brand_step" ON "lead_step_disqualifications" ("brand_id", "step");
