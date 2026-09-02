-- 0036: what we owe a prospect NEXT, and when — the follow-up queue (src/lib/followup-queue.ts).
--
-- Once somebody has shown a sales interest we owe them an answer now, and, if they then go quiet,
-- further answers at increasingly spaced intervals, indefinitely, until they book, opt out, or
-- answer again. Nothing anywhere recorded that debt: a reply arrived, it was qualified, and there
-- was no record of what we owed that person or when. A worker that wants to answer the next person
-- who is due had nothing to ask.
--
-- The debt is a property of the (lead, campaign) pair, which is exactly this row, so it is four
-- columns on it rather than a second table. `status` keeps its meaning — a served row stays
-- 'served' — so the delivery overlay and the read paths' winner ordering are untouched, exactly as
-- the paid-pool retry columns (0030) left them.
--
--   followup_due_at      WHEN we next owe this person an action. NULL means we owe them nothing
--                        right now: never scheduled, already answered and not yet re-scheduled, or
--                        stopped. It is the queue's whole ordering key — oldest due first, so a
--                        backlog cannot starve the people who have waited longest.
--   followup_claimed_at  the claim lease. The conditional UPDATE that writes it is what makes two
--                        concurrent workers unable to take the same person: several replies land at
--                        the same moment and a read-then-write race would answer one prospect twice,
--                        which is the worst failure this queue can have. It EXPIRES, because a
--                        worker that dies mid-answer must not strand the person forever.
--   followup_count       how many follow-up actions we have taken toward this person. Recorded
--                        because it is worth knowing; deliberately NOT a cap — there is no ceiling
--                        on the number of follow-ups, the growing intervals are the limit, and the
--                        interval is chosen per lead by the worker (a prospect who writes
--                        "recontact me in January" must be honoured) rather than by a ladder here.
--   followup_last_action_at  when we last acted, so "how long have they been quiet" is readable.
--   followup_stopped_reason  why the schedule is currently empty, when it is. Free text stated by
--                        the caller; NULL while a due date stands.
--
-- Guarded so a partially-applied state is a no-op.
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "followup_due_at" timestamp with time zone;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "followup_claimed_at" timestamp with time zone;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "followup_count" integer DEFAULT 0 NOT NULL;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "followup_last_action_at" timestamp with time zone;
ALTER TABLE "leads_campaigns" ADD COLUMN IF NOT EXISTS "followup_stopped_reason" text;

-- The queue's only read: this campaign's due rows, oldest due first. Partial, because a campaign's
-- population is overwhelmingly rows that owe nothing — the index stays the size of the debt.
CREATE INDEX IF NOT EXISTS "idx_lc_followup_queue"
  ON "leads_campaigns" ("org_id", "campaign_id", "followup_due_at")
  WHERE "followup_due_at" IS NOT NULL;
