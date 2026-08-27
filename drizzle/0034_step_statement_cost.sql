-- 0034: what the CUSTOMER spent getting a lead through a funnel step.
--
-- The platform automates the first link of a sales chain and the customer performs the rest: they
-- run the meeting, they close the deal. So they are the only one who knows what that leg cost, and
-- without it a chain's cost of acquisition counts only the leg we billed for — every return we
-- display for that chain is too good.
--
-- Stating it is MANDATORY at the API, and the author chooses what goes in: zero, their time valued
-- however they like, real expenses. A stated ZERO is a real answer and reads as one, which is why
-- the column is a nullable integer rather than a NOT NULL DEFAULT 0 — NULL means "nobody was ever
-- asked" (every statement made before this shipped), 0 means "somebody answered zero". Defaulting
-- the old rows to 0 would fabricate an answer nobody gave, and a consumer could never tell the two
-- apart again.
--
-- This money is the CUSTOMER'S. It is never charged to them and it never enters the platform's own
-- spend ledger: no runs-service cost is declared for it, no billing authorize is called. It is
-- recorded because they told us, and it is read by whoever computes a cost of acquisition.
--
-- Idempotent: a partially-applied state is a no-op.

ALTER TABLE "conversion_events" ADD COLUMN IF NOT EXISTS "cost_cents" integer;
ALTER TABLE "lead_step_disqualifications" ADD COLUMN IF NOT EXISTS "cost_cents" integer;
