-- Step 2 of the input derivation for scripts/requeue-uncontacted-serves.ts.
--
-- Step 1 (uncontacted-timezone-serves.sql, against runs-service) lists every
-- recipient whose send failed on the vendor's timezone enum. "The send failed"
-- is NOT evidence that the person was never contacted: the failure is per-run,
-- and a recipient can have been submitted to the vendor at some other time.
--
-- Only the service that submitted to the vendor knows what it actually sent, so
-- this step runs against instantly-service's own record (`instantly_campaigns`,
-- one row per campaign it created) and drops any recipient that has one. That is
-- the authoritative "was this person ever handed to the vendor" answer; it is
-- deliberately CROSS-BRAND and CROSS-ORG, i.e. the conservative direction — a
-- recipient with any vendor record at all is excluded.
--
-- The repair then applies a SECOND, independent gate in code: it asks
-- email-gateway (which owns the delivery record) for the per-brand contact
-- status of everything that survives here. Both gates fail closed.
--
-- Run against the instantly-service database, after step 1:
--   docker cp /tmp/uncontacted-timezone-serves.csv distribute-postgres-1:/tmp/
--   psql -U postgres -d instantly_service -f exclude-vendor-contacted.sql
-- Output: /tmp/uncontacted-timezone-serves-final.csv (the repair's --input).
\set ON_ERROR_STOP on

CREATE TEMP TABLE candidate (email text, campaign_id text);
\copy candidate FROM '/tmp/uncontacted-timezone-serves.csv' CSV HEADER

SELECT count(*) AS candidates FROM candidate;

SELECT c.email, ic.instantly_campaign_id, ic.delivery_status, ic.org_id, ic.brand_ids
FROM candidate c
JOIN instantly_campaigns ic ON lower(ic.lead_email) = c.email
ORDER BY c.email;

\copy (SELECT c.email, c.campaign_id FROM candidate c WHERE NOT EXISTS (SELECT 1 FROM instantly_campaigns ic WHERE lower(ic.lead_email) = c.email) ORDER BY c.email) TO '/tmp/uncontacted-timezone-serves-final.csv' CSV HEADER
