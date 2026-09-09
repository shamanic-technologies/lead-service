-- Derive the input set for scripts/backfill-lead-org-enrichment.ts.
--
-- apollo-service has held the rich company facts and the full career history for
-- every person it enriched since long before lead-service could carry them: the
-- neutral person human-service served was slim on the organization (name, domain,
-- website, industry, employee count, revenue, linkedin, logo, geography) and
-- carried no employment history at all, so short description, keywords,
-- technology names, industry lists, funding, founded year and every past role
-- were dropped at that hop and never reached a lead row.
--
-- Measured in production on 2026-09-09, over the last 30 days: apollo-service
-- holds a company short description for 94% of enriched people, keywords and
-- technology names for ~100%, a founded year for 79% and a latest funding stage
-- for 21%, and a full employment list for 100% — while the lead rows for the same
-- people carried 3.1% / 3.2% / 3.2% / 2.8% / 0.6% and exactly one employment row
-- each. Downstream, the company-description, keywords and tech-stack variables
-- rendered EMPTY in 87% of the 11,070 sales emails generated in that window.
--
-- This query reads apollo-service's OWN store. It is a plain SELECT against
-- `apollo_people_enrichments` — it calls no Apollo API, decrypts no key,
-- authorizes no credit, and therefore CANNOT spend a credit. That is the whole
-- reason the repair is an operator SQL step plus a local script rather than a
-- call to apollo-service `POST /enrich`, which BUYS on a cache miss (and whose
-- cache hit additionally requires a verified email newer than 12 months, so
-- everyone those predicates exclude would have been re-purchased).
--
-- It lives in SQL, against a sibling's database, for the same reason
-- apollo-lead-timezones.sql and exclude-vendor-contacted.sql do: only
-- apollo-service's own store can answer what Apollo returned for a person, and
-- lead-service must not reach into a sibling's store from its own source tree.
--
-- COALESCE(<typed column>, response_raw -> 'organization' -> ...) is deliberate,
-- following the timezone repair: apollo-service promotes a typed column only for
-- enrichments written after its own reader for that field landed, so the raw
-- payload recovers people the typed column alone would miss. (Measured
-- 2026-09-09 the two agree at 94.0% for the short description on the last 30
-- days, so today the COALESCE mostly buys older rows — which is exactly the
-- population a backfill is for.)
--
-- Output: ONE JSON object per line, one line per person, newest enrichment
-- winning — a re-enrichment can correct a company's facts, and the most recent
-- answer is the one to carry. It carries BOTH keys the script can match a lead
-- on (`apolloPersonId` and `email`) and is deliberately NOT narrowed to the leads
-- that need it: the script does the matching, so the two databases never have to
-- be joined.
--
-- Run against the apollo-service database (production is the Hetzner box:
-- `docker exec distribute-postgres-1 psql -U postgres -d apollo_service`), then
-- feed the file to the backfill:
--
--   psql -U postgres -d apollo_service -f apollo-lead-org-enrichment.sql
--   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/backfill-lead-org-enrichment.ts \
--     --input /tmp/apollo-lead-org-enrichment.jsonl --dry-run
\set ON_ERROR_STOP on

-- One JSON object per line, CSV-quoted (a single column), so a value carrying a
-- comma or a quote survives the round trip; the script un-doubles the quotes.
\copy (SELECT row_to_json(t)::text FROM (
    SELECT DISTINCT ON (coalesce(e.apollo_person_id, e.email))
           e.apollo_person_id AS "apolloPersonId",
           e.email AS "email",
           coalesce(e.organization_id, e.response_raw->'organization'->>'id') AS "providerOrganizationId",
           coalesce(e.organization_name, e.response_raw->'organization'->>'name') AS "name",
           coalesce(e.organization_domain, e.response_raw->'organization'->>'primary_domain') AS "domain",
           coalesce(e.organization_website_url, e.response_raw->'organization'->>'website_url') AS "websiteUrl",
           coalesce(e.organization_industry, e.response_raw->'organization'->>'industry') AS "industry",
           coalesce(e.organization_logo_url, e.response_raw->'organization'->>'logo_url') AS "logoUrl",
           coalesce(e.organization_linkedin_url, e.response_raw->'organization'->>'linkedin_url') AS "linkedinUrl",
           coalesce(e.organization_twitter_url, e.response_raw->'organization'->>'twitter_url') AS "twitterUrl",
           coalesce(e.organization_facebook_url, e.response_raw->'organization'->>'facebook_url') AS "facebookUrl",
           coalesce(e.organization_blog_url, e.response_raw->'organization'->>'blog_url') AS "blogUrl",
           coalesce(e.organization_crunchbase_url, e.response_raw->'organization'->>'crunchbase_url') AS "crunchbaseUrl",
           coalesce(e.organization_angellist_url, e.response_raw->'organization'->>'angellist_url') AS "angellistUrl",
           coalesce(e.organization_short_description, e.response_raw->'organization'->>'short_description') AS "shortDescription",
           coalesce(e.organization_seo_description, e.response_raw->'organization'->>'seo_description') AS "seoDescription",
           coalesce(e.organization_keywords, e.response_raw->'organization'->'keywords') AS "keywords",
           coalesce(e.organization_technology_names, e.response_raw->'organization'->'technology_names') AS "technologyNames",
           coalesce(e.organization_industries, e.response_raw->'organization'->'industries') AS "industries",
           coalesce(e.organization_secondary_industries, e.response_raw->'organization'->'secondary_industries') AS "secondaryIndustries",
           coalesce(e.organization_latest_funding_stage, e.response_raw->'organization'->>'latest_funding_stage') AS "latestFundingStage",
           coalesce(e.organization_latest_funding_round_date, e.response_raw->'organization'->>'latest_funding_round_date') AS "latestFundingRoundDate",
           coalesce(e.organization_total_funding::text, e.response_raw->'organization'->>'total_funding') AS "totalFunding",
           coalesce(e.organization_total_funding_printed, e.response_raw->'organization'->>'total_funding_printed') AS "totalFundingPrinted",
           coalesce(e.organization_funding_events, e.response_raw->'organization'->'funding_events') AS "fundingEvents",
           coalesce(e.organization_founded_year, (e.response_raw->'organization'->>'founded_year')::int) AS "foundedYear",
           coalesce(e.organization_revenue_usd::text, e.response_raw->'organization'->>'annual_revenue') AS "annualRevenue",
           coalesce(e.organization_size, e.response_raw->'organization'->>'estimated_num_employees') AS "estimatedNumEmployees",
           coalesce(e.organization_city, e.response_raw->'organization'->>'city') AS "city",
           coalesce(e.organization_state, e.response_raw->'organization'->>'state') AS "state",
           coalesce(e.organization_country, e.response_raw->'organization'->>'country') AS "country",
           coalesce(e.organization_street_address, e.response_raw->'organization'->>'street_address') AS "streetAddress",
           coalesce(e.organization_postal_code, e.response_raw->'organization'->>'postal_code') AS "postalCode",
           coalesce(e.organization_primary_phone, e.response_raw->'organization'->'primary_phone'->>'number') AS "primaryPhone",
           coalesce(e.organization_publicly_traded_symbol, e.response_raw->'organization'->>'publicly_traded_symbol') AS "publiclyTradedSymbol",
           coalesce(e.organization_publicly_traded_exchange, e.response_raw->'organization'->>'publicly_traded_exchange') AS "publiclyTradedExchange",
           coalesce(e.organization_num_suborganizations, (e.response_raw->'organization'->>'num_suborganizations')::int) AS "numSuborganizations",
           coalesce(e.organization_retail_location_count, (e.response_raw->'organization'->>'retail_location_count')::int) AS "retailLocationCount",
           coalesce(e.organization_alexa_ranking, (e.response_raw->'organization'->>'alexa_ranking')::int) AS "alexaRanking",
           coalesce(e.employment_history, e.response_raw->'employment_history') AS "employmentHistory"
    FROM apollo_people_enrichments e
    WHERE e.apollo_person_id IS NOT NULL OR e.email IS NOT NULL
    ORDER BY coalesce(e.apollo_person_id, e.email), e.created_at DESC
  ) t) TO '/tmp/apollo-lead-org-enrichment.jsonl' CSV
