import {
  pgTable,
  uuid,
  text,
  timestamp,
  date,
  integer,
  numeric,
  boolean,
  uniqueIndex,
  index,
  jsonb,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

// --- Leads — global identity registry ---
export const leads = pgTable(
  "leads",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    apolloPersonId: text("apollo_person_id"),
    firstName: text("first_name"),
    lastName: text("last_name"),
    name: text("name"),
    linkedinUrl: text("linkedin_url"),
    photoUrl: text("photo_url"),
    headline: text("headline"),
    city: text("city"),
    state: text("state"),
    country: text("country"),
    seniority: text("seniority"),
    // Recipient's IANA timezone (e.g. "America/New_York"), sourced from upstream
    // (human-service / apollo-service) off the person's location. Forwarded on the
    // canonical lead so downstream send paths (email-gateway → instantly-service)
    // can schedule cold email in the recipient's local business hours. Null when
    // upstream provides none — downstream falls back to a safe default.
    timezone: text("timezone"),
    // Language(s) this person plausibly conducts business in, ISO 639-1 lowercase
    // codes, ORDERED most-plausible-first. Derived and owned by human-service (see
    // its `businessLanguages`); lead-service only carries it. A Postgres array is
    // used precisely because it preserves order — the consumer selects by position,
    // so a set or a re-sorted list would silently break it. Empty array means the
    // producer had no usable signal; that is distinct from ["en"] (= known English),
    // and NULL means the lead predates this being carried. Never derived here.
    businessLanguages: text("business_languages").array(),
    departments: text("departments").array(),
    subdepartments: text("subdepartments").array(),
    functions: text("functions").array(),
    twitterUrl: text("twitter_url"),
    githubUrl: text("github_url"),
    facebookUrl: text("facebook_url"),
    metadata: jsonb("metadata"),
    enrichedAt: timestamp("enriched_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [uniqueIndex("idx_leads_apollo_person_id").on(table.apolloPersonId)],
);

// --- Lead contact methods — polymorphic (email, phone, twitter, etc.) ---
export const leadContactMethods = pgTable(
  "lead_contact_methods",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    leadId: uuid("lead_id")
      .notNull()
      .references(() => leads.id, { onDelete: "cascade" }),
    channel: text("channel").notNull(),
    value: text("value").notNull(),
    status: text("status"),
    source: text("source").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_lcm_lead_channel_value").on(table.leadId, table.channel, table.value),
    uniqueIndex("idx_lcm_channel_value").on(table.channel, table.value),
    index("idx_lcm_value").on(table.value),
  ],
);

// --- Organizations — global org registry ---
export const organizations = pgTable(
  "organizations",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    apolloOrganizationId: text("apollo_organization_id"),
    name: text("name"),
    primaryDomain: text("primary_domain"),
    websiteUrl: text("website_url"),
    industry: text("industry"),
    estimatedNumEmployees: integer("estimated_num_employees"),
    annualRevenue: numeric("annual_revenue"),
    logoUrl: text("logo_url"),
    shortDescription: text("short_description"),
    linkedinUrl: text("linkedin_url"),
    twitterUrl: text("twitter_url"),
    facebookUrl: text("facebook_url"),
    blogUrl: text("blog_url"),
    crunchbaseUrl: text("crunchbase_url"),
    foundedYear: integer("founded_year"),
    city: text("city"),
    state: text("state"),
    country: text("country"),
    streetAddress: text("street_address"),
    postalCode: text("postal_code"),
    technologyNames: text("technology_names").array(),
    industries: text("industries").array(),
    secondaryIndustries: text("secondary_industries").array(),
    latestFundingStage: text("latest_funding_stage"),
    latestFundingRoundDate: date("latest_funding_round_date"),
    totalFunding: numeric("total_funding"),
    totalFundingPrinted: text("total_funding_printed"),
    fundingEvents: jsonb("funding_events"),
    retailLocationCount: integer("retail_location_count"),
    publiclyTradedSymbol: text("publicly_traded_symbol"),
    publiclyTradedExchange: text("publicly_traded_exchange"),
    primaryPhone: text("primary_phone"),
    seoDescription: text("seo_description"),
    angellistUrl: text("angellist_url"),
    numSuborganizations: integer("num_suborganizations"),
    alexaRanking: integer("alexa_ranking"),
    keywords: text("keywords").array(),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_organizations_apollo_organization_id").on(table.apolloOrganizationId),
  ],
);

// --- Lead employment history (M:N leads <-> organizations) ---
export const leadsOrganizations = pgTable(
  "leads_organizations",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    leadId: uuid("lead_id")
      .notNull()
      .references(() => leads.id, { onDelete: "cascade" }),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    title: text("title"),
    startDate: date("start_date"),
    endDate: date("end_date"),
    current: boolean("current").notNull().default(false),
    description: text("description"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_lo_lead_org_start").on(table.leadId, table.organizationId, table.startDate),
    index("idx_lo_lead_current").on(table.leadId, table.current),
  ],
);

// --- Leads ↔ campaigns: per-campaign lifecycle ---
export const leadsCampaigns = pgTable(
  "leads_campaigns",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    leadId: uuid("lead_id")
      .notNull()
      .references(() => leads.id),
    campaignId: text("campaign_id").notNull(),
    orgId: text("org_id").notNull(),
    brandIds: text("brand_ids").array().notNull(),
    status: text("status").notNull().default("buffered"),
    statusReason: text("status_reason"),
    statusDetails: text("status_details"),
    pushRunId: text("push_run_id"),
    parentRunId: text("parent_run_id"),
    runId: text("run_id"),
    userId: text("user_id"),
    workflowSlug: text("workflow_slug"),
    featureSlug: text("feature_slug"),
    goal: text("goal"),
    activeGoalId: text("active_goal_id"),
    brandProfileId: text("brand_profile_id"),
    audienceId: text("audience_id"),
    // Audit trail for the one-time served-lead email repair
    // (scripts/repair-served-lead-emails.ts). Rows served before the
    // email-owner-first identity fix were attributed to a lead that could never
    // carry the person's email (another lead already owned it), so their
    // delivery status was unresolvable forever. The repair re-points lead_id to
    // the owning lead and records the PREVIOUS lead_id here. NULL on every row
    // the repair never touched; the serve path never writes it.
    repointedFromLeadId: uuid("repointed_from_lead_id"),
    // --- Paid-pool retry (src/lib/retry-pool.ts) ---
    // A serve is paid for and suppressed for three months the moment it happens, so a
    // downstream failure after it strands a prospect the brand can no longer reach. The
    // pool re-serves those people before buying new ones; these three columns are its
    // state, and none of them changes what `status` means (a served row stays 'served',
    // so the delivery overlay and the read paths' winner ordering are untouched).
    //
    // sent_at          — terminal. An email went out; this row leaves the pool for good
    //                    and is never re-queried against email-gateway.
    // retry_claimed_at — the claim lease. Written by the conditional UPDATE that makes
    //                    two concurrent pulls unable to take the same person, and read
    //                    back as the queue position so a repeatedly-failing person moves
    //                    to the back instead of blocking the head.
    // retry_count      — how many times this paid serve has been handed out again.
    sentAt: timestamp("sent_at", { withTimezone: true }),
    retryClaimedAt: timestamp("retry_claimed_at", { withTimezone: true }),
    retryCount: integer("retry_count").notNull().default(0),
    servedAt: timestamp("served_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_lc_lead_campaign").on(table.leadId, table.campaignId),
    index("idx_lc_org_campaign_status").on(table.orgId, table.campaignId, table.status),
    index("idx_lc_brand_ids").using("gin", table.brandIds),
    index("idx_lc_org").on(table.orgId),
    index("idx_lc_campaign").on(table.campaignId),
    index("idx_lc_user").on(table.userId),
    // The retry pool's only read: this campaign's non-terminal serves, oldest first.
    index("idx_lc_retry_pool").on(table.orgId, table.campaignId, table.retryClaimedAt),
    index("idx_lc_persona_attribution").on(
      table.orgId,
      table.featureSlug,
      table.goal,
      table.activeGoalId,
      table.brandProfileId,
      table.audienceId,
      table.status,
    ),
  ],
);

// --- Apollo strategies per campaign (multi-strategy cursor) ---
export const campaignsApolloStrategies = pgTable(
  "campaigns_apollo_strategies",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    orgId: text("org_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    strategies: jsonb("strategies").notNull().default(sql`'[]'::jsonb`),
    currentIndex: integer("current_index").notNull().default(0),
    // apify pagination is client-managed; persist the offset for the current
    // strategy so it survives across buffer/next calls. apollo ignores this
    // (its cursor is server-managed by the gateway, keyed on org + campaign).
    apifyOffset: integer("apify_offset").notNull().default(0),
    exhausted: boolean("exhausted").notNull().default(false),
    exhaustionReason: text("exhaustion_reason"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_cas_org_campaign").on(table.orgId, table.campaignId),
  ],
);

// --- Idempotency cache (kept) ---
export const idempotencyCache = pgTable(
  "idempotency_cache",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    idempotencyKey: text("idempotency_key").notNull(),
    orgId: text("org_id").notNull(),
    response: jsonb("response").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [uniqueIndex("idx_idempotency_key").on(table.idempotencyKey)],
);

// --- Conversion tracking (beta) ---
// One publishable write-key per brand. The token is embedded in a client-side
// JS pixel on the brand's own website, so it is NOT a secret — stored plaintext,
// returned in full. It can only write conversion events for its one brand; it can
// never read anything. Rotation is the abuse remedy (old token → 401).
export const brandConversionTokens = pgTable(
  "brand_conversion_tokens",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    brandId: text("brand_id").notNull(),
    orgId: text("org_id").notNull(),
    token: text("token").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    rotatedAt: timestamp("rotated_at", { withTimezone: true }),
    // Liveness heartbeat: last time the client's on-page tag fired a { event: "ping" }.
    // Derives the "tracker is alive" signal BEFORE any real conversion arrives. A ping
    // is NOT a conversion — it never lands in conversion_events, never runs attribution.
    lastPingAt: timestamp("last_ping_at", { withTimezone: true }),
  },
  (table) => [
    uniqueIndex("idx_bct_brand_id").on(table.brandId),
    uniqueIndex("idx_bct_token").on(table.token),
  ],
);

// A conversion event reported by a client's website, plus the attribution result.
// Every row is fail-loud provenance: it stores every identity field received and the
// full match decision (method, confidence, status, candidateCount) so a reviewer can
// audit exactly why a conversion was (or was not) credited to a lead.
export const conversionEvents = pgTable(
  "conversion_events",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    brandId: text("brand_id").notNull(),
    orgId: text("org_id").notNull(),
    event: text("event").notNull(), // canonical: "signup" | "meeting_booked" | "form_submission" | "sale" (legacy "purchase" normalized to "sale" at write)
    email: text("email"),
    phone: text("phone"),
    firstName: text("first_name"),
    lastName: text("last_name"),
    companyUrl: text("company_url"),
    dedupeKey: text("dedupe_key"), // client-provided key, verbatim (provenance)
    // Effective uniqueness signature. Null when there is no dedupe basis
    // (no dedupeKey and no email/phone) — such rows always insert. The unique
    // index is partial (WHERE dedupe_signature IS NOT NULL).
    dedupeSignature: text("dedupe_signature"),
    valueCents: integer("value_cents"),
    // What the CUSTOMER spent getting the lead through this step — their money, never ours. The
    // platform automates the first leg of a sales funnel and the customer performs the rest (they
    // run the meeting, they close the deal), so they are the only one who can state what that leg
    // cost. Stating it is mandatory at the API, so a hand-stated outcome always carries one.
    // NULL means nobody was ever asked (every row written before this shipped, and every
    // tracker-reported event, which observes a page load and knows nothing about spend); 0 means
    // somebody answered zero. The two are deliberately distinguishable. This NEVER enters the
    // platform's cost ledger: no runs-service cost is declared for it and nothing is billed.
    costCents: integer("cost_cents"),
    matchedLeadId: uuid("matched_lead_id").references(() => leads.id, {
      onDelete: "set null",
    }),
    // "email" | "phone" | "domain_name" | "full_name" | "last_name" | null
    matchMethod: text("match_method"),
    // "deterministic" | "strong" | "probabilistic" | "unmatched"
    matchConfidence: text("match_confidence").notNull(),
    // "attributed" | "needs_review" | "unmatched"
    attributionStatus: text("attribution_status").notNull(),
    candidateCount: integer("candidate_count").notNull().default(0),
    // WHEN the outcome happened. The tracker stamps the moment it received the event; a human
    // stating a past fact supplies the date, so the by-day series places it on the right day.
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull().defaultNow(),
    // "tracker" (reported by the client's website) | "manual" (stated by a human about a lead we
    // already know by id). Frozen at write; what makes the two distinguishable after the fact
    // WITHOUT changing what either counts toward — every count reads both.
    source: text("source").notNull().default("tracker"),
    // The campaign a hand-stated outcome was stated on. NULL for a tracker event: a website pixel
    // knows the brand and nothing about which campaign reached the person.
    campaignId: text("campaign_id"),
    // The leads_campaigns row a human named (the id a list row already carries).
    leadCampaignId: uuid("lead_campaign_id"),
    statedByUserId: text("stated_by_user_id"),
    note: text("note"),
  },
  (table) => [
    uniqueIndex("idx_ce_brand_dedupe_signature")
      .on(table.brandId, table.dedupeSignature)
      .where(sql`dedupe_signature IS NOT NULL`),
    index("idx_ce_brand_event").on(table.brandId, table.event),
    index("idx_ce_matched_lead").on(table.matchedLeadId),
    index("idx_ce_brand_source").on(table.brandId, table.source),
    index("idx_ce_lead_campaign").on(table.leadCampaignId),
  ],
);

// --- "This will never happen": a step a lead is DEAD at ---
//
// The negative twin of a conversion event, and deliberately NOT one: a "won't book" / "won't
// attend" / "won't buy" is not an outcome and nothing counts it, which is enforced by it living
// outside conversion_events entirely rather than by a filter every consumer must remember. Its
// only job is to let a reader tell a lead that is dead at a step from one still pending — the
// difference between a cost-per-acquisition denominator that is still waiting and one that never
// will be. One row per (person, campaign, step): restating corrects, never accumulates.
export const leadStepDisqualifications = pgTable(
  "lead_step_disqualifications",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    leadId: uuid("lead_id")
      .notNull()
      .references(() => leads.id, { onDelete: "cascade" }),
    /** id of the leads_campaigns row the statement was made on */
    leadCampaignId: uuid("lead_campaign_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    brandId: text("brand_id").notNull(),
    orgId: text("org_id").notNull(),
    /** one of LEAD_STEP_OUTCOMES — the step this person will never reach */
    step: text("step").notNull(),
    /**
     * What the CUSTOMER spent on this leg before concluding it will never complete. A dead leg
     * still costs — the meeting was run, the call was taken — and a cost of acquisition that
     * ignores it is too good. Same three states as on a conversion event: NULL = nobody was ever
     * asked (rows written before this shipped), 0 = somebody answered zero. Never billed, never
     * declared to the platform's cost ledger.
     */
    costCents: integer("cost_cents"),
    note: text("note"),
    statedByUserId: text("stated_by_user_id"),
    /**
     * A "never" contradicted by an outcome is RETRACTED, never deleted: the record of what a
     * person stated is what makes this auditable, so every read filters `retracted_at IS NULL`
     * and the row survives. `retracted_by_step` is the outcome that retracted it — the same step
     * for the same-step rule, a LATER step of the funnel for the funnel rule.
     */
    retractedAt: timestamp("retracted_at", { withTimezone: true }),
    retractedByStep: text("retracted_by_step"),
    retractedByUserId: text("retracted_by_user_id"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_lsd_lead_campaign_step").on(table.leadId, table.campaignId, table.step),
    index("idx_lsd_brand_step").on(table.brandId, table.step),
  ],
);

// --- Requeued serves: audit + undo ledger for the one-time uncontacted-serve recovery ---
//
// A serve that was paid for and then never reached the vendor leaves a
// status='served' lifecycle row that idx_lc_lead_campaign makes permanent: the
// serve path's ON CONFLICT DO NOTHING would keep the stale row (old run ids, old
// served_at) if the person were re-served to the campaign that lost them. The
// recovery (scripts/requeue-uncontacted-serves.ts) archives the whole
// leads_campaigns row here verbatim and deletes it, so the person is un-served
// for that campaign and a genuine re-serve records cleanly. The serve path never
// writes this table; it exists only so the repair is traceable and reversible.
export const requeuedServes = pgTable(
  "requeued_serves",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    /** id of the leads_campaigns row that was deleted */
    leadCampaignId: uuid("lead_campaign_id").notNull(),
    leadId: uuid("lead_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    orgId: text("org_id").notNull(),
    brandIds: text("brand_ids").array().notNull(),
    /** the registered email the no-contact decision was made on */
    email: text("email").notNull(),
    /** which recovery produced this row (see REQUEUE_REASON in the script) */
    reason: text("reason").notNull(),
    /** the deleted leads_campaigns row, verbatim — the undo source */
    rowSnapshot: jsonb("row_snapshot").notNull(),
    requeuedAt: timestamp("requeued_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_requeued_serves_lead_campaign_reason").on(
      table.leadId,
      table.campaignId,
      table.reason,
    ),
    index("idx_requeued_serves_reason").on(table.reason),
    index("idx_requeued_serves_brand_ids").using("gin", table.brandIds),
  ],
);

// --- Type exports ---
export type Lead = typeof leads.$inferSelect;
export type NewLead = typeof leads.$inferInsert;
export type LeadContactMethod = typeof leadContactMethods.$inferSelect;
export type NewLeadContactMethod = typeof leadContactMethods.$inferInsert;
export type Organization = typeof organizations.$inferSelect;
export type NewOrganization = typeof organizations.$inferInsert;
export type LeadOrganization = typeof leadsOrganizations.$inferSelect;
export type NewLeadOrganization = typeof leadsOrganizations.$inferInsert;
export type LeadCampaign = typeof leadsCampaigns.$inferSelect;
export type NewLeadCampaign = typeof leadsCampaigns.$inferInsert;
export type CampaignApolloStrategies = typeof campaignsApolloStrategies.$inferSelect;
export type NewCampaignApolloStrategies = typeof campaignsApolloStrategies.$inferInsert;
export type IdempotencyCacheRow = typeof idempotencyCache.$inferSelect;
export type NewIdempotencyCacheRow = typeof idempotencyCache.$inferInsert;
export type BrandConversionToken = typeof brandConversionTokens.$inferSelect;
export type NewBrandConversionToken = typeof brandConversionTokens.$inferInsert;
export type ConversionEvent = typeof conversionEvents.$inferSelect;
export type NewConversionEvent = typeof conversionEvents.$inferInsert;
export type RequeuedServe = typeof requeuedServes.$inferSelect;
export type NewRequeuedServe = typeof requeuedServes.$inferInsert;
export type LeadStepDisqualification = typeof leadStepDisqualifications.$inferSelect;
export type NewLeadStepDisqualification = typeof leadStepDisqualifications.$inferInsert;
