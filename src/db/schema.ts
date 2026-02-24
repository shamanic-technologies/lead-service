import { pgTable, uuid, text, timestamp, uniqueIndex, index, jsonb } from "drizzle-orm/pg-core";

// Organizations — maps external org IDs (e.g., Clerk) to internal UUIDs
export const organizations = pgTable(
  "organizations",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    appId: text("app_id").notNull(),
    externalId: text("external_id").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_orgs_app_external").on(table.appId, table.externalId),
  ]
);

// Users — maps external user IDs to internal UUIDs
export const users = pgTable(
  "users",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    externalId: text("external_id").notNull(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_users_org_external").on(table.organizationId, table.externalId),
  ]
);

// Leads — global identity registry (no org/brand/campaign scoping)
export const leads = pgTable(
  "leads",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    apolloPersonId: text("apollo_person_id"),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_leads_apollo_person_id").on(table.apolloPersonId),
  ]
);

// Lead emails — email addresses belonging to a lead (1:N)
export const leadEmails = pgTable(
  "lead_emails",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    leadId: uuid("lead_id")
      .notNull()
      .references(() => leads.id, { onDelete: "cascade" }),
    email: text("email").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_lead_emails_lead_email").on(table.leadId, table.email),
    uniqueIndex("idx_lead_emails_email").on(table.email),
  ]
);

// Served leads — audit log of leads pulled from buffer (dedup now via email-gateway)
export const servedLeads = pgTable(
  "served_leads",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    leadId: uuid("lead_id").references(() => leads.id),
    namespace: text("namespace").notNull(),
    email: text("email").notNull(),
    externalId: text("external_id"),
    metadata: jsonb("metadata"),
    parentRunId: text("parent_run_id"),
    runId: text("run_id"),
    brandId: text("brand_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    clerkOrgId: text("clerk_org_id"),
    clerkUserId: text("clerk_user_id"),
    servedAt: timestamp("served_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_served_org_brand_email").on(table.organizationId, table.brandId, table.email),
    index("idx_served_org").on(table.organizationId),
    index("idx_served_brand").on(table.brandId),
    index("idx_served_campaign").on(table.campaignId),
    index("idx_served_clerk_org").on(table.clerkOrgId),
    index("idx_served_clerk_user").on(table.clerkUserId),
  ]
);

// Lead buffer — temporary staging for leads not yet served
export const leadBuffer = pgTable(
  "lead_buffer",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    namespace: text("namespace").notNull(),
    campaignId: text("campaign_id").notNull(),
    email: text("email").notNull(),
    externalId: text("external_id"),
    data: jsonb("data"),
    status: text("status").notNull().default("buffered"),
    pushRunId: text("push_run_id"),
    brandId: text("brand_id"),
    clerkOrgId: text("clerk_org_id"),
    clerkUserId: text("clerk_user_id"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index("idx_buffer_org_ns_status").on(table.organizationId, table.namespace, table.status),
  ]
);

// Enrichments — global cache for Apollo enrichment data (no orgId)
export const enrichments = pgTable(
  "enrichments",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    email: text("email"),
    apolloPersonId: text("apollo_person_id"),
    firstName: text("first_name"),
    lastName: text("last_name"),
    title: text("title"),
    linkedinUrl: text("linkedin_url"),
    organizationName: text("organization_name"),
    organizationDomain: text("organization_domain"),
    organizationIndustry: text("organization_industry"),
    organizationSize: text("organization_size"),
    responseRaw: jsonb("response_raw"),
    enrichedAt: timestamp("enriched_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_enrichments_email").on(table.email),
    uniqueIndex("idx_enrichments_apollo_person_id").on(table.apolloPersonId),
  ]
);

// Idempotency cache — prevents duplicate lead consumption on retries
export const idempotencyCache = pgTable(
  "idempotency_cache",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    idempotencyKey: text("idempotency_key").notNull(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    response: jsonb("response").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_idempotency_key").on(table.idempotencyKey),
  ]
);

// Cursors — pagination state per org+namespace
export const cursors = pgTable(
  "cursors",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    namespace: text("namespace").notNull(),
    state: jsonb("state"),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_cursors_org_ns").on(table.organizationId, table.namespace),
  ]
);

// Type exports
export type Organization = typeof organizations.$inferSelect;
export type NewOrganization = typeof organizations.$inferInsert;
export type User = typeof users.$inferSelect;
export type NewUser = typeof users.$inferInsert;
export type ServedLead = typeof servedLeads.$inferSelect;
export type NewServedLead = typeof servedLeads.$inferInsert;
export type LeadBufferRow = typeof leadBuffer.$inferSelect;
export type NewLeadBufferRow = typeof leadBuffer.$inferInsert;
export type Cursor = typeof cursors.$inferSelect;
export type NewCursor = typeof cursors.$inferInsert;
export type Enrichment = typeof enrichments.$inferSelect;
export type NewEnrichment = typeof enrichments.$inferInsert;
export type IdempotencyCacheRow = typeof idempotencyCache.$inferSelect;
export type NewIdempotencyCacheRow = typeof idempotencyCache.$inferInsert;
export type Lead = typeof leads.$inferSelect;
export type NewLead = typeof leads.$inferInsert;
export type LeadEmail = typeof leadEmails.$inferSelect;
export type NewLeadEmail = typeof leadEmails.$inferInsert;
