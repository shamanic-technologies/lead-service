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

// Served leads — the dedup registry
export const servedLeads = pgTable(
  "served_leads",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    organizationId: uuid("organization_id")
      .notNull()
      .references(() => organizations.id, { onDelete: "cascade" }),
    namespace: text("namespace").notNull(),
    email: text("email").notNull(),
    externalId: text("external_id"),
    metadata: jsonb("metadata"),
    parentRunId: text("parent_run_id"),
    runId: text("run_id"),
    servedAt: timestamp("served_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex("idx_served_org_ns_email").on(table.organizationId, table.namespace, table.email),
    index("idx_served_org").on(table.organizationId),
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
    email: text("email").notNull(),
    externalId: text("external_id"),
    data: jsonb("data"),
    status: text("status").notNull().default("buffered"),
    pushRunId: text("push_run_id"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index("idx_buffer_org_ns_status").on(table.organizationId, table.namespace, table.status),
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
