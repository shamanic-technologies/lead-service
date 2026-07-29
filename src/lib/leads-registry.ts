import { eq, and, sql } from "drizzle-orm";
import { db } from "../db/index.js";
import {
  leads,
  leadContactMethods,
  organizations,
  leadsOrganizations,
  type NewLead,
  type NewOrganization,
} from "../db/schema.js";
import type { Person } from "./people-client.js";

// Person fields the gateway's neutral Person provides that map 1:1 to lead columns.
// (departments / functions / twitter / github / facebook are NOT in the neutral
// shape — those columns simply stay null under the gateway.)
const PERSON_FIELDS = [
  "firstName",
  "lastName",
  "name",
  "linkedinUrl",
  "photoUrl",
  "headline",
  "city",
  "state",
  "country",
  "seniority",
  "timezone",
] as const;

function pickPersonFields(person: Person): Partial<NewLead> {
  const out: Partial<NewLead> = {};
  for (const key of PERSON_FIELDS) {
    const v = (person as unknown as Record<string, unknown>)[key];
    if (v !== undefined && v !== null && v !== "") {
      (out as Record<string, unknown>)[key] = v;
    }
  }
  return out;
}

function pickOrgFields(org: NonNullable<Person["organization"]>): Partial<NewOrganization> {
  const out: Partial<NewOrganization> = {};
  if (org.name) out.name = org.name;
  if (org.domain) out.primaryDomain = org.domain;
  if (org.websiteUrl) out.websiteUrl = org.websiteUrl;
  if (org.industry) out.industry = org.industry;
  if (org.logoUrl) out.logoUrl = org.logoUrl;
  if (org.linkedinUrl) out.linkedinUrl = org.linkedinUrl;
  if (org.city) out.city = org.city;
  if (org.state) out.state = org.state;
  if (org.country) out.country = org.country;
  if (org.estimatedNumEmployees != null) out.estimatedNumEmployees = org.estimatedNumEmployees;
  if (org.annualRevenue != null) out.annualRevenue = String(org.annualRevenue);
  return out;
}

/**
 * Resolve the canonical lead for a person and upsert its structured fields.
 * Returns the leadId.
 *
 * Identity precedence — EMAIL OWNER FIRST:
 *   1. the lead that already owns `person.email` in lead_contact_methods
 *   2. the lead carrying `person.providerPersonId` (leads.apollo_person_id)
 *   3. a fresh row
 *
 * A person is ONE identity, and the email is the strongest key we have for it.
 * `idx_lcm_channel_value` makes "one email = one lead" a hard invariant, so once
 * a lead owns an email NO other lead can ever carry it. Attributing a serve to
 * any other lead therefore produces a permanently email-less lead: the read path
 * keys the email-gateway delivery overlay on the REGISTERED email, so such a
 * lead can never resolve contacted/sent/delivered and is invisible to every
 * consumer. Provider person ids churn (a re-crawl mints a new id for the same
 * human); an already-registered email does not.
 */
export async function upsertLeadFromPerson(
  person: Person,
  options: { enriched: boolean },
): Promise<string> {
  const fields = pickPersonFields(person);
  const metadata = person as unknown;
  const enrichedAt = options.enriched ? new Date() : null;

  if (!person.email && !person.providerPersonId) {
    throw new Error(
      "[lead-service] upsertLeadFromPerson: person has no providerPersonId and no email",
    );
  }

  const emailOwnerId = person.email ? await findLeadByEmail(person.email) : null;
  const providerOwnerId = person.providerPersonId
    ? await findLeadByApolloPersonId(person.providerPersonId)
    : null;

  if (emailOwnerId && providerOwnerId && emailOwnerId !== providerOwnerId) {
    // Provider id churn: the same human exists twice in silver. The email owner
    // is the identity that can actually carry the email, so it wins.
    console.log(
      `[lead-service] identity: email owner wins over provider person id — email=${person.email} emailLeadId=${emailOwnerId} providerPersonId=${person.providerPersonId} providerLeadId=${providerOwnerId}`,
    );
  }

  const existingId = emailOwnerId ?? providerOwnerId;
  if (existingId) {
    await db
      .update(leads)
      .set({ ...fields, metadata, ...(enrichedAt ? { enrichedAt } : {}) })
      .where(eq(leads.id, existingId));
    return existingId;
  }

  const inserted = await db
    .insert(leads)
    .values({
      apolloPersonId: person.providerPersonId ?? null,
      ...fields,
      metadata,
      enrichedAt,
    })
    .returning({ id: leads.id });
  if (inserted[0]) return inserted[0].id;

  // Race: another writer inserted the same identity between our lookup and insert.
  const raced = person.providerPersonId
    ? await findLeadByApolloPersonId(person.providerPersonId)
    : person.email
      ? await findLeadByEmail(person.email)
      : null;
  if (raced) return raced;
  throw new Error("[lead-service] upsertLeadFromPerson failed to insert or locate the lead");
}

/**
 * Upsert organization from a neutral Person's top-level org. The gateway provides
 * no provider org id, so we key on primaryDomain (stable) and fall back to name.
 * Returns organizationId or null when the person has no org.
 */
export async function upsertOrganizationFromPerson(person: Person): Promise<string | null> {
  const org = person.organization;
  if (!org || (!org.domain && !org.name)) return null;
  const fields = pickOrgFields(org);

  const existing = org.domain
    ? await db.query.organizations.findFirst({ where: eq(organizations.primaryDomain, org.domain) })
    : await db.query.organizations.findFirst({ where: eq(organizations.name, org.name as string) });

  if (existing) {
    await db
      .update(organizations)
      .set({ ...fields, updatedAt: new Date() })
      .where(eq(organizations.id, existing.id));
    return existing.id;
  }

  const inserted = await db
    .insert(organizations)
    .values({ ...fields })
    .returning({ id: organizations.id });
  return inserted[0]?.id ?? null;
}

export type UpsertContactResult =
  | { inserted: true }
  | { inserted: false; reason: "global_collision" };

/**
 * Upsert a lead contact method (email/phone/twitter/etc.).
 *
 * Two unique indexes apply:
 *   - idx_lcm_lead_channel_value (lead_id, channel, value) — same-lead re-enrichment, handled by ON CONFLICT DO UPDATE.
 *   - idx_lcm_channel_value (channel, value)               — global "one email = one lead" invariant.
 *
 * When the second collides (the provider returns an email already attached to a
 * different lead — role inboxes, EA addresses, person-id churn), Postgres raises
 * 23505 because ON CONFLICT can target only one constraint. We catch that specific
 * case and return { inserted: false, reason: "global_collision" } so the caller can
 * mark the lead skipped under a distinct status reason.
 */
export async function upsertContactMethod(params: {
  leadId: string;
  channel: string;
  value: string;
  status?: string | null;
  source: string;
}): Promise<UpsertContactResult> {
  try {
    await db
      .insert(leadContactMethods)
      .values({
        leadId: params.leadId,
        channel: params.channel,
        value: params.value,
        status: params.status ?? null,
        source: params.source,
      })
      .onConflictDoUpdate({
        target: [leadContactMethods.leadId, leadContactMethods.channel, leadContactMethods.value],
        set: {
          status: params.status ?? null,
          source: params.source,
        },
      });
    return { inserted: true };
  } catch (err) {
    if (isGlobalContactDupKey(err)) {
      return { inserted: false, reason: "global_collision" };
    }
    throw err;
  }
}

/**
 * Register `email` on `leadId` and return the lead that OWNS it afterwards.
 *
 * Normally that is `leadId` itself — `upsertLeadFromPerson` already resolves the
 * email owner first, so the insert succeeds. The re-resolution below covers the
 * race where a concurrent serve registered the same email on another lead
 * between that lookup and this insert.
 *
 * Either way the caller ends up with a lead whose email IS registered, so a
 * serve is never recorded against a lead whose delivery status can't resolve.
 * A collision we cannot resolve to an owner is a broken invariant, not a
 * warning: it fails loud.
 */
export async function registerServedEmail(params: {
  leadId: string;
  email: string;
  status: string | null;
  source: string;
}): Promise<string> {
  const result = await upsertContactMethod({
    leadId: params.leadId,
    channel: "email",
    value: params.email,
    status: params.status,
    source: params.source,
  });
  if (result.inserted) return params.leadId;

  const owner = await findLeadByEmail(params.email);
  if (!owner) {
    throw new Error(
      `[lead-service] email ${params.email} hit the global one-email-one-lead index but no owning lead could be found (leadId=${params.leadId})`,
    );
  }
  console.log(
    `[lead-service] identity: re-attributed serve to the lead that owns the email — email=${params.email} from=${params.leadId} to=${owner}`,
  );
  return owner;
}

function isGlobalContactDupKey(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;
  const e = err as { code?: string; constraint_name?: string };
  return e.code === "23505" && e.constraint_name === "idx_lcm_channel_value";
}

/**
 * Mark the lead's link to `organizationId` as the current employer, idempotent on
 * (leadId, organizationId). Updates the existing link in place when present (so
 * re-enrichment never grows the row count); inserts otherwise.
 */
async function markCurrentEmployment(
  leadId: string,
  organizationId: string,
  title: string | null,
): Promise<void> {
  const existing = await db
    .select({ id: leadsOrganizations.id })
    .from(leadsOrganizations)
    .where(
      and(
        eq(leadsOrganizations.leadId, leadId),
        eq(leadsOrganizations.organizationId, organizationId),
      ),
    )
    .limit(1);
  if (existing[0]) {
    await db
      .update(leadsOrganizations)
      .set({ current: true, title })
      .where(eq(leadsOrganizations.id, existing[0].id));
    return;
  }
  await db.insert(leadsOrganizations).values({ leadId, organizationId, title, current: true });
}

/**
 * Persist the current employer from a neutral Person.
 *
 * The gateway provides no employment-history array (Apollo's firehose did), only
 * the top-level org. We still enforce exactly ONE current employer per lead at
 * write time, history-preserving (no rows deleted):
 *   1. Expire the current flag on all existing rows for this lead.
 *   2. Upsert the top-level org and mark it current=true.
 *
 * Idempotent on re-enrichment: org id is stable (by domain/name), the current
 * link is upserted — so re-running does not grow row count.
 */
export async function recordEmploymentHistory(params: {
  leadId: string;
  person: Person;
}): Promise<void> {
  const { leadId, person } = params;

  await db
    .update(leadsOrganizations)
    .set({ current: false })
    .where(and(eq(leadsOrganizations.leadId, leadId), eq(leadsOrganizations.current, true)));

  const orgId = await upsertOrganizationFromPerson(person);
  if (orgId) {
    await markCurrentEmployment(leadId, orgId, person.title ?? null);
  }
}

/**
 * Find leadId by apolloPersonId.
 */
export async function findLeadByApolloPersonId(apolloPersonId: string): Promise<string | null> {
  const lead = await db.query.leads.findFirst({
    where: eq(leads.apolloPersonId, apolloPersonId),
  });
  return lead?.id ?? null;
}

/**
 * Find leadId by email (joins lead_contact_methods).
 */
export async function findLeadByEmail(email: string): Promise<string | null> {
  const row = await db.query.leadContactMethods.findFirst({
    where: and(eq(leadContactMethods.channel, "email"), eq(leadContactMethods.value, email)),
  });
  return row?.leadId ?? null;
}

/**
 * Returns true when the given lead has at least one email contact method.
 */
export async function leadHasEmail(leadId: string): Promise<boolean> {
  const result = await db
    .select({ exists: sql<boolean>`true` })
    .from(leadContactMethods)
    .where(and(eq(leadContactMethods.leadId, leadId), eq(leadContactMethods.channel, "email")))
    .limit(1);
  return result.length > 0;
}

/**
 * Get the primary email for a lead (most recently inserted).
 */
export async function getPrimaryEmail(leadId: string): Promise<{ email: string; status: string | null } | null> {
  const row = await db.query.leadContactMethods.findFirst({
    where: and(eq(leadContactMethods.leadId, leadId), eq(leadContactMethods.channel, "email")),
    orderBy: (methods, { desc }) => [desc(methods.createdAt)],
  });
  if (!row) return null;
  return { email: row.value, status: row.status };
}
