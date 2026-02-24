import { eq } from "drizzle-orm";
import { db } from "../db/index.js";
import { leads, leadEmails } from "../db/schema.js";

export interface ResolvedLead {
  leadId: string;
  isNew: boolean;
}

/**
 * Find or create a lead by apolloPersonId, and ensure the email is associated.
 * Lookup priority: apolloPersonId first, then email.
 */
export async function resolveOrCreateLead(params: {
  apolloPersonId?: string | null;
  email: string;
  metadata?: unknown;
}): Promise<ResolvedLead> {
  // 1. Try to find by apolloPersonId (strongest identity signal)
  if (params.apolloPersonId) {
    const existing = await db.query.leads.findFirst({
      where: eq(leads.apolloPersonId, params.apolloPersonId),
    });

    if (existing) {
      // Ensure this email is linked to the lead
      await db
        .insert(leadEmails)
        .values({ leadId: existing.id, email: params.email })
        .onConflictDoNothing();

      return { leadId: existing.id, isNew: false };
    }
  }

  // 2. Try to find by email
  const existingByEmail = await db.query.leadEmails.findFirst({
    where: eq(leadEmails.email, params.email),
  });

  if (existingByEmail) {
    // If we now have an apolloPersonId, update the lead record
    if (params.apolloPersonId) {
      await db
        .update(leads)
        .set({
          apolloPersonId: params.apolloPersonId,
          ...(params.metadata !== undefined ? { metadata: params.metadata } : {}),
        })
        .where(eq(leads.id, existingByEmail.leadId));
    }

    return { leadId: existingByEmail.leadId, isNew: false };
  }

  // 3. Create new lead + email
  const [newLead] = await db
    .insert(leads)
    .values({
      apolloPersonId: params.apolloPersonId ?? null,
      metadata: params.metadata ?? null,
    })
    .onConflictDoNothing()
    .returning();

  if (newLead) {
    await db
      .insert(leadEmails)
      .values({ leadId: newLead.id, email: params.email })
      .onConflictDoNothing();

    return { leadId: newLead.id, isNew: true };
  }

  // Race condition: apolloPersonId was inserted by another request
  if (params.apolloPersonId) {
    const racedLead = await db.query.leads.findFirst({
      where: eq(leads.apolloPersonId, params.apolloPersonId),
    });
    if (racedLead) {
      await db
        .insert(leadEmails)
        .values({ leadId: racedLead.id, email: params.email })
        .onConflictDoNothing();
      return { leadId: racedLead.id, isNew: false };
    }
  }

  throw new Error(
    `Failed to resolve or create lead for email=${params.email}`
  );
}

/**
 * Find leadId by apolloPersonId.
 */
export async function findLeadByApolloPersonId(
  apolloPersonId: string
): Promise<string | null> {
  const lead = await db.query.leads.findFirst({
    where: eq(leads.apolloPersonId, apolloPersonId),
  });
  return lead?.id ?? null;
}

/**
 * Find leadId by email.
 */
export async function findLeadByEmail(
  email: string
): Promise<string | null> {
  const row = await db.query.leadEmails.findFirst({
    where: eq(leadEmails.email, email),
  });
  return row?.leadId ?? null;
}
