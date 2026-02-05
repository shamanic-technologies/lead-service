import { eq } from "drizzle-orm";
import { db } from "../db/index.js";
import { enrichments, type Enrichment } from "../db/schema.js";
import { apolloEnrich } from "./apollo-client.js";

export interface EnrichmentResult extends Enrichment {
  cached: boolean;
}

export async function getEnrichment(email: string): Promise<EnrichmentResult | null> {
  const normalizedEmail = email.toLowerCase().trim();

  // Check cache first
  const cached = await db.query.enrichments.findFirst({
    where: eq(enrichments.email, normalizedEmail),
  });

  if (cached) {
    return { ...cached, cached: true };
  }

  // Call Apollo
  const apolloData = await apolloEnrich(normalizedEmail);
  if (!apolloData) {
    return null;
  }

  // Cache the result
  const [inserted] = await db
    .insert(enrichments)
    .values({
      email: normalizedEmail,
      apolloPersonId: apolloData.id,
      firstName: apolloData.firstName,
      lastName: apolloData.lastName,
      title: apolloData.title,
      linkedinUrl: apolloData.linkedinUrl,
      organizationName: apolloData.organizationName,
      organizationDomain: apolloData.organizationDomain,
      organizationIndustry: apolloData.organizationIndustry,
      organizationSize: apolloData.organizationSize,
      responseRaw: apolloData,
    })
    .onConflictDoNothing()
    .returning();

  // Handle race condition - if another request inserted, fetch it
  if (!inserted) {
    const existing = await db.query.enrichments.findFirst({
      where: eq(enrichments.email, normalizedEmail),
    });
    return existing ? { ...existing, cached: true } : null;
  }

  return { ...inserted, cached: false };
}
