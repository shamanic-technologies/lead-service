import { eq, and, sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { leadBuffer, enrichments } from "../db/schema.js";
import { checkDelivered, markServed } from "./dedup.js";
import { resolveOrCreateLead, findLeadByApolloPersonId } from "./leads-registry.js";
import { apolloSearchNext, apolloEnrich, apolloSearchParams, type ApolloSearchParams } from "./apollo-client.js";
import { fetchCampaign } from "./campaign-client.js";
import { fetchBrand } from "./brand-client.js";

async function isInBuffer(organizationId: string, campaignId: string, externalId: string): Promise<boolean> {
  const row = await db.query.leadBuffer.findFirst({
    where: and(
      eq(leadBuffer.organizationId, organizationId),
      eq(leadBuffer.namespace, campaignId),
      eq(leadBuffer.externalId, externalId)
    ),
  });
  return !!row;
}

const MAX_PAGES = 50;

async function fillBufferFromSearch(params: {
  organizationId: string;
  campaignId: string;
  brandId: string;
  searchParams: ApolloSearchParams;
  keySource: "platform" | "app" | "byok";
  pushRunId?: string | null;
  orgId?: string | null;
  userId?: string | null;
  appId?: string;
  workflowName?: string;
}): Promise<{ filled: number }> {
  // Fetch campaign + brand details in parallel for rich LLM context
  const [campaign, brand] = await Promise.all([
    fetchCampaign(params.campaignId, params.orgId),
    fetchBrand(params.brandId, params.orgId),
  ]);

  // Build rich context string from campaign, brand, and raw searchParams
  const contextParts: string[] = [];

  if (campaign?.targetAudience) contextParts.push(`Target audience: ${campaign.targetAudience}`);
  if (campaign?.targetOutcome) contextParts.push(`Expected outcome: ${campaign.targetOutcome}`);
  if (campaign?.valueForTarget) contextParts.push(`Value for the audience: ${campaign.valueForTarget}`);

  if (brand?.name) contextParts.push(`Brand: ${brand.name}`);
  if (brand?.elevatorPitch) contextParts.push(`Brand pitch: ${brand.elevatorPitch}`);
  if (brand?.bio) contextParts.push(`Brand bio: ${brand.bio}`);
  if (brand?.mission) contextParts.push(`Brand mission: ${brand.mission}`);
  if (brand?.categories) contextParts.push(`Brand categories: ${brand.categories}`);
  if (brand?.location) contextParts.push(`Brand location: ${brand.location}`);

  // Include raw searchParams as additional context
  const rawSearch = typeof params.searchParams === "string"
    ? params.searchParams
    : JSON.stringify(params.searchParams);
  contextParts.push(`Search parameters: ${rawSearch}`);

  const context = contextParts.join("\n");

  const { searchParams: validatedParams } = await apolloSearchParams({
    context,
    keySource: params.keySource,
    runId: params.pushRunId ?? "",
    appId: params.appId ?? "",
    brandId: params.brandId,
    campaignId: params.campaignId,
    orgId: params.orgId,
    workflowName: params.workflowName,
  });

  let totalFilled = 0;

  // Call apolloSearchNext in a loop — apollo-service manages pagination server-side.
  for (let page = 1; page <= MAX_PAGES; page++) {
    const result = await apolloSearchNext({
      campaignId: params.campaignId,
      brandId: params.brandId,
      appId: params.appId ?? "",
      searchParams: validatedParams,
      runId: params.pushRunId,
      orgId: params.orgId,
      workflowName: params.workflowName,
    });

    if (!result) {
      console.warn(`[fillBuffer] apolloSearchNext returned null (network error)`);
      break;
    }

    if (result.people.length === 0) {
      console.log(`[fillBuffer] Apollo returned 0 people (done=${result.done}), stopping`);
      break;
    }

    // Collect candidates from this page (not already in buffer)
    const candidates: Array<{
      data: Record<string, unknown>;
      externalId?: string;
      email?: string;
      leadId?: string;
    }> = [];

    for (const person of result.people) {
      // Skip if already in buffer
      if (person.id && await isInBuffer(params.organizationId, params.campaignId, person.id)) {
        continue;
      }

      let email = person.email || undefined;
      let leadId: string | undefined;
      let data: Record<string, unknown> = person;

      // Check enrichment cache for people without email
      if (!email && person.id) {
        const cached = await db.query.enrichments.findFirst({
          where: eq(enrichments.apolloPersonId, person.id),
        });

        if (cached) {
          if (!cached.email) {
            // Previously enriched, no email found — skip entirely
            continue;
          }
          email = cached.email;
          // Merge enriched data so buffer row has full person data (lastName, etc.)
          if (cached.responseRaw && typeof cached.responseRaw === "object") {
            data = { ...person, ...(cached.responseRaw as Record<string, unknown>) };
          }
        }
      }

      // Try to find existing leadId
      if (person.id) {
        const existingLeadId = await findLeadByApolloPersonId(person.id);
        if (existingLeadId) leadId = existingLeadId;
      }

      candidates.push({ data, externalId: person.id, email, leadId });
    }

    // Batch delivery check for candidates with emails and leadIds
    const itemsWithEmails = candidates
      .filter((c): c is typeof c & { email: string; leadId: string } => !!c.email && !!c.leadId)
      .map((c) => ({ email: c.email, leadId: c.leadId }));

    const deliveredMap =
      itemsWithEmails.length > 0
        ? await checkDelivered(params.brandId, params.campaignId, itemsWithEmails)
        : new Map<string, boolean>();

    let pageFilled = 0;

    for (const { data, externalId, email } of candidates) {
      if (email && deliveredMap.get(email)) continue;

      await db.insert(leadBuffer).values({
        organizationId: params.organizationId,
        namespace: params.campaignId,
        campaignId: params.campaignId,
        email: email ?? "",
        externalId: externalId,
        data,
        status: "buffered",
        pushRunId: params.pushRunId ?? null,
        brandId: params.brandId,
        orgId: params.orgId ?? null,
        userId: params.userId ?? null,
      });
      pageFilled++;
    }

    totalFilled += pageFilled;

    // Found new leads on this page — stop walking, let pullNext serve them
    if (pageFilled > 0) {
      console.log(`[fillBuffer] Buffered ${pageFilled} new leads from page ${page}`);
      return { filled: totalFilled };
    }

    // All people on this page were dupes — continue to next page
    console.log(`[fillBuffer] Page ${page}: all ${result.people.length} people already seen, continuing`);

    if (result.done) {
      console.log(`[fillBuffer] Apollo exhausted all pages, no new leads found`);
      break;
    }
  }

  return { filled: totalFilled };
}

export async function pullNext(params: {
  organizationId: string;
  campaignId: string;
  brandId: string;
  parentRunId?: string | null;
  runId?: string | null;
  keySource?: "platform" | "app" | "byok";
  searchParams?: ApolloSearchParams;
  orgId?: string | null;
  userId?: string | null;
  appId?: string;
  workflowName?: string;
}): Promise<{
  found: boolean;
  lead?: {
    leadId: string;
    email: string;
    externalId: string | null;
    data: unknown;
    brandId: string;
    orgId: string | null;
    userId: string | null;
  };
}> {
  const MAX_ITERATIONS = 100;
  let iterations = 0;
  while (true) {
    iterations++;

    if (iterations > MAX_ITERATIONS) {
      console.warn(`[pullNext] Hit MAX_ITERATIONS (${MAX_ITERATIONS}), giving up`);
      return { found: false };
    }
    const row = await db.query.leadBuffer.findFirst({
      where: and(
        eq(leadBuffer.organizationId, params.organizationId),
        eq(leadBuffer.namespace, params.campaignId),
        eq(leadBuffer.status, "buffered")
      ),
      orderBy: [sql`CASE WHEN ${leadBuffer.email} != '' THEN 0 ELSE 1 END`],
    });

    if (!row) {
      // Buffer empty - try to fill from search if searchParams provided
      if (params.searchParams && params.keySource) {
        const { filled } = await fillBufferFromSearch({
          organizationId: params.organizationId,
          campaignId: params.campaignId,
          brandId: params.brandId,
          searchParams: params.searchParams,
          keySource: params.keySource,
          pushRunId: params.runId,
          orgId: params.orgId,
          userId: params.userId,
          appId: params.appId,
          workflowName: params.workflowName,
        });

        if (filled > 0) {
          continue; // Retry pulling from buffer
        }
      }

      return { found: false };
    }

    // Enrich if no email (search results don't include emails)
    let email = row.email;
    let enrichedData = row.data;
    if (!email && row.externalId) {
      // Check enrichment cache first to avoid duplicate Apollo API calls
      const cached = await db.query.enrichments.findFirst({
        where: eq(enrichments.apolloPersonId, row.externalId),
      });

      if (cached) {
        if (cached.email) {
          // Use cached enrichment — no apollo-service call needed
          email = cached.email;
          enrichedData = cached.responseRaw ?? row.data;
          console.log(`[pullNext] Enrichment cache hit for personId=${row.externalId}`);
        } else {
          // Previously enriched but no email found — skip without calling Apollo
          console.log(`[pullNext] Enrichment cache hit (no email) for personId=${row.externalId}, skipping`);
          await db
            .update(leadBuffer)
            .set({ status: "skipped" })
            .where(eq(leadBuffer.id, row.id));
          continue;
        }
      } else {
        const enrichResult = await apolloEnrich(row.externalId, {
          runId: params.runId,
          orgId: params.orgId,
          appId: params.appId,
          brandId: params.brandId,
          campaignId: params.campaignId,
          workflowName: params.workflowName,
        });

        if (!enrichResult?.person?.email) {
          console.log(`[pullNext] Enrichment returned no email for personId=${row.externalId}`);
          // Cache the no-email result to avoid re-enriching this person
          await db.insert(enrichments).values({
            email: null,
            apolloPersonId: row.externalId,
            firstName: enrichResult?.person?.firstName ?? null,
            lastName: enrichResult?.person?.lastName ?? null,
            title: enrichResult?.person?.title ?? null,
            linkedinUrl: enrichResult?.person?.linkedinUrl ?? null,
            organizationName: enrichResult?.person?.organizationName ?? null,
            organizationDomain: enrichResult?.person?.organizationDomain ?? null,
            organizationIndustry: enrichResult?.person?.organizationIndustry ?? null,
            organizationSize: enrichResult?.person?.organizationSize ?? null,
            responseRaw: enrichResult?.person ?? null,
          }).onConflictDoNothing();
          await db
            .update(leadBuffer)
            .set({ status: "skipped" })
            .where(eq(leadBuffer.id, row.id));
          continue;
        }

        email = enrichResult.person.email;
        enrichedData = { ...(row.data as object ?? {}), ...enrichResult.person };

        // Save to enrichment cache for future lookups
        await db.insert(enrichments).values({
          email,
          apolloPersonId: row.externalId,
          firstName: enrichResult.person.firstName ?? null,
          lastName: enrichResult.person.lastName ?? null,
          title: enrichResult.person.title ?? null,
          linkedinUrl: enrichResult.person.linkedinUrl ?? null,
          organizationName: enrichResult.person.organizationName ?? null,
          organizationDomain: enrichResult.person.organizationDomain ?? null,
          organizationIndustry: enrichResult.person.organizationIndustry ?? null,
          organizationSize: enrichResult.person.organizationSize ?? null,
          responseRaw: enrichResult.person,
        }).onConflictDoNothing();
      }

      // Update buffer row with enriched email
      await db
        .update(leadBuffer)
        .set({ email, data: enrichedData })
        .where(eq(leadBuffer.id, row.id));
    }

    // Skip leads with no email — found: true must always have a usable email
    if (!email) {
      console.log(`[pullNext] No email after enrichment for buffer row ${row.id}, skipping`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    // Resolve or create the lead identity
    const { leadId } = await resolveOrCreateLead({
      apolloPersonId: row.externalId,
      email,
      metadata: enrichedData,
    });

    // Check delivery status via email-gateway
    const deliveredMap = await checkDelivered(params.brandId, params.campaignId, [
      { leadId, email },
    ]);

    if (deliveredMap.get(email)) {
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    await markServed({
      organizationId: params.organizationId,
      namespace: params.campaignId,
      brandId: params.brandId,
      campaignId: params.campaignId,
      email,
      leadId,
      externalId: row.externalId,
      metadata: enrichedData,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      orgId: row.orgId,
      userId: row.userId,
    });

    await db
      .update(leadBuffer)
      .set({ status: "served" })
      .where(eq(leadBuffer.id, row.id));

    // Ensure data.email always matches the canonical email
    const finalData =
      enrichedData && typeof enrichedData === "object"
        ? { ...(enrichedData as Record<string, unknown>), email }
        : { email };

    console.log(`[pullNext] Served lead: ${email} (leadId=${leadId})`);

    return {
      found: true,
      lead: {
        leadId,
        email,
        externalId: row.externalId,
        data: finalData,
        brandId: params.brandId,
        orgId: row.orgId,
        userId: row.userId,
      },
    };
  }
}
