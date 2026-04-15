import { eq, and, sql } from "drizzle-orm";
import { db, sql as pgSql } from "../db/index.js";
import { leadBuffer, enrichments } from "../db/schema.js";
import { checkContacted, markServed, isAlreadyServedForBrand, checkRaceWindow } from "./dedup.js";
import { resolveOrCreateLead, findLeadByApolloPersonId } from "./leads-registry.js";
import { apolloSearchNext, apolloEnrich, apolloSearchParams } from "./apollo-client.js";
import { fetchCampaign } from "./campaign-client.js";
import { extractBrandFields } from "./brand-client.js";

async function isInBuffer(orgId: string, campaignId: string, externalId: string): Promise<boolean> {
  const row = await db.query.leadBuffer.findFirst({
    where: and(
      eq(leadBuffer.orgId, orgId),
      eq(leadBuffer.campaignId, campaignId),
      eq(leadBuffer.externalId, externalId)
    ),
  });
  return !!row;
}

const MAX_PAGES = 50;

async function fillBufferFromSearch(params: {
  orgId: string;
  campaignId: string;
  brandIds: string[];
  pushRunId?: string | null;
  userId?: string | null;
  workflowSlug?: string;
  featureSlug?: string;
}): Promise<{ filled: number }> {
  const primaryBrandId = params.brandIds[0];
  const brandIdCsv = params.brandIds.join(",");

  const serviceContext = {
    userId: params.userId ?? undefined,
    runId: params.pushRunId ?? undefined,
    campaignId: params.campaignId,
    brandId: brandIdCsv,
    workflowSlug: params.workflowSlug,
    featureSlug: params.featureSlug,
  };

  // Fetch campaign + brand fields in parallel for rich LLM context
  const [campaign, brandFields] = await Promise.all([
    fetchCampaign(params.campaignId, params.orgId, serviceContext),
    extractBrandFields(
      [
        { key: "brand_name", description: "The brand's display name" },
        { key: "elevator_pitch", description: "A short elevator pitch describing the brand" },
        { key: "industry", description: "The brand's primary industry vertical" },
        { key: "target_geography", description: "Priority geographic markets for outreach" },
        { key: "ideal_lead_type", description: "Type of leads to target (journalists, editors, producers, executives...)" },
        { key: "target_job_titles", description: "Job titles to prioritize in outreach" },
        { key: "offerings", description: "Key products or services the brand offers" },
      ],
      params.orgId,
      serviceContext,
    ),
  ]);

  // Build rich context string from campaign, brand fields, and featureInputs
  const contextParts: string[] = [];

  if (campaign?.targetAudience) contextParts.push(`Target audience: ${campaign.targetAudience}`);
  if (campaign?.targetOutcome) contextParts.push(`Expected outcome: ${campaign.targetOutcome}`);
  if (campaign?.valueForTarget) contextParts.push(`Value for the audience: ${campaign.valueForTarget}`);

  // Inject campaign featureInputs
  const featureInputs = campaign?.featureInputs;
  if (featureInputs && Object.keys(featureInputs).length > 0) {
    contextParts.push(`Campaign context: ${JSON.stringify(featureInputs)}`);
  }

  // Inject brand fields from extract-fields (convention 1)
  if (brandFields) {
    for (const field of brandFields) {
      if (field.value != null) {
        const label = field.key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
        const value = typeof field.value === "string" ? field.value : JSON.stringify(field.value);
        contextParts.push(`${label}: ${value}`);
      }
    }
  }

  const context = contextParts.join("\n");

  const { searchParams: validatedParams } = await apolloSearchParams({
    context,
    runId: params.pushRunId ?? "",
    brandId: brandIdCsv,
    campaignId: params.campaignId,
    orgId: params.orgId,
    userId: params.userId,
    workflowSlug: params.workflowSlug,
    featureSlug: params.featureSlug,
  });

  let totalFilled = 0;

  // Call apolloSearchNext in a loop — apollo-service manages pagination server-side.
  for (let page = 1; page <= MAX_PAGES; page++) {
    const result = await apolloSearchNext({
      campaignId: params.campaignId,
      brandId: brandIdCsv,
      searchParams: validatedParams,
      runId: params.pushRunId,
      orgId: params.orgId,
      userId: params.userId,
      workflowSlug: params.workflowSlug,
      featureSlug: params.featureSlug,
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
      if (person.id && await isInBuffer(params.orgId, params.campaignId, person.id)) {
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

    // Batch contacted check for candidates with emails and leadIds
    const itemsWithEmails = candidates
      .filter((c): c is typeof c & { email: string; leadId: string } => !!c.email && !!c.leadId)
      .map((c) => ({ email: c.email }));

    const contactedMap =
      itemsWithEmails.length > 0
        ? await checkContacted(params.brandIds, params.campaignId, itemsWithEmails, {
            orgId: params.orgId,
            userId: params.userId ?? undefined,
            runId: params.pushRunId ?? undefined,
            campaignId: params.campaignId,
            brandId: brandIdCsv,
            workflowSlug: params.workflowSlug,
            featureSlug: params.featureSlug,
          })
        : new Map<string, import("./email-gateway-client.js").EmailCheckResult>();

    let pageFilled = 0;

    for (const { data, externalId, email } of candidates) {
      const status = email ? contactedMap.get(email) : undefined;
      if (status?.contacted || status?.bounced || status?.unsubscribed) continue;

      await db.insert(leadBuffer).values({
        namespace: "apollo",
        campaignId: params.campaignId,
        email: email ?? "",
        externalId: externalId,
        data,
        status: "buffered",
        pushRunId: params.pushRunId ?? null,
        brandIds: params.brandIds,
        orgId: params.orgId,
        userId: params.userId ?? null,
        workflowSlug: params.workflowSlug ?? null,
        featureSlug: params.featureSlug ?? null,
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

    if (result.done) {
      console.log(`[fillBuffer] Apollo exhausted all pages, no new leads found`);
      break;
    }
  }

  return { filled: totalFilled };
}

export async function pullNext(params: {
  orgId: string;
  campaignId: string;
  brandIds: string[];
  runId?: string | null;
  userId?: string | null;
  workflowSlug?: string;
  featureSlug?: string;
}): Promise<{
  found: boolean;
  lead?: {
    leadId: string;
    email: string;
    data: unknown;
    brandIds: string[];
    orgId: string | null;
    userId: string | null;
    apolloPersonId: string | null;
  };
}> {
  const brandIdCsv = params.brandIds.join(",");
  const MAX_ITERATIONS = 100;
  let iterations = 0;
  while (true) {
    iterations++;

    if (iterations > MAX_ITERATIONS) {
      console.warn(`[pullNext] Hit MAX_ITERATIONS (${MAX_ITERATIONS}), giving up`);
      return { found: false };
    }
    // Use FOR UPDATE SKIP LOCKED to atomically claim a buffer row,
    // preventing concurrent pullNext calls from picking the same lead.
    const claimedRows = await pgSql`
      UPDATE lead_buffer
      SET status = 'claimed'
      WHERE id = (
        SELECT id FROM lead_buffer
        WHERE org_id = ${params.orgId}
          AND campaign_id = ${params.campaignId}
          AND namespace = 'apollo'
          AND status = 'buffered'
        ORDER BY CASE WHEN email != '' THEN 0 ELSE 1 END
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *
    `;

    const row = claimedRows.length > 0 ? {
      id: claimedRows[0].id as string,
      namespace: claimedRows[0].namespace as string,
      campaignId: claimedRows[0].campaign_id as string,
      email: claimedRows[0].email as string,
      externalId: claimedRows[0].external_id as string | null,
      data: claimedRows[0].data as unknown,
      status: claimedRows[0].status as string,
      pushRunId: claimedRows[0].push_run_id as string | null,
      brandIds: claimedRows[0].brand_ids as string[] | null,
      orgId: claimedRows[0].org_id as string,
      userId: claimedRows[0].user_id as string | null,
      createdAt: claimedRows[0].created_at as Date,
    } : null;

    if (!row) {
      // Buffer empty — fill from Apollo search
      const result = await fillBufferFromSearch({
        orgId: params.orgId,
        campaignId: params.campaignId,
        brandIds: params.brandIds,
        pushRunId: params.runId,
        userId: params.userId,
        workflowSlug: params.workflowSlug,
        featureSlug: params.featureSlug,
      });

      if (result.filled > 0) {
        continue; // Retry pulling from buffer
      }

      console.log(`[lead-service] pullNext found=false campaign=${params.campaignId}`);
      return { found: false };
    }

    // Enrich if no email
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
        } else {
          // Previously enriched but no email found — skip without calling Apollo
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
          userId: params.userId,
          brandId: brandIdCsv,
          campaignId: params.campaignId,
          workflowSlug: params.workflowSlug,
          featureSlug: params.featureSlug,
        });

        if (!enrichResult?.person?.email) {
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

    // DB-level brand dedup: check served_leads cross-campaign via brand_ids overlap
    const brandCheck = await isAlreadyServedForBrand({
      orgId: params.orgId,
      brandIds: params.brandIds,
      leadId,
      email,
      externalId: row.externalId,
    });

    if (brandCheck.blocked) {
      console.log(`[lead-service] pullNext skip (brand dedup): ${brandCheck.reason} email=${email}`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    // Race window: skip if another buffer row for the same brand was recently claimed/served
    const inRaceWindow = await checkRaceWindow({
      orgId: params.orgId,
      brandIds: params.brandIds,
      email,
      excludeBufferId: row.id,
    });

    if (inRaceWindow) {
      console.log(`[lead-service] pullNext skip (race window) email=${email}`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    // Email-gateway: contacted + bounce + unsub check (fails loud if unreachable)
    const statusMap = await checkContacted(params.brandIds, params.campaignId, [
      { email },
    ], {
      orgId: params.orgId,
      userId: params.userId ?? undefined,
      runId: params.runId ?? undefined,
      campaignId: params.campaignId,
      brandId: brandIdCsv,
      workflowSlug: params.workflowSlug,
      featureSlug: params.featureSlug,
    });

    const emailStatus = statusMap.get(email);
    if (emailStatus?.contacted) {
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }
    if (emailStatus?.bounced) {
      console.log(`[lead-service] pullNext skip (bounced) email=${email}`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }
    if (emailStatus?.unsubscribed) {
      console.log(`[lead-service] pullNext skip (unsubscribed) email=${email}`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    const { inserted } = await markServed({
      orgId: params.orgId,
      namespace: "apollo",
      brandIds: params.brandIds,
      campaignId: params.campaignId,
      email,
      leadId,
      externalId: row.externalId,
      metadata: enrichedData,
      runId: params.runId ?? null,
      userId: row.userId,
      workflowSlug: params.workflowSlug ?? null,
      featureSlug: params.featureSlug ?? null,
    });

    if (!inserted) {
      // Another request already served this email for this org+campaign — skip
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    await db
      .update(leadBuffer)
      .set({ status: "served" })
      .where(eq(leadBuffer.id, row.id));

    // Ensure data.email always matches the canonical email
    const finalData =
      enrichedData && typeof enrichedData === "object"
        ? { ...(enrichedData as Record<string, unknown>), email }
        : { email };

    const dataObj = finalData as Record<string, unknown>;

    console.log(`[lead-service] pullNext found=true campaign=${params.campaignId} email=${email} leadId=${leadId}`);
    return {
      found: true,
      lead: {
        leadId,
        email,
        data: finalData,
        brandIds: params.brandIds,
        orgId: row.orgId,
        userId: row.userId,
        apolloPersonId: (dataObj.id as string) ?? null,
      },
    };
  }
}
