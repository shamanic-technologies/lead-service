import { eq, and, sql } from "drizzle-orm";
import { db, sql as pgSql } from "../db/index.js";
import { leadBuffer, enrichments, cursors } from "../db/schema.js";
import { checkContacted, markServed } from "./dedup.js";
import { resolveOrCreateLead, findLeadByApolloPersonId } from "./leads-registry.js";
import { apolloSearchNext, apolloEnrich, apolloMatch, apolloSearchParams } from "./apollo-client.js";
import { fetchCampaign } from "./campaign-client.js";
import { extractBrandFields } from "./brand-client.js";
import { fetchOutletsByCampaign, fetchNextOutlet, type OutletDetails } from "./outlet-client.js";
import { fetchNextJournalist } from "./journalist-client.js";

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
      .map((c) => ({ email: c.email, leadId: c.leadId }));

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
        : new Map<string, boolean>();

    let pageFilled = 0;

    for (const { data, externalId, email } of candidates) {
      if (email && contactedMap.get(email)) continue;

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

// --- Journalist source helpers ---

function extractDomain(url: string): string {
  try {
    const parsed = new URL(url.startsWith("http") ? url : `https://${url}`);
    return parsed.hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

async function saveCursor(orgId: string, namespace: string, state: unknown): Promise<void> {
  const existing = await db.query.cursors.findFirst({
    where: and(eq(cursors.orgId, orgId), eq(cursors.namespace, namespace)),
  });
  if (existing) {
    await db.update(cursors).set({ state, updatedAt: new Date() }).where(eq(cursors.id, existing.id));
  } else {
    await db.insert(cursors).values({ orgId, namespace, state });
  }
}

async function fillBufferFromJournalists(params: {
  orgId: string;
  campaignId: string;
  brandIds: string[];
  pushRunId?: string | null;
  userId?: string | null;
  workflowSlug?: string;
  featureSlug?: string;
}): Promise<{ filled: number }> {
  const brandIdCsv = params.brandIds.join(",");

  const serviceContext = {
    userId: params.userId ?? undefined,
    runId: params.pushRunId ?? undefined,
    campaignId: params.campaignId,
    brandId: brandIdCsv,
    workflowSlug: params.workflowSlug,
    featureSlug: params.featureSlug,
  };

  // Load cursor state
  const cursorNamespace = `journalist:${params.campaignId}`;
  const existingCursor = await db.query.cursors.findFirst({
    where: and(
      eq(cursors.orgId, params.orgId),
      eq(cursors.namespace, cursorNamespace)
    ),
  });
  const cursorState = (existingCursor?.state as { outletIndex: number } | null)
    ?? { outletIndex: 0 };

  // Helper: call buffer/next and convert to OutletDetails format
  const discoverNextOutlet = async (): Promise<OutletDetails | null> => {
    const nextResult = await fetchNextOutlet({
      orgId: params.orgId,
      userId: params.userId ?? undefined,
      runId: params.pushRunId ?? undefined,
      campaignId: params.campaignId,
      brandId: brandIdCsv,
      workflowSlug: params.workflowSlug,
      featureSlug: params.featureSlug,
    });
    if (!nextResult.found || !nextResult.outlet) return null;
    const o = nextResult.outlet;
    return {
      id: o.outletId,
      outletName: o.outletName,
      outletUrl: o.outletUrl,
      outletDomain: o.outletDomain,
      campaignId: o.campaignId,
      relevanceScore: o.relevanceScore,
      whyRelevant: o.whyRelevant,
      whyNotRelevant: o.whyNotRelevant,
      overallRelevance: o.overallRelevance ?? "",
      outletStatus: "open",
    };
  };

  // Fetch outlets for this campaign
  let outlets = await fetchOutletsByCampaign(params.campaignId, params.orgId, serviceContext);
  if (!outlets || outlets.length === 0) {
    // No outlets yet — trigger auto-discovery via buffer/next on outlets-service
    console.log(`[fillBufferFromJournalists] No outlets for campaign=${params.campaignId}, triggering discovery via buffer/next...`);
    const discovered = await discoverNextOutlet();
    if (!discovered) {
      console.log(`[fillBufferFromJournalists] Outlet discovery returned 0 results for campaign=${params.campaignId}`);
      return { filled: 0 };
    }
    outlets = [discovered];
  }

  let totalFilled = 0;
  let oi = cursorState.outletIndex;

  // Outer loop: iterate through known outlets, discovering more when exhausted.
  // Keep going until outlet-service has no more outlets to offer.
  const MAX_DISCOVERY_ROUNDS = 50;
  let discoveryRounds = 0;

  while (true) {
    // Inner loop: iterate through known outlets starting from cursor
    for (; oi < outlets.length; oi++) {
      const outlet = outlets[oi];
      const organizationDomain = outlet.outletDomain || extractDomain(outlet.outletUrl);

      // Call journalists-service buffer/next in a loop for this outlet
      // journalists-service manages its own internal buffer per (campaign, outlet)
      const MAX_JOURNALIST_PULLS = 50;
      for (let pull = 0; pull < MAX_JOURNALIST_PULLS; pull++) {
        const result = await fetchNextJournalist(outlet.id, {
          ...serviceContext,
          campaignId: params.campaignId,
          orgId: params.orgId,
        });

        if (!result.found || !result.journalist) {
          // No more journalists for this outlet
          break;
        }

        const journalist = result.journalist;

        // Skip organization-type entities (we need individual people)
        if (journalist.entityType === "organization") continue;

        const externalId = journalist.id;

        if (await isInBuffer(params.orgId, params.campaignId, externalId)) continue;

        let validEmail: string | null = null;
        let enrichedData: Record<string, unknown> | null = null;

        // journalists-service does not return emails — match via Apollo Service
        if (journalist.firstName && journalist.lastName && organizationDomain) {
          const matchResult = await apolloMatch(
            {
              firstName: journalist.firstName,
              lastName: journalist.lastName,
              organizationDomain,
            },
            {
              runId: params.pushRunId,
              orgId: params.orgId,
              userId: params.userId,
              brandId: brandIdCsv,
              campaignId: params.campaignId,
              workflowSlug: params.workflowSlug,
              featureSlug: params.featureSlug,
            }
          );

          if (matchResult?.person?.email) {
            validEmail = matchResult.person.email;
            enrichedData = matchResult.person;

            // Cache enrichment for later lookups
            await db.insert(enrichments).values({
              email: validEmail,
              apolloPersonId: matchResult.person.id ?? null,
              firstName: matchResult.person.firstName ?? null,
              lastName: matchResult.person.lastName ?? null,
              title: matchResult.person.title ?? null,
              linkedinUrl: matchResult.person.linkedinUrl ?? null,
              organizationName: matchResult.person.organizationName ?? null,
              organizationDomain: matchResult.person.organizationDomain ?? null,
              organizationIndustry: matchResult.person.organizationIndustry ?? null,
              organizationSize: matchResult.person.organizationSize ?? null,
              responseRaw: matchResult.person,
            }).onConflictDoNothing();
          }
        }

        // No email after all enrichment attempts — skip, can't serve without email
        if (!validEmail) {
          continue;
        }

        const data: Record<string, unknown> = {
          firstName: journalist.firstName,
          lastName: journalist.lastName,
          journalistName: journalist.journalistName,
          organizationDomain,
          organizationName: outlet.outletName,
          outletUrl: outlet.outletUrl,
          outletId: outlet.id,
          journalistId: journalist.id,
          sourceType: "journalist",
          ...(enrichedData ?? {}),
        };

        await db.insert(leadBuffer).values({
          namespace: "journalist",
          campaignId: params.campaignId,
          email: validEmail,
          externalId,
          data,
          status: "buffered",
          pushRunId: params.pushRunId ?? null,
          brandIds: params.brandIds,
          orgId: params.orgId,
          userId: params.userId ?? null,
          workflowSlug: params.workflowSlug ?? null,
          featureSlug: params.featureSlug ?? null,
        });
        totalFilled++;
      }

      // Save cursor after each outlet
      if (totalFilled > 0) {
        await saveCursor(params.orgId, cursorNamespace, { outletIndex: oi + 1 });
        console.log(`[fillBufferFromJournalists] Buffered ${totalFilled} journalists from outlet ${outlet.outletName}`);
        return { filled: totalFilled };
      }
    }

    // All known outlets exhausted — ask outlet-service for more via buffer/next
    discoveryRounds++;
    if (discoveryRounds > MAX_DISCOVERY_ROUNDS) {
      console.warn(`[fillBufferFromJournalists] Hit MAX_DISCOVERY_ROUNDS (${MAX_DISCOVERY_ROUNDS}), stopping`);
      break;
    }

    console.log(`[fillBufferFromJournalists] All ${outlets.length} known outlets exhausted for campaign=${params.campaignId}, discovering more...`);

    const discovered = await discoverNextOutlet();
    if (!discovered) {
      console.log(`[fillBufferFromJournalists] Outlet-service has no more outlets for campaign=${params.campaignId}`);
      break;
    }

    // Append discovered outlet directly — don't re-fetch the full list
    console.log(`[fillBufferFromJournalists] Discovered new outlet "${discovered.outletName}" for campaign=${params.campaignId}`);
    outlets.push(discovered);
    // oi stays where it was — the for loop will pick up the new outlet
  }

  // All outlets truly exhausted
  await saveCursor(params.orgId, cursorNamespace, { outletIndex: outlets.length });
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
  sourceType: "apollo" | "journalist";
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
    journalistId: string | null;
    outletId: string | null;
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
          AND namespace = ${params.sourceType}
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
      // Buffer empty — fill from the appropriate source
      const st = params.sourceType;
      let filled = 0;

      if (st === "journalist") {
        const result = await fillBufferFromJournalists({
          orgId: params.orgId,
          campaignId: params.campaignId,
          brandIds: params.brandIds,
          pushRunId: params.runId,
          userId: params.userId,
          workflowSlug: params.workflowSlug,
          featureSlug: params.featureSlug,
        });
        filled = result.filled;
      } else {
        const result = await fillBufferFromSearch({
          orgId: params.orgId,
          campaignId: params.campaignId,
          brandIds: params.brandIds,
          pushRunId: params.runId,
          userId: params.userId,
          workflowSlug: params.workflowSlug,
          featureSlug: params.featureSlug,
        });
        filled = result.filled;
      }

      if (filled > 0) {
        continue; // Retry pulling from buffer
      }

      console.log(`[lead-service] pullNext found=false campaign=${params.campaignId} source=${st}`);
      return { found: false };
    }

    // Enrich if no email
    let email = row.email;
    let enrichedData = row.data;
    const rowData = row.data as Record<string, unknown> | null;
    const isJournalistLead = rowData?.sourceType === "journalist";

    if (!email && isJournalistLead && rowData?.firstName && rowData?.lastName && rowData?.organizationDomain) {
      // Journalist without email — try Apollo match by name + domain
      const matchResult = await apolloMatch(
        {
          firstName: rowData.firstName as string,
          lastName: rowData.lastName as string,
          organizationDomain: rowData.organizationDomain as string,
        },
        {
          runId: params.runId,
          orgId: params.orgId,
          userId: params.userId,
          brandId: brandIdCsv,
          campaignId: params.campaignId,
          workflowSlug: params.workflowSlug,
          featureSlug: params.featureSlug,
        }
      );

      if (!matchResult?.person?.email) {
        await db
          .update(leadBuffer)
          .set({ status: "skipped" })
          .where(eq(leadBuffer.id, row.id));
        continue;
      }

      email = matchResult.person.email;
      enrichedData = { ...(rowData ?? {}), ...matchResult.person };

      // Cache the enrichment result
      await db.insert(enrichments).values({
        email,
        apolloPersonId: matchResult.person.id ?? null,
        firstName: matchResult.person.firstName ?? null,
        lastName: matchResult.person.lastName ?? null,
        title: matchResult.person.title ?? null,
        linkedinUrl: matchResult.person.linkedinUrl ?? null,
        organizationName: matchResult.person.organizationName ?? null,
        organizationDomain: matchResult.person.organizationDomain ?? null,
        organizationIndustry: matchResult.person.organizationIndustry ?? null,
        organizationSize: matchResult.person.organizationSize ?? null,
        responseRaw: matchResult.person,
      }).onConflictDoNothing();

      // Update buffer row with email
      await db
        .update(leadBuffer)
        .set({ email, data: enrichedData })
        .where(eq(leadBuffer.id, row.id));
    } else if (!email && row.externalId) {
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

    // Check contacted status via email-gateway
    const contactedMap = await checkContacted(params.brandIds, params.campaignId, [
      { leadId, email },
    ], {
      orgId: params.orgId,
      userId: params.userId ?? undefined,
      runId: params.runId ?? undefined,
      campaignId: params.campaignId,
      brandId: brandIdCsv,
      workflowSlug: params.workflowSlug,
      featureSlug: params.featureSlug,
    });

    if (contactedMap.get(email)) {
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    const { inserted } = await markServed({
      orgId: params.orgId,
      namespace: params.sourceType,
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

    // Extract typed IDs from the data blob
    const dataObj = finalData as Record<string, unknown>;
    const isJournalist = dataObj.sourceType === "journalist";

    console.log(`[lead-service] pullNext found=true campaign=${params.campaignId} source=${params.sourceType} email=${email} leadId=${leadId}`);
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
        journalistId: isJournalist ? ((dataObj.journalistId as string) ?? null) : null,
        outletId: isJournalist ? ((dataObj.outletId as string) ?? null) : null,
      },
    };
  }
}
