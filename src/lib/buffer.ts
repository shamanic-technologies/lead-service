import { eq, and, sql } from "drizzle-orm";
import { db, sql as pgSql } from "../db/index.js";
import { leadBuffer, enrichments, cursors } from "../db/schema.js";
import { checkContacted, markServed } from "./dedup.js";
import { resolveOrCreateLead, findLeadByApolloPersonId } from "./leads-registry.js";
import { apolloSearchNext, apolloEnrich, apolloMatch, apolloSearchParams, type ApolloSearchParams } from "./apollo-client.js";
import { fetchCampaign } from "./campaign-client.js";
import { fetchBrand, fetchExtractedFields, extractBrandFields } from "./brand-client.js";
import { fetchOutletsByCampaign, discoverOutlets } from "./outlet-client.js";
import { fetchJournalistsByOutlet } from "./journalist-client.js";

async function isInBuffer(orgId: string, campaignId: string, externalId: string): Promise<boolean> {
  const row = await db.query.leadBuffer.findFirst({
    where: and(
      eq(leadBuffer.orgId, orgId),
      eq(leadBuffer.namespace, campaignId),
      eq(leadBuffer.externalId, externalId)
    ),
  });
  return !!row;
}

const MAX_PAGES = 50;

async function fillBufferFromSearch(params: {
  orgId: string;
  campaignId: string;
  brandId: string;
  searchParams: ApolloSearchParams;
  pushRunId?: string | null;
  userId?: string | null;
  workflowName?: string;
  featureSlug?: string;
}): Promise<{ filled: number }> {
  const serviceContext = {
    userId: params.userId ?? undefined,
    runId: params.pushRunId ?? undefined,
    campaignId: params.campaignId,
    brandId: params.brandId,
    workflowName: params.workflowName,
    featureSlug: params.featureSlug,
  };

  // Fetch campaign + brand details in parallel for rich LLM context
  const [campaign, brand] = await Promise.all([
    fetchCampaign(params.campaignId, params.orgId, serviceContext),
    fetchBrand(params.brandId, params.orgId, serviceContext),
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
    runId: params.pushRunId ?? "",
    brandId: params.brandId,
    campaignId: params.campaignId,
    orgId: params.orgId,
    userId: params.userId,
    workflowName: params.workflowName,
    featureSlug: params.featureSlug,
  });

  let totalFilled = 0;

  // Call apolloSearchNext in a loop — apollo-service manages pagination server-side.
  for (let page = 1; page <= MAX_PAGES; page++) {
    const result = await apolloSearchNext({
      campaignId: params.campaignId,
      brandId: params.brandId,
      searchParams: validatedParams,
      runId: params.pushRunId,
      orgId: params.orgId,
      userId: params.userId,
      workflowName: params.workflowName,
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
        ? await checkContacted(params.brandId, params.campaignId, itemsWithEmails, {
            orgId: params.orgId,
            userId: params.userId ?? undefined,
            runId: params.pushRunId ?? undefined,
            campaignId: params.campaignId,
            brandId: params.brandId,
            workflowName: params.workflowName,
            featureSlug: params.featureSlug,
          })
        : new Map<string, boolean>();

    let pageFilled = 0;

    for (const { data, externalId, email } of candidates) {
      if (email && contactedMap.get(email)) continue;

      await db.insert(leadBuffer).values({
        namespace: params.campaignId,
        campaignId: params.campaignId,
        email: email ?? "",
        externalId: externalId,
        data,
        status: "buffered",
        pushRunId: params.pushRunId ?? null,
        brandId: params.brandId,
        orgId: params.orgId,
        userId: params.userId ?? null,
        workflowName: params.workflowName ?? null,
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

const DISCOVERY_FIELDS = [
  { key: "elevator_pitch", description: "A one-sentence pitch describing what the brand does" },
  { key: "categories", description: "Comma-separated industry categories the brand operates in" },
  { key: "target_geo", description: "Primary geographic market the brand targets" },
];

interface BrandContext {
  brandName: string | null;
  brandDescription: string | null;
  industry: string | null;
  targetGeo: string | null;
}

async function resolveBrandContext(
  brandId: string,
  orgId: string,
  serviceContext: { userId?: string; runId?: string; campaignId?: string; brandId?: string; workflowName?: string; featureSlug?: string },
): Promise<BrandContext> {
  const empty: BrandContext = { brandName: null, brandDescription: null, industry: null, targetGeo: null };

  // 1. Check cached extracted fields first (fast, no AI cost)
  const cached = await fetchExtractedFields(brandId, orgId, serviceContext);
  if (cached && cached.length > 0) {
    const fieldMap = new Map(cached.map(f => [f.key, f.value]));
    const pitch = fieldMap.get("elevator_pitch");
    const cats = fieldMap.get("categories");
    const geo = fieldMap.get("target_geo");

    // If we have the key fields cached, return them directly
    if (pitch && cats) {
      console.log(`[resolveBrandContext] Using cached extracted fields for brand=${brandId}`);
      return {
        brandName: null, // will be filled from brand.name by caller
        brandDescription: typeof pitch === "string" ? pitch : JSON.stringify(pitch),
        industry: typeof cats === "string" ? cats : Array.isArray(cats) ? cats.join(", ") : JSON.stringify(cats),
        targetGeo: geo ? (typeof geo === "string" ? geo : JSON.stringify(geo)) : null,
      };
    }
  }

  // 2. Extract fields via AI (slow, but results get cached for 30 days)
  console.log(`[resolveBrandContext] Extracting fields via AI for brand=${brandId}`);
  const results = await extractBrandFields(brandId, DISCOVERY_FIELDS, orgId, serviceContext);
  if (!results) return empty;

  const fieldMap = new Map(results.map(f => [f.key, f.value]));
  const pitch = fieldMap.get("elevator_pitch");
  const cats = fieldMap.get("categories");
  const geo = fieldMap.get("target_geo");

  return {
    brandName: null,
    brandDescription: pitch ? (typeof pitch === "string" ? pitch : JSON.stringify(pitch)) : null,
    industry: cats ? (typeof cats === "string" ? cats : Array.isArray(cats) ? cats.join(", ") : JSON.stringify(cats)) : null,
    targetGeo: geo ? (typeof geo === "string" ? geo : JSON.stringify(geo)) : null,
  };
}

async function fillBufferFromJournalists(params: {
  orgId: string;
  campaignId: string;
  brandId: string;
  brandContext?: Record<string, unknown>;
  pushRunId?: string | null;
  userId?: string | null;
  workflowName?: string;
  featureSlug?: string;
}): Promise<{ filled: number }> {
  const serviceContext = {
    userId: params.userId ?? undefined,
    runId: params.pushRunId ?? undefined,
    campaignId: params.campaignId,
    brandId: params.brandId,
    workflowName: params.workflowName,
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
  const cursorState = (existingCursor?.state as { outletIndex: number; journalistIndex: number } | null)
    ?? { outletIndex: 0, journalistIndex: 0 };

  // Fetch outlets for this campaign
  let outlets = await fetchOutletsByCampaign(params.campaignId, params.orgId, serviceContext);
  if (!outlets || outlets.length === 0) {
    // No outlets yet — discover them via outlets-service
    console.log(`[fillBufferFromJournalists] No outlets for campaign=${params.campaignId}, discovering...`);

    const [campaign, brand] = await Promise.all([
      fetchCampaign(params.campaignId, params.orgId, serviceContext),
      fetchBrand(params.brandId, params.orgId, serviceContext),
    ]);

    // Resolve brand context: extract-fields (primary) → brandContext (DAG) → legacy brand columns (fallback)
    const extracted = await resolveBrandContext(params.brandId, params.orgId, serviceContext);
    const ctx = params.brandContext ?? {};

    const brandName = extracted.brandName ?? (ctx.brandName as string) ?? (ctx.companyName as string) ?? brand?.name ?? null;
    const brandDescription = extracted.brandDescription ?? (ctx.companyContext as string) ?? (ctx.brandDescription as string) ?? brand?.elevatorPitch ?? brand?.bio ?? null;
    const industry = extracted.industry ?? (ctx.industry as string) ?? (ctx.categories as string) ?? brand?.categories ?? null;
    const targetGeo = extracted.targetGeo ?? (ctx.targetGeo as string) ?? brand?.location ?? undefined;
    const targetAudience = (ctx.targetAudience as string) ?? campaign?.targetAudience ?? undefined;

    if (!brandName || !brandDescription || !industry) {
      console.warn(`[fillBufferFromJournalists] Cannot discover outlets — insufficient context (need brandName, brandDescription, industry). extracted=${JSON.stringify(extracted)}, brandContext=${JSON.stringify(ctx)}, brand=${JSON.stringify(brand ? { name: brand.name, elevatorPitch: brand.elevatorPitch, categories: brand.categories } : null)}`);
      return { filled: 0 };
    }

    const discovered = await discoverOutlets(
      {
        campaignId: params.campaignId,
        brandId: params.brandId,
        brandName,
        brandDescription,
        industry,
        targetGeo,
        targetAudience,
        workflowName: params.workflowName,
      },
      {
        orgId: params.orgId,
        userId: params.userId ?? undefined,
        runId: params.pushRunId ?? undefined,
        featureSlug: params.featureSlug,
      }
    );

    if (!discovered || discovered.length === 0) {
      console.log(`[fillBufferFromJournalists] Outlet discovery returned 0 results for campaign=${params.campaignId}`);
      return { filled: 0 };
    }

    // Re-fetch outlets now that discovery has saved them
    outlets = await fetchOutletsByCampaign(params.campaignId, params.orgId, serviceContext);
    if (!outlets || outlets.length === 0) {
      console.warn(`[fillBufferFromJournalists] Outlets still empty after discovery for campaign=${params.campaignId}`);
      return { filled: 0 };
    }
  }

  let totalFilled = 0;

  for (let oi = cursorState.outletIndex; oi < outlets.length; oi++) {
    const outlet = outlets[oi];

    const journalists = await fetchJournalistsByOutlet(outlet.id, {
      ...serviceContext,
      campaignId: params.campaignId,
      orgId: params.orgId,
    });

    if (!journalists || journalists.length === 0) {
      // No journalists for this outlet — advance to next
      continue;
    }

    const organizationDomain = outlet.outletDomain || extractDomain(outlet.outletUrl);

    const startJ = oi === cursorState.outletIndex ? cursorState.journalistIndex : 0;

    for (let ji = startJ; ji < journalists.length; ji++) {
      const journalist = journalists[ji];

      // Skip organization-type entities (we need individual people)
      if (journalist.entityType === "organization") continue;

      const externalId = `journalist:${journalist.id}`;

      if (await isInBuffer(params.orgId, params.campaignId, externalId)) continue;

      // Try email from resolve response first
      let validEmail = journalist.emails
        ?.filter(e => e.isValid)
        ?.sort((a, b) => b.confidence - a.confidence)
        ?.[0]?.email ?? null;

      let enrichedData: Record<string, unknown> | null = null;

      // No email from resolve — proactively match via Apollo Service
      if (!validEmail && journalist.firstName && journalist.lastName && organizationDomain) {
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
            brandId: params.brandId,
            campaignId: params.campaignId,
            workflowName: params.workflowName,
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
        namespace: params.campaignId,
        campaignId: params.campaignId,
        email: validEmail ?? "",
        externalId,
        data,
        status: "buffered",
        pushRunId: params.pushRunId ?? null,
        brandId: params.brandId,
        orgId: params.orgId,
        userId: params.userId ?? null,
        workflowName: params.workflowName ?? null,
        featureSlug: params.featureSlug ?? null,
      });
      totalFilled++;
    }

    // Save cursor after each outlet
    if (totalFilled > 0) {
      await saveCursor(params.orgId, cursorNamespace, { outletIndex: oi + 1, journalistIndex: 0 });
      console.log(`[fillBufferFromJournalists] Buffered ${totalFilled} journalists from outlet ${outlet.outletName}`);
      return { filled: totalFilled };
    }
  }

  // All outlets exhausted
  await saveCursor(params.orgId, cursorNamespace, { outletIndex: outlets.length, journalistIndex: 0 });
  return { filled: totalFilled };
}

export async function pullNext(params: {
  orgId: string;
  campaignId: string;
  brandId: string;
  runId?: string | null;
  searchParams?: ApolloSearchParams;
  brandContext?: Record<string, unknown>;
  userId?: string | null;
  workflowName?: string;
  featureSlug?: string;
  sourceType?: "apollo" | "journalist";
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
    // Use FOR UPDATE SKIP LOCKED to atomically claim a buffer row,
    // preventing concurrent pullNext calls from picking the same lead.
    const claimedRows = await pgSql`
      UPDATE lead_buffer
      SET status = 'claimed'
      WHERE id = (
        SELECT id FROM lead_buffer
        WHERE org_id = ${params.orgId}
          AND namespace = ${params.campaignId}
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
      brandId: claimedRows[0].brand_id as string | null,
      orgId: claimedRows[0].org_id as string,
      userId: claimedRows[0].user_id as string | null,
      createdAt: claimedRows[0].created_at as Date,
    } : null;

    if (!row) {
      // Buffer empty — fill from the appropriate source
      const st = params.sourceType ?? "apollo";
      let filled = 0;

      if (st === "journalist") {
        const result = await fillBufferFromJournalists({
          orgId: params.orgId,
          campaignId: params.campaignId,
          brandId: params.brandId,
          brandContext: params.brandContext,
          pushRunId: params.runId,
          userId: params.userId,
          workflowName: params.workflowName,
          featureSlug: params.featureSlug,
        });
        filled = result.filled;
      } else if (params.searchParams) {
        const result = await fillBufferFromSearch({
          orgId: params.orgId,
          campaignId: params.campaignId,
          brandId: params.brandId,
          searchParams: params.searchParams,
          pushRunId: params.runId,
          userId: params.userId,
          workflowName: params.workflowName,
          featureSlug: params.featureSlug,
        });
        filled = result.filled;
      }

      if (filled > 0) {
        continue; // Retry pulling from buffer
      }

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
          brandId: params.brandId,
          campaignId: params.campaignId,
          workflowName: params.workflowName,
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
          brandId: params.brandId,
          campaignId: params.campaignId,
          workflowName: params.workflowName,
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
    const contactedMap = await checkContacted(params.brandId, params.campaignId, [
      { leadId, email },
    ], {
      orgId: params.orgId,
      userId: params.userId ?? undefined,
      runId: params.runId ?? undefined,
      campaignId: params.campaignId,
      brandId: params.brandId,
      workflowName: params.workflowName,
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
      namespace: params.campaignId,
      brandId: params.brandId,
      campaignId: params.campaignId,
      email,
      leadId,
      externalId: row.externalId,
      metadata: enrichedData,
      runId: params.runId ?? null,
      userId: row.userId,
      workflowName: params.workflowName ?? null,
      featureSlug: params.featureSlug ?? null,
    });

    if (!inserted) {
      // Another request already served this email for this org+brand — skip
      // Another concurrent request already served this email — skip
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
