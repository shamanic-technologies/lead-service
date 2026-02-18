import { eq, and, sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { leadBuffer, cursors } from "../db/schema.js";
import { isServed, markServed } from "./dedup.js";
import { apolloSearch, apolloEnrich, type ApolloSearchParams } from "./apollo-client.js";
import { transformSearchParams } from "./search-transform.js";

export async function pushLeads(params: {
  organizationId: string;
  campaignId: string;
  brandId: string;
  pushRunId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
  leads: Array<{
    email: string;
    externalId?: string | null;
    data?: unknown;
  }>;
}): Promise<{ buffered: number; skippedAlreadyServed: number }> {
  let buffered = 0;
  let skippedAlreadyServed = 0;

  for (const lead of params.leads) {
    const alreadyServed = await isServed(
      params.organizationId,
      params.brandId,
      lead.email
    );

    if (alreadyServed) {
      skippedAlreadyServed++;
      continue;
    }

    await db.insert(leadBuffer).values({
      organizationId: params.organizationId,
      namespace: params.campaignId,
      campaignId: params.campaignId,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: params.pushRunId ?? null,
      brandId: params.brandId,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    });
    buffered++;
  }

  console.log(`[pushLeads] buffered=${buffered} skipped=${skippedAlreadyServed}`);
  return { buffered, skippedAlreadyServed };
}

interface CursorState {
  page: number;
  exhausted: boolean;
}

async function getCursor(organizationId: string, campaignId: string): Promise<CursorState> {
  const cursor = await db.query.cursors.findFirst({
    where: and(
      eq(cursors.organizationId, organizationId),
      eq(cursors.namespace, campaignId)
    ),
  });
  return (cursor?.state as CursorState) ?? { page: 1, exhausted: false };
}

async function setCursor(organizationId: string, campaignId: string, state: CursorState): Promise<void> {
  const existing = await db.query.cursors.findFirst({
    where: and(
      eq(cursors.organizationId, organizationId),
      eq(cursors.namespace, campaignId)
    ),
  });

  if (existing) {
    await db
      .update(cursors)
      .set({ state, updatedAt: new Date() })
      .where(eq(cursors.id, existing.id));
  } else {
    await db.insert(cursors).values({
      organizationId,
      namespace: campaignId,
      state,
    });
  }
}

async function fillBufferFromSearch(params: {
  organizationId: string;
  campaignId: string;
  brandId: string;
  searchParams: ApolloSearchParams;
  pushRunId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
  appId?: string;
}): Promise<{ filled: number; exhausted: boolean }> {
  const cursor = await getCursor(params.organizationId, params.campaignId);

  if (cursor.exhausted) {
    return { filled: 0, exhausted: true };
  }

  // Transform + validate search params via LLM → Apollo /validate loop
  const validatedParams = await transformSearchParams(
    params.searchParams as Record<string, unknown>,
    params.clerkOrgId,
    params.pushRunId
  );

  const result = await apolloSearch(validatedParams, cursor.page, {
    runId: params.pushRunId,
    clerkOrgId: params.clerkOrgId,
    appId: params.appId,
    brandId: params.brandId,
    campaignId: params.campaignId,
  });

  if (!result) {
    console.warn("[fillBuffer] Apollo returned null (search failed or network error)");
    await setCursor(params.organizationId, params.campaignId, { page: cursor.page, exhausted: true });
    return { filled: 0, exhausted: true };
  }

  if (result.people.length === 0) {
    await setCursor(params.organizationId, params.campaignId, { page: cursor.page, exhausted: true });
    return { filled: 0, exhausted: true };
  }

  let filled = 0;
  let skippedAlreadyServed = 0;

  for (const person of result.people) {
    // If person has email, check dedup early; otherwise defer to enrichment in pullNext
    if (person.email) {
      const alreadyServed = await isServed(
        params.organizationId,
        params.brandId,
        person.email
      );

      if (alreadyServed) {
        skippedAlreadyServed++;
        continue;
      }
    }

    await db.insert(leadBuffer).values({
      organizationId: params.organizationId,
      namespace: params.campaignId,
      campaignId: params.campaignId,
      email: person.email ?? "",
      externalId: person.id,
      data: person,
      status: "buffered",
      pushRunId: params.pushRunId ?? null,
      brandId: params.brandId,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    });
    filled++;
  }

  const isExhausted = cursor.page >= result.pagination.totalPages;
  await setCursor(params.organizationId, params.campaignId, {
    page: cursor.page + 1,
    exhausted: isExhausted,
  });

  return { filled, exhausted: isExhausted && filled === 0 };
}

export async function pullNext(params: {
  organizationId: string;
  campaignId: string;
  brandId: string;
  parentRunId?: string | null;
  runId?: string | null;
  searchParams?: ApolloSearchParams;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
  appId?: string;
}): Promise<{
  found: boolean;
  lead?: {
    email: string;
    externalId: string | null;
    data: unknown;
    brandId: string;
    clerkOrgId: string | null;
    clerkUserId: string | null;
  };
}> {
  const MAX_EMPTY_PAGES = 10;
  const MAX_ITERATIONS = 100;
  let iterations = 0;
  let emptyPages = 0;
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
      if (params.searchParams) {
        const { filled, exhausted } = await fillBufferFromSearch({
          organizationId: params.organizationId,
          campaignId: params.campaignId,
          brandId: params.brandId,
          searchParams: params.searchParams,
          pushRunId: params.runId,
          clerkOrgId: params.clerkOrgId,
          clerkUserId: params.clerkUserId,
          appId: params.appId,
        });

        if (filled > 0) {
          emptyPages = 0;
          continue; // Retry pulling from buffer
        }

        if (exhausted) {
          return { found: false };
        }

        emptyPages++;
        if (emptyPages >= MAX_EMPTY_PAGES) {
          console.warn(`[pullNext] Gave up after ${emptyPages} consecutive empty pages`);
          return { found: false };
        }

        continue;
      }

      return { found: false };
    }

    // Enrich if no email (search results don't include emails)
    let email = row.email;
    let enrichedData = row.data;
    if (!email && row.externalId) {
      const enrichResult = await apolloEnrich(row.externalId, {
        runId: params.runId,
        clerkOrgId: params.clerkOrgId,
        appId: params.appId,
        brandId: params.brandId,
        campaignId: params.campaignId,
      });

      if (!enrichResult?.person?.email) {
        console.warn(`[pullNext] Enrichment returned no email for personId=${row.externalId}`);
        await db
          .update(leadBuffer)
          .set({ status: "skipped" })
          .where(eq(leadBuffer.id, row.id));
        continue;
      }

      email = enrichResult.person.email;
      enrichedData = { ...(row.data as object ?? {}), ...enrichResult.person };

      // Update buffer row with enriched email
      await db
        .update(leadBuffer)
        .set({ email, data: enrichedData })
        .where(eq(leadBuffer.id, row.id));
    }

    const alreadyServed = await isServed(
      params.organizationId,
      params.brandId,
      email
    );

    if (alreadyServed) {
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
      externalId: row.externalId,
      metadata: enrichedData,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      clerkOrgId: row.clerkOrgId,
      clerkUserId: row.clerkUserId,
    });

    await db
      .update(leadBuffer)
      .set({ status: "served" })
      .where(eq(leadBuffer.id, row.id));

    console.log(`[pullNext] Served lead: ${email}`);

    return {
      found: true,
      lead: {
        email,
        externalId: row.externalId,
        data: enrichedData,
        brandId: params.brandId,
        clerkOrgId: row.clerkOrgId,
        clerkUserId: row.clerkUserId,
      },
    };
  }
}
