import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import { leadBuffer, cursors } from "../db/schema.js";
import { isServed, markServed } from "./dedup.js";
import { apolloSearch, type ApolloSearchParams } from "./apollo-client.js";

export async function pushLeads(params: {
  organizationId: string;
  namespace: string;
  pushRunId?: string | null;
  brandId?: string | null;
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

  console.log(`[pushLeads] Processing ${params.leads.length} leads for org=${params.organizationId} ns=${params.namespace}`);

  for (const lead of params.leads) {
    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      lead.email
    );

    if (alreadyServed) {
      console.log(`[pushLeads] Skipped (already served): ${lead.email}`);
      skippedAlreadyServed++;
      continue;
    }

    await db.insert(leadBuffer).values({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: lead.email,
      externalId: lead.externalId ?? null,
      data: lead.data ?? null,
      status: "buffered",
      pushRunId: params.pushRunId ?? null,
      brandId: params.brandId ?? null,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    });
    buffered++;
  }

  console.log(`[pushLeads] Done: buffered=${buffered} skipped=${skippedAlreadyServed}`);
  return { buffered, skippedAlreadyServed };
}

interface CursorState {
  page: number;
  exhausted: boolean;
}

async function getCursor(organizationId: string, namespace: string): Promise<CursorState> {
  const cursor = await db.query.cursors.findFirst({
    where: and(
      eq(cursors.organizationId, organizationId),
      eq(cursors.namespace, namespace)
    ),
  });
  return (cursor?.state as CursorState) ?? { page: 1, exhausted: false };
}

async function setCursor(organizationId: string, namespace: string, state: CursorState): Promise<void> {
  const existing = await db.query.cursors.findFirst({
    where: and(
      eq(cursors.organizationId, organizationId),
      eq(cursors.namespace, namespace)
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
      namespace,
      state,
    });
  }
}

async function fillBufferFromSearch(params: {
  organizationId: string;
  namespace: string;
  searchParams: ApolloSearchParams;
  pushRunId?: string | null;
  brandId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
}): Promise<{ filled: number; exhausted: boolean }> {
  const cursor = await getCursor(params.organizationId, params.namespace);

  console.log(`[fillBuffer] org=${params.organizationId} ns=${params.namespace} cursor={page:${cursor.page}, exhausted:${cursor.exhausted}}`);

  if (cursor.exhausted) {
    console.log("[fillBuffer] Cursor already exhausted, returning 0");
    return { filled: 0, exhausted: true };
  }

  console.log(`[fillBuffer] Calling Apollo search page=${cursor.page} params=${JSON.stringify(params.searchParams)}`);
  const result = await apolloSearch(params.searchParams, cursor.page);

  if (!result) {
    console.log("[fillBuffer] Apollo returned null (search failed or network error)");
    await setCursor(params.organizationId, params.namespace, { page: cursor.page, exhausted: true });
    return { filled: 0, exhausted: true };
  }

  if (result.people.length === 0) {
    console.log(`[fillBuffer] Apollo returned 0 people (page=${cursor.page} totalPages=${result.pagination.totalPages} totalEntries=${result.pagination.totalEntries})`);
    await setCursor(params.organizationId, params.namespace, { page: cursor.page, exhausted: true });
    return { filled: 0, exhausted: true };
  }

  console.log(`[fillBuffer] Apollo returned ${result.people.length} people (page=${cursor.page}/${result.pagination.totalPages}, total=${result.pagination.totalEntries})`);

  let filled = 0;
  let skippedNoEmail = 0;
  let skippedAlreadyServed = 0;

  for (const person of result.people) {
    if (!person.email) {
      skippedNoEmail++;
      continue;
    }

    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      person.email
    );

    if (alreadyServed) {
      skippedAlreadyServed++;
      continue;
    }

    await db.insert(leadBuffer).values({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: person.email,
      externalId: person.id,
      data: person,
      status: "buffered",
      pushRunId: params.pushRunId ?? null,
      brandId: params.brandId ?? null,
      clerkOrgId: params.clerkOrgId ?? null,
      clerkUserId: params.clerkUserId ?? null,
    });
    filled++;
  }

  const isExhausted = cursor.page >= result.pagination.totalPages;
  await setCursor(params.organizationId, params.namespace, {
    page: cursor.page + 1,
    exhausted: isExhausted,
  });

  console.log(`[fillBuffer] Done: filled=${filled} skippedNoEmail=${skippedNoEmail} skippedAlreadyServed=${skippedAlreadyServed} exhausted=${isExhausted}`);

  return { filled, exhausted: isExhausted && filled === 0 };
}

export async function pullNext(params: {
  organizationId: string;
  namespace: string;
  parentRunId?: string | null;
  runId?: string | null;
  searchParams?: ApolloSearchParams;
  brandId?: string | null;
  clerkOrgId?: string | null;
  clerkUserId?: string | null;
}): Promise<{
  found: boolean;
  lead?: {
    email: string;
    externalId: string | null;
    data: unknown;
    brandId: string | null;
    clerkOrgId: string | null;
    clerkUserId: string | null;
  };
}> {
  console.log(`[pullNext] Called for org=${params.organizationId} ns=${params.namespace} hasSearchParams=${!!params.searchParams}`);

  let iterations = 0;
  while (true) {
    iterations++;
    const row = await db.query.leadBuffer.findFirst({
      where: and(
        eq(leadBuffer.organizationId, params.organizationId),
        eq(leadBuffer.namespace, params.namespace),
        eq(leadBuffer.status, "buffered")
      ),
    });

    if (!row) {
      console.log(`[pullNext] Buffer empty (iteration ${iterations})`);
      // Buffer empty - try to fill from search if searchParams provided
      if (params.searchParams) {
        console.log("[pullNext] Attempting to fill buffer from Apollo search...");
        const { filled, exhausted } = await fillBufferFromSearch({
          organizationId: params.organizationId,
          namespace: params.namespace,
          searchParams: params.searchParams,
          pushRunId: params.runId,
          brandId: params.brandId,
          clerkOrgId: params.clerkOrgId,
          clerkUserId: params.clerkUserId,
        });

        if (filled > 0) {
          console.log(`[pullNext] Buffer filled with ${filled} leads, retrying pull`);
          continue; // Retry pulling from buffer
        }

        if (exhausted) {
          console.log("[pullNext] Search exhausted, no more leads available -> found=false");
          return { found: false };
        }

        // No results but not exhausted - keep trying next page
        console.log("[pullNext] Page had no usable results but not exhausted, trying next page");
        continue;
      }

      console.log("[pullNext] No searchParams provided and buffer empty -> found=false");
      return { found: false };
    }

    console.log(`[pullNext] Found buffered lead: ${row.email} (iteration ${iterations})`);

    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      row.email
    );

    if (alreadyServed) {
      console.log(`[pullNext] Lead ${row.email} already served, skipping`);
      await db
        .update(leadBuffer)
        .set({ status: "skipped" })
        .where(eq(leadBuffer.id, row.id));
      continue;
    }

    await markServed({
      organizationId: params.organizationId,
      namespace: params.namespace,
      email: row.email,
      externalId: row.externalId,
      metadata: row.data,
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      brandId: row.brandId,
      clerkOrgId: row.clerkOrgId,
      clerkUserId: row.clerkUserId,
    });

    await db
      .update(leadBuffer)
      .set({ status: "served" })
      .where(eq(leadBuffer.id, row.id));

    console.log(`[pullNext] Serving lead: ${row.email} (after ${iterations} iterations)`);

    return {
      found: true,
      lead: {
        email: row.email,
        externalId: row.externalId,
        data: row.data,
        brandId: row.brandId,
        clerkOrgId: row.clerkOrgId,
        clerkUserId: row.clerkUserId,
      },
    };
  }
}
