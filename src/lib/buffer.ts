import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import { leadBuffer, cursors } from "../db/schema.js";
import { isServed, markServed } from "./dedup.js";
import { apolloSearch, type ApolloSearchParams } from "./apollo-client.js";
import { transformSearchParams } from "./search-transform.js";

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

  for (const lead of params.leads) {
    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      lead.email
    );

    if (alreadyServed) {
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

  if (cursor.exhausted) {
    return { filled: 0, exhausted: true };
  }

  // Transform + validate search params via LLM → Apollo /validate loop
  const validatedParams = await transformSearchParams(
    params.searchParams as Record<string, unknown>,
    params.clerkOrgId,
    params.pushRunId
  );

  const result = await apolloSearch(validatedParams, cursor.page);

  if (!result || result.people.length === 0) {
    await setCursor(params.organizationId, params.namespace, { page: cursor.page, exhausted: true });
    return { filled: 0, exhausted: true };
  }

  let filled = 0;
  for (const person of result.people) {
    if (!person.email) continue;

    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      person.email
    );

    if (alreadyServed) continue;

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
  while (true) {
    const row = await db.query.leadBuffer.findFirst({
      where: and(
        eq(leadBuffer.organizationId, params.organizationId),
        eq(leadBuffer.namespace, params.namespace),
        eq(leadBuffer.status, "buffered")
      ),
    });

    if (!row) {
      // Buffer empty - try to fill from search if searchParams provided
      if (params.searchParams) {
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
          continue; // Retry pulling from buffer
        }

        if (exhausted) {
          return { found: false };
        }

        // No results but not exhausted - keep trying next page
        continue;
      }

      return { found: false };
    }

    const alreadyServed = await isServed(
      params.organizationId,
      params.namespace,
      row.email
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
