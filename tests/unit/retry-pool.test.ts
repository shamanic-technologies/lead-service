import { beforeEach, describe, expect, it, vi } from "vitest";

/**
 * The paid-pool retry: drain people this campaign already bought and never contacted
 * before asking human-service for someone new.
 *
 * The db mock is a small state machine over real rows rather than a canned response,
 * because the two things worth proving here — that a `sent` person leaves the pool for
 * good, and that two concurrent pulls cannot take the same person — are properties of
 * the conditional UPDATE, not of the returned array. It is also Bind-faithful: a raw
 * `Date` param throws exactly as postgres.js does at Bind time, so a handler that binds
 * one cannot ship green (see CLAUDE.md).
 */

interface Row {
  id: string;
  lead_id: string;
  email: string | null;
  served_at: string | null;
  audience_id: string | null;
  goal: string | null;
  retry_count: number;
  sent_at: string | null;
  retry_claimed_at: string | null;
  status: string;
}

let rows: Row[] = [];
let executed: string[] = [];

interface Chunk {
  value?: string[];
}

/** Flatten a drizzle SQL object into its text plus its params, each with its preceding text. */
function readQuery(query: { queryChunks: unknown[] }): {
  text: string;
  params: Array<{ before: string; value: unknown }>;
} {
  let text = "";
  const params: Array<{ before: string; value: unknown }> = [];
  for (const chunk of query.queryChunks) {
    const asChunk = chunk as Chunk;
    if (asChunk && typeof asChunk === "object" && Array.isArray(asChunk.value)) {
      text += asChunk.value.join("");
      continue;
    }
    if (chunk instanceof Date) {
      throw new TypeError(
        'The "string" argument must be of type string or an instance of Buffer or ArrayBuffer. Received an instance of Date',
      );
    }
    params.push({ before: text, value: chunk });
  }
  return { text, params };
}

function paramAfter(
  params: Array<{ before: string; value: unknown }>,
  pattern: RegExp,
): unknown {
  const hit = params.find((p) => pattern.test(p.before));
  return hit?.value;
}

function execute(query: { queryChunks: unknown[] }): Promise<unknown[]> {
  const { text, params } = readQuery(query);
  executed.push(text.replace(/\s+/g, " ").trim());

  if (/^\s*UPDATE leads_campaigns\s+SET sent_at/.test(text)) {
    const now = paramAfter(params, /SET sent_at = $/) as string;
    const ids = paramAfter(params, /WHERE id::text = ANY\($/) as string[];
    for (const row of rows) {
      if (ids.includes(row.id) && row.sent_at === null) row.sent_at = now;
    }
    return Promise.resolve([]);
  }

  if (/retry_claimed_at =/.test(text) && /^\s*UPDATE leads_campaigns/.test(text)) {
    const now = paramAfter(params, /SET\s+retry_claimed_at = $/) as string;
    const id = paramAfter(params, /WHERE id = $/) as string;
    const leaseCutoff = paramAfter(params, /retry_claimed_at < $/) as string;
    const row = rows.find((r) => r.id === id);
    if (
      !row ||
      row.status !== "served" ||
      row.sent_at !== null ||
      (row.retry_claimed_at !== null && row.retry_claimed_at >= leaseCutoff)
    ) {
      return Promise.resolve([]);
    }
    row.retry_claimed_at = now;
    row.retry_count += 1;
    return Promise.resolve([{ id: row.id }]);
  }

  // The candidate read.
  const campaignId = paramAfter(params, /lc\.campaign_id = $/) as string;
  const leaseCutoff = paramAfter(params, /lc\.retry_claimed_at < $/) as string;
  const limit = paramAfter(params, /LIMIT $/) as number;
  const eligible = rows
    .filter(
      (r) =>
        r.status === "served" &&
        r.sent_at === null &&
        (r.retry_claimed_at === null || r.retry_claimed_at < leaseCutoff),
    )
    .sort((a, b) =>
      (a.retry_claimed_at ?? a.served_at ?? "").localeCompare(b.retry_claimed_at ?? b.served_at ?? ""),
    )
    .slice(0, limit)
    .map((r) => ({
      id: r.id,
      lead_id: r.lead_id,
      email: r.email,
      served_at: r.served_at,
      audience_id: r.audience_id,
      goal: r.goal,
      retry_count: r.retry_count,
      __campaign: campaignId,
    }));
  return Promise.resolve(eligible);
}

const markSentUpdates: { ids: unknown; set: unknown }[] = [];

// `update` is a builder chain, so the mock has to be one too. Note what that means: this
// double cannot fail the way a real database does — the statement it stands in for shipped
// broken past a green run of this very file. The statements themselves are exercised in
// tests/integration/retry-pool-sql.test.ts, against a real database; keep them there.
vi.mock("../../src/db/index.js", () => ({
  db: {
    execute: (q: { queryChunks: unknown[] }) => execute(q),
    update: () => ({
      set: (values: unknown) => ({
        where: async (predicate: unknown) => {
          markSentUpdates.push({ ids: predicate, set: values });
        },
      }),
    }),
  },
}));

const checkDeliveryStatus = vi.fn();
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (...args: unknown[]) => checkDeliveryStatus(...args),
}));

import {
  campaignScopeFlags,
  decideRetry,
  pickRetryCandidate,
  RETRY_QUEUE_TTL_MS,
} from "../../src/lib/retry-pool.js";

const NOW = Date.parse("2026-08-25T12:00:00.000Z");
const CAMPAIGN = "campaign-1";

function scoped(over: Partial<{ contacted: boolean; sent: boolean }>) {
  return {
    contacted: false,
    sent: false,
    delivered: false,
    opened: false,
    clicked: false,
    replied: false,
    replyClassification: null,
    bounced: false,
    unsubscribed: false,
    lastDeliveredAt: null,
    firstContactedAt: null,
    firstSentAt: null,
    firstDeliveredAt: null,
    firstOpenedAt: null,
    firstClickedAt: null,
    firstRepliedAt: null,
    firstBouncedAt: null,
    firstUnsubscribedAt: null,
    ...over,
  };
}

function row(over: Partial<Row> & { id: string }): Row {
  return {
    lead_id: `lead-${over.id}`,
    email: `${over.id}@example.com`,
    served_at: "2026-08-25T09:00:00.000Z",
    audience_id: "aud-1",
    goal: "signup",
    retry_count: 0,
    sent_at: null,
    retry_claimed_at: null,
    status: "served",
    ...over,
  };
}

const base = {
  orgId: "org-1",
  campaignId: CAMPAIGN,
  brandId: "brand-1",
  runId: "run-2",
  parentRunId: "run-1",
  context: { orgId: "org-1", campaignId: CAMPAIGN, brandId: "brand-1" },
  nowMs: NOW,
};

beforeEach(() => {
  rows = [];
  executed = [];
  vi.resetAllMocks();
  checkDeliveryStatus.mockResolvedValue({ results: [] });
});

describe("decideRetry", () => {
  const servedAt = new Date(NOW - 60_000).toISOString();

  it("re-serves a person who was paid for and never handed to the vendor", () => {
    expect(decideRetry({ contacted: false, sent: false }, servedAt, NOW)).toBe("retry");
  });

  it("re-serves a person email-gateway holds no record of at all", () => {
    expect(decideRetry(null, servedAt, NOW)).toBe("retry");
  });

  it("leaves a person the vendor is still holding inside the TTL alone", () => {
    const queued = new Date(NOW - (RETRY_QUEUE_TTL_MS - 1)).toISOString();
    expect(decideRetry({ contacted: true, sent: false }, queued, NOW)).toBe("in_vendor_queue");
  });

  it("re-serves a queued person once the TTL has elapsed — the vendor lost it", () => {
    const lost = new Date(NOW - RETRY_QUEUE_TTL_MS).toISOString();
    expect(decideRetry({ contacted: true, sent: false }, lost, NOW)).toBe("retry");
  });

  it("treats a served row with no timestamp as still queued, never as lost", () => {
    expect(decideRetry({ contacted: true, sent: false }, null, NOW)).toBe("in_vendor_queue");
  });

  it("is terminal once an email went out, whatever `contacted` says", () => {
    expect(decideRetry({ contacted: true, sent: true }, servedAt, NOW)).toBe("terminal");
    expect(decideRetry({ contacted: false, sent: true }, servedAt, NOW)).toBe("terminal");
  });

  it("accepts a Date served_at as well as a string — raw sql hands back either", () => {
    expect(decideRetry({ contacted: true, sent: false }, new Date(NOW - 1000), NOW)).toBe(
      "in_vendor_queue",
    );
  });
});

describe("campaignScopeFlags", () => {
  it("reads the campaign scope of either provider", () => {
    expect(
      campaignScopeFlags(
        { email: "a@example.com", broadcast: { campaign: scoped({ contacted: true }) } },
        CAMPAIGN,
      ),
    ).toEqual({ contacted: true, sent: false });

    expect(
      campaignScopeFlags(
        { email: "a@example.com", transactional: { campaign: scoped({ sent: true }) } },
        CAMPAIGN,
      ),
    ).toEqual({ contacted: false, sent: true });
  });

  it("reads the byCampaign entry for this campaign too", () => {
    expect(
      campaignScopeFlags(
        {
          email: "a@example.com",
          broadcast: { byCampaign: { [CAMPAIGN]: scoped({ contacted: true, sent: true }) } },
        },
        CAMPAIGN,
      ),
    ).toEqual({ contacted: true, sent: true });
  });

  it("ignores another campaign's entry — that is human-service's suppression policy, not this pool's", () => {
    expect(
      campaignScopeFlags(
        {
          email: "a@example.com",
          broadcast: { byCampaign: { "other-campaign": scoped({ contacted: true, sent: true }) } },
        },
        CAMPAIGN,
      ),
    ).toEqual({ contacted: false, sent: false });
  });

  it("is null when email-gateway has no result for the email", () => {
    expect(campaignScopeFlags(undefined, CAMPAIGN)).toBeNull();
  });
});

describe("pickRetryCandidate", () => {
  it("returns nothing and asks email-gateway nothing when the pool is empty", async () => {
    expect(await pickRetryCandidate(base)).toBeNull();
    expect(checkDeliveryStatus).not.toHaveBeenCalled();
  });

  it("hands back the person who was served and never contacted", async () => {
    rows = [row({ id: "aaaaaaaa-0000-4000-8000-000000000001" })];
    checkDeliveryStatus.mockResolvedValue({ results: [] });

    const picked = await pickRetryCandidate(base);

    expect(picked?.leadId).toBe("lead-aaaaaaaa-0000-4000-8000-000000000001");
    expect(picked?.email).toBe("aaaaaaaa-0000-4000-8000-000000000001@example.com");
    // Claimed: run ids re-pointed, attempt counted.
    expect(rows[0].retry_claimed_at).toBe(new Date(NOW).toISOString());
    expect(rows[0].retry_count).toBe(1);
  });

  it("asks email-gateway once, campaign-scoped, for the whole batch", async () => {
    rows = [
      row({ id: "aaaaaaaa-0000-4000-8000-000000000001" }),
      row({ id: "aaaaaaaa-0000-4000-8000-000000000002" }),
    ];

    await pickRetryCandidate(base);

    expect(checkDeliveryStatus).toHaveBeenCalledTimes(1);
    const [brandId, campaignId, items] = checkDeliveryStatus.mock.calls[0];
    expect(brandId).toBe("brand-1");
    expect(campaignId).toBe(CAMPAIGN);
    expect(items).toHaveLength(2);
  });

  it("skips a person the vendor is still holding and takes the next one", async () => {
    rows = [
      row({ id: "aaaaaaaa-0000-4000-8000-000000000001", served_at: "2026-08-25T09:00:00.000Z" }),
      row({ id: "aaaaaaaa-0000-4000-8000-000000000002", served_at: "2026-08-25T10:00:00.000Z" }),
    ];
    checkDeliveryStatus.mockResolvedValue({
      results: [
        {
          email: "aaaaaaaa-0000-4000-8000-000000000001@example.com",
          broadcast: { campaign: scoped({ contacted: true }) },
        },
      ],
    });

    const picked = await pickRetryCandidate(base);

    expect(picked?.id).toBe("aaaaaaaa-0000-4000-8000-000000000002");
    // The queued person is untouched — no claim, no terminal marker.
    expect(rows[0].retry_claimed_at).toBeNull();
    expect(rows[0].sent_at).toBeNull();
  });

  it("marks a sent person terminal, never returns them, and never re-queries them", async () => {
    markSentUpdates.length = 0;
    rows = [row({ id: "aaaaaaaa-0000-4000-8000-000000000001" })];
    checkDeliveryStatus.mockResolvedValue({
      results: [
        {
          email: "aaaaaaaa-0000-4000-8000-000000000001@example.com",
          broadcast: { campaign: scoped({ contacted: true, sent: true }) },
        },
      ],
    });

    expect(await pickRetryCandidate(base)).toBeNull();
    expect(markSentUpdates).toHaveLength(1);
    expect(markSentUpdates[0].set).toMatchObject({ sentAt: new Date(NOW) });

    // The fixture stands in for what the database persists from that write. Asserting the
    // row here would only be asserting this file's own double — the statement itself is
    // executed against a real database in tests/integration/retry-pool-sql.test.ts.
    rows[0].sent_at = new Date(NOW).toISOString();

    // A later pull neither sees nor asks about them.
    checkDeliveryStatus.mockClear();
    expect(await pickRetryCandidate({ ...base, nowMs: NOW + 60_000 })).toBeNull();
    expect(checkDeliveryStatus).not.toHaveBeenCalled();
  });

  it("falls through to serve-next when email-gateway is unreachable — fail-closed", async () => {
    rows = [row({ id: "aaaaaaaa-0000-4000-8000-000000000001" })];
    checkDeliveryStatus.mockRejectedValue(new Error("ECONNREFUSED"));

    expect(await pickRetryCandidate(base)).toBeNull();
    expect(rows[0].retry_claimed_at).toBeNull();
  });

  it("never hands the same person to two concurrent pulls", async () => {
    rows = [
      row({ id: "aaaaaaaa-0000-4000-8000-000000000001", served_at: "2026-08-25T09:00:00.000Z" }),
      row({ id: "aaaaaaaa-0000-4000-8000-000000000002", served_at: "2026-08-25T10:00:00.000Z" }),
    ];

    const first = await pickRetryCandidate({ ...base, runId: "run-a" });
    const second = await pickRetryCandidate({ ...base, runId: "run-b" });

    expect(first?.id).toBe("aaaaaaaa-0000-4000-8000-000000000001");
    expect(second?.id).toBe("aaaaaaaa-0000-4000-8000-000000000002");
    expect(first?.id).not.toBe(second?.id);
  });

  it("returns nothing rather than a duplicate when every candidate is already claimed", async () => {
    rows = [row({ id: "aaaaaaaa-0000-4000-8000-000000000001" })];

    expect((await pickRetryCandidate(base))?.id).toBe("aaaaaaaa-0000-4000-8000-000000000001");
    expect(await pickRetryCandidate(base)).toBeNull();
  });

  it("moves a person who keeps failing to the back of the queue", async () => {
    rows = [
      row({ id: "aaaaaaaa-0000-4000-8000-000000000001", served_at: "2026-08-25T09:00:00.000Z" }),
      row({ id: "aaaaaaaa-0000-4000-8000-000000000002", served_at: "2026-08-25T10:00:00.000Z" }),
    ];

    // First pull takes the oldest serve.
    expect((await pickRetryCandidate(base))?.id).toBe("aaaaaaaa-0000-4000-8000-000000000001");

    // Their claim lease expires without a contact: they are retryable again, but they now
    // sort BEHIND the person who has never been tried.
    const later = NOW + 2 * 60 * 60 * 1000;
    expect((await pickRetryCandidate({ ...base, nowMs: later }))?.id).toBe(
      "aaaaaaaa-0000-4000-8000-000000000002",
    );
  });

  it("skips a served row that carries no registered email", async () => {
    rows = [row({ id: "aaaaaaaa-0000-4000-8000-000000000001", email: null })];

    expect(await pickRetryCandidate(base)).toBeNull();
    expect(checkDeliveryStatus).not.toHaveBeenCalled();
  });
});
