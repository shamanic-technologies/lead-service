/**
 * The standing reads: how many leads stand in each state for a scope (no rows), and one column of
 * that board as a bounded page.
 *
 * What is under test is that the two describe the SAME set — the count a column states and the
 * rows the column shows come from one index, one delivery overlay and one standing resolver — and
 * that the page hydrates ONLY the ids the plan chose. A board whose column header and column body
 * disagree is the bug this exists to remove, so a test that mocked the plan away would prove
 * nothing.
 */
import { describe, it, expect, beforeAll, beforeEach, vi } from "vitest";
import express from "express";
import request from "supertest";

interface SqlNode { __sql: true; strings: readonly string[]; values: unknown[] }
function isNode(v: unknown): v is SqlNode {
  return !!v && typeof v === "object" && (v as SqlNode).__sql === true;
}

let hydrated: string[][] = [];

/** The `rowIds` an index-driven read hydrates by, read straight off the compiled query. */
function readRowIds(node: SqlNode): string[] | null {
  let ids: string[] | null = null;
  node.strings.forEach((s, i) => {
    if (/lc\.id = ANY\(\s*$/.test(s)) ids = node.values[i] as string[];
  });
  for (const v of node.values) {
    if (!isNode(v)) continue;
    ids = ids ?? readRowIds(v);
  }
  return ids;
}

vi.mock("../../src/db/index.js", () => ({
  sql: (strings: readonly string[], ...values: unknown[]) => {
    const node: SqlNode = { __sql: true, strings, values };
    const text = strings.join(" ");
    if (!/ORDER BY lc\.created_at/.test(text)) return node;
    return {
      then: (resolve: (rows: unknown[]) => void) => {
        const ids = readRowIds(node);
        hydrated.push(ids ?? []);
        const rows = (ids ?? indexRows.map((r) => r.id)).map((id) => rawRow(id));
        return Promise.resolve(rows).then(resolve);
      },
    };
  },
  db: { execute: () => Promise.resolve([]) },
}));

let indexRows: Array<{
  id: string;
  leadId: string;
  campaignId: string;
  brandIds: string[];
  status: string;
  email: string | null;
  servedAt: string | null;
  createdAtText: string;
}> = [];

vi.mock("../../src/lib/lead-index.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/lib/lead-index.js")>()),
  fetchLeadIndex: () => Promise.resolve(indexRows),
  fetchOutcomesByLead: () => Promise.resolve(new Map()),
  countLeadListRows: () => Promise.resolve(indexRows.length),
}));

let gatewayFails = false;
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (_brandId: string, _campaignId: string | undefined, items: Array<{ email: string }>) =>
    gatewayFails
      ? Promise.reject(new Error("[email-gateway-client] Status check failed: 503"))
      : Promise.resolve({
          results: items.map((i) => ({ email: i.email, broadcast: { brand: { contacted: true } } })),
        }),
}));

/** The state each row's standing resolves to, by `leads_campaigns.id`. */
let standingByRow: Record<string, string> = {};
let standingChunks: number[] = [];
let standingFails = false;
vi.mock("../../src/lib/lead-standing-resolver.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/lib/lead-standing-resolver.js")>()),
  createLeadStandingResolver: () => ({
    resolve: (rows: Array<{ id: string }>) => {
      if (standingFails) return Promise.reject(new Error("campaign-service unreachable"));
      standingChunks.push(rows.length);
      return Promise.resolve(
        new Map(
          rows
            .filter((r) => standingByRow[r.id])
            .map((r) => [r.id, { state: standingByRow[r.id] }]),
        ),
      );
    },
  }),
}));

vi.mock("../../src/lib/lead-shape.js", () => ({
  buildFullLeadsBatch: (ids: string[]) =>
    Promise.resolve(new Map(ids.map((id) => [id, { leadId: id, contacts: [] }]))),
}));
vi.mock("../../src/lib/audience-client.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/lib/audience-client.js")>()),
  resolveAudiencesForBrand: vi.fn().mockResolvedValue({ byAudienceId: {}, byEmail: {} }),
}));
vi.mock("../../src/lib/offer-card-client.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../src/lib/offer-card-client.js")>()),
  createOfferCardResolver: () => ({ resolve: () => Promise.resolve(new Map()) }),
}));
vi.mock("../../src/lib/campaign-identity-client.js", () => ({ resolveCampaignFamily: () => Promise.resolve(null) }));
vi.mock("../../src/lib/trace-event.js", () => ({ traceEvent: vi.fn().mockResolvedValue(undefined) }));
vi.mock("../../src/config.js", () => ({ LEAD_SERVICE_API_KEY: "test-api-key" }));

const ORG = "30000000-0000-0000-0000-000000000001";
const USER = "40000000-0000-0000-0000-000000000001";
const BRAND = "50000000-0000-0000-0000-000000000001";

function rawRow(id: string) {
  const index = indexRows.find((r) => r.id === id)!;
  return {
    id,
    lead_id: index.leadId,
    campaign_id: index.campaignId,
    org_id: ORG,
    user_id: null,
    brand_ids: [BRAND],
    status: index.status,
    status_reason: null,
    status_details: null,
    parent_run_id: null,
    run_id: null,
    served_at: null,
    workflow_slug: null,
    feature_slug: null,
    goal: null,
    active_goal_id: null,
    brand_profile_id: null,
    audience_id: null,
    created_at: index.createdAtText,
    created_at_cursor: index.createdAtText,
    lead_apollo_person_id: null,
  };
}

function person(i: number) {
  return {
    id: `aaaaaaaa-0000-0000-0000-${String(i).padStart(12, "0")}`,
    leadId: `bbbbbbbb-0000-0000-0000-${String(i).padStart(12, "0")}`,
    campaignId: "camp-1",
    brandIds: [BRAND],
    status: "served",
    email: `p${i}@example.test`,
    servedAt: null,
    createdAtText: `2026-01-01 00:00:00.${String(i).padStart(6, "0")}+00`,
  };
}

let app: express.Express;
beforeAll(async () => {
  const { default: route } = await import("../../src/routes/leads.js");
  app = express();
  app.use(express.json());
  app.use(route);
}, 30_000);

beforeEach(() => {
  indexRows = Array.from({ length: 6 }, (_, i) => person(i));
  // Two in play, one interested, one bought, one out — and one nobody can resolve.
  standingByRow = {
    [indexRows[0].id]: "contacted",
    [indexRows[1].id]: "engaged",
    [indexRows[2].id]: "sales_interest",
    [indexRows[3].id]: "customer",
    [indexRows[4].id]: "disqualified",
  };
  hydrated = [];
  standingChunks = [];
  gatewayFails = false;
  standingFails = false;
});

function counts(query: string) {
  return request(app)
    .get(`/orgs/leads/standing-counts${query}`)
    .set("x-api-key", "test-api-key")
    .set("x-org-id", ORG)
    .set("x-user-id", USER);
}

function get(query: string) {
  return request(app)
    .get(`/orgs/leads${query}`)
    .set("x-api-key", "test-api-key")
    .set("x-org-id", ORG)
    .set("x-user-id", USER);
}

describe("GET /orgs/leads/standing-counts", () => {
  it("answers every standing state's count and NO rows", async () => {
    const res = await counts(`?brandId=${BRAND}`);
    expect(res.status).toBe(200);
    expect(res.body.leads).toBeUndefined();
    expect(res.body.total).toBe(6);
    expect(res.body.counts).toEqual({
      unresolved: 1,
      not_contacted: 0,
      contacted: 1,
      engaged: 1,
      sales_interest: 1,
      customer: 1,
      disqualified: 1,
    });
    // A standing is a partition, so the columns add up to the population they describe.
    const summed = Object.values(res.body.counts as Record<string, number>).reduce((a, b) => a + b, 0);
    expect(summed).toBe(res.body.total);
    // A count is a number: nothing was hydrated to produce it.
    expect(hydrated).toEqual([]);
  });

  it("counts a lead nobody could resolve as unresolved rather than dropping it", async () => {
    standingByRow = {};
    const res = await counts(`?brandId=${BRAND}`);
    expect(res.status).toBe(200);
    expect(res.body.counts.unresolved).toBe(6);
    expect(res.body.total).toBe(6);
  });

  it("refuses rather than reporting zeros when the delivery evidence cannot be read", async () => {
    gatewayFails = true;
    expect((await counts(`?brandId=${BRAND}`)).status).toBe(502);
  });

  it("refuses rather than reporting zeros when the standing cannot be resolved", async () => {
    standingFails = true;
    expect((await counts(`?brandId=${BRAND}`)).status).toBe(502);
  });

  it("answers an offer no campaign sells as an honest zero, never the brand", async () => {
    const res = await counts(`?brandId=${BRAND}&campaignId=c1&offerId=o1`);
    expect(res.status).toBe(400);
  });
});

describe("GET /orgs/leads?standing=", () => {
  it("returns exactly the leads in that state, and hydrates only those", async () => {
    const res = await get(`?brandId=${BRAND}&view=basic&standing=sales_interest`);
    expect(res.status).toBe(200);
    expect(res.body.leads).toHaveLength(1);
    expect(res.body.leads[0].id).toBe(indexRows[2].id);
    expect(res.body.total).toBe(1);
    expect(hydrated).toEqual([[indexRows[2].id]]);
  });

  it("states the column's size even when the page is bounded below it", async () => {
    standingByRow = Object.fromEntries(indexRows.map((r) => [r.id, "engaged"]));
    const res = await get(`?brandId=${BRAND}&view=basic&standing=engaged&limit=2`);
    expect(res.status).toBe(200);
    expect(res.body.leads).toHaveLength(2);
    // The column's real size, not the size of the page — the whole point of the read.
    expect(res.body.total).toBe(6);
    expect(res.body.nextCursor).toBeTruthy();
  });

  it("walks one column with limit + cursor, visiting every lead exactly once", async () => {
    standingByRow = Object.fromEntries(indexRows.map((r) => [r.id, "engaged"]));
    const seen: string[] = [];
    let cursor: string | null = null;
    for (let page = 0; page < 5; page++) {
      const query = `?brandId=${BRAND}&view=basic&standing=engaged&limit=2` +
        (cursor ? `&cursor=${encodeURIComponent(cursor)}` : "");
      const res: request.Response = await get(query);
      expect(res.status).toBe(200);
      for (const lead of res.body.leads) seen.push(lead.id);
      cursor = res.body.nextCursor;
      if (!cursor) break;
    }
    expect(seen).toEqual(indexRows.map((r) => r.id));
    expect(new Set(seen).size).toBe(6);
  });

  it("agrees with the count for the same scope", async () => {
    const counted = await counts(`?brandId=${BRAND}`);
    const listed = await get(`?brandId=${BRAND}&view=basic&standing=customer`);
    expect(listed.body.total).toBe(counted.body.counts.customer);
    expect(listed.body.leads).toHaveLength(counted.body.counts.customer);
  });

  it("narrows to the rows satisfying BOTH when a bucket is named too", async () => {
    const res = await get(`?brandId=${BRAND}&view=basic&standing=customer&bucket=positive_reply`);
    expect(res.status).toBe(200);
    expect(res.body.leads).toHaveLength(0);
    expect(res.body.total).toBe(0);
  });

  it("400s on a standing vocabulary it does not know", async () => {
    const res = await get(`?brandId=${BRAND}&standing=interested`);
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("Unknown standing");
  });

  it("refuses rather than answering a differently-filtered list when the standing cannot be resolved", async () => {
    standingFails = true;
    const res = await get(`?brandId=${BRAND}&view=basic&standing=customer`);
    expect(res.status).toBe(502);
  });

  it("resolves standings in bounded chunks rather than one enormous bind", async () => {
    indexRows = Array.from({ length: 2_500 }, (_, i) => person(i));
    standingByRow = Object.fromEntries(indexRows.map((r) => [r.id, "contacted"]));
    const res = await counts(`?brandId=${BRAND}`);
    expect(res.status).toBe(200);
    expect(standingChunks).toEqual([1000, 1000, 500]);
  });
});
