import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// A lead's OFFER is the offer named by the campaign it was served under — the same resolution the
// offer FILTER established, read in the other direction. The name is brand-service's. Neither read
// may cost anything per row: a brand's list reaches tens of thousands of rows and is polled every
// 30 seconds.

vi.mock("../../src/config.js", () => ({
  CAMPAIGN_SERVICE_URL: "https://campaign.test",
  CAMPAIGN_SERVICE_API_KEY: "test-campaign-key",
  BRAND_SERVICE_URL: "https://brand.test",
  BRAND_SERVICE_API_KEY: "test-brand-key",
}));

const ORG = "30000000-0000-0000-0000-000000000001";
const BRAND = "75d7e3e8-6926-4f85-a557-976895400666";
const OTHER_BRAND = "75d7e3e8-6926-4f85-a557-976895400999";
const OFFER = "0ffe0000-0000-4000-8000-000000000001";
const OTHER_OFFER = "0ffe0000-0000-4000-8000-000000000002";

const CAMPAIGNS_URL = "https://campaign.test/campaigns";
const offersUrl = (brandId: string) => `https://brand.test/internal/brands/${brandId}/offers`;

function campaign(id: string, offerId: string | null, brandId: string = BRAND) {
  return { id, orgId: ORG, brandIds: [brandId], offerId };
}

function jsonOnce(body: unknown) {
  return { ok: true, json: async () => body };
}

/** Answer each URL from a table, so call ORDER never makes a test pass or fail by accident. */
function routeFetch(fetchSpy: ReturnType<typeof vi.fn>, table: Record<string, unknown>) {
  fetchSpy.mockImplementation(async (url: string) => {
    const body = table[url];
    if (body === undefined) throw new Error(`unexpected fetch: ${url}`);
    return jsonOnce(body);
  });
}

const load = () => import("../../src/lib/offer-card-client.js");

describe("createOfferCardResolver", () => {
  const fetchSpy = vi.fn();

  beforeEach(() => {
    fetchSpy.mockReset();
    vi.stubGlobal("fetch", fetchSpy);
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  // AC1 — the offer, with its name, for the campaign the lead was served under.
  it("names the offer the row's campaign sells", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, brandId: BRAND, name: "Fractional CFO retainer" }] },
    });

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG, brandId: BRAND }).resolve(["c-1"]);

    expect(cards.get("c-1")).toEqual({ id: OFFER, name: "Fractional CFO retainer" });
  });

  // AC2 — a campaign that names no offer gives its leads none. Never a sibling's, never the brand's.
  it("gives no card to a campaign that states no offer", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER), campaign("c-2", null)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, name: "Retainer" }] },
    });

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG }).resolve(["c-1", "c-2"]);

    expect(cards.has("c-2")).toBe(false);
    expect(cards.get("c-1")).toEqual({ id: OFFER, name: "Retainer" });
  });

  it("never lets one brand's catalogue name another brand's offer", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER, BRAND), campaign("c-2", OTHER_OFFER, OTHER_BRAND)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, name: "Ours" }] },
      [offersUrl(OTHER_BRAND)]: { offers: [{ offerId: OTHER_OFFER, name: "Theirs" }] },
    });

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG }).resolve(["c-1", "c-2"]);

    expect(cards.get("c-1")).toEqual({ id: OFFER, name: "Ours" });
    expect(cards.get("c-2")).toEqual({ id: OTHER_OFFER, name: "Theirs" });
  });

  it("states the id with no name when brand-service does not list the offer back", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OTHER_OFFER, name: "Something else" }] },
    });

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"]);

    // The offer is real; only its name is unknown. Collapsing the card would say "no offer".
    expect(cards.get("c-1")).toEqual({ id: OFFER, name: null });
  });

  // AC5 — the cost is per REQUEST, not per row and not per chunk.
  it("reads campaign-service ONCE and each brand ONCE, whatever the row count", async () => {
    const campaigns = Array.from({ length: 60 }, (_, i) => campaign(`c-${i}`, OFFER));
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, name: "Retainer" }] },
    });

    const { createOfferCardResolver } = await load();
    const resolver = createOfferCardResolver({ orgId: ORG, brandId: BRAND });

    // Ten chunks of 500 rows spread over 60 campaigns — the shape of a real large-brand walk.
    for (let chunk = 0; chunk < 10; chunk += 1) {
      const rowCampaignIds = Array.from({ length: 500 }, (_, i) => `c-${i % 60}`);
      const cards = await resolver.resolve(rowCampaignIds);
      expect(cards.size).toBe(60);
    }

    // 5,000 rows, 2 calls. A per-row (or per-chunk) implementation fails here.
    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(fetchSpy.mock.calls.filter(([url]) => url === CAMPAIGNS_URL)).toHaveLength(1);
    expect(fetchSpy.mock.calls.filter(([url]) => url === offersUrl(BRAND))).toHaveLength(1);
  });

  it("shares one in-flight read between chunks resolved concurrently", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, name: "Retainer" }] },
    });

    const { createOfferCardResolver } = await load();
    const resolver = createOfferCardResolver({ orgId: ORG });
    await Promise.all([resolver.resolve(["c-1"]), resolver.resolve(["c-1"]), resolver.resolve(["c-1"])]);

    expect(fetchSpy).toHaveBeenCalledTimes(2);
  });

  it("asks for no brand it returned no rows for", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER, BRAND), campaign("c-2", OTHER_OFFER, OTHER_BRAND)] },
      [offersUrl(BRAND)]: { offers: [{ offerId: OFFER, name: "Ours" }] },
    });

    const { createOfferCardResolver } = await load();
    await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"]);

    expect(fetchSpy.mock.calls.map(([url]) => url)).toEqual([CAMPAIGNS_URL, offersUrl(BRAND)]);
  });

  it("costs nothing at all when the chunk carries no campaign ids", async () => {
    const { createOfferCardResolver } = await load();
    expect((await createOfferCardResolver({ orgId: ORG }).resolve([])).size).toBe(0);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("reads the whole org list — a bound would leave stopped campaigns unable to name their offer", async () => {
    routeFetch(fetchSpy, { [CAMPAIGNS_URL]: { campaigns: [] } });

    const { createOfferCardResolver } = await load();
    await createOfferCardResolver({ orgId: ORG, userId: "u-1", runId: "r-1", brandId: BRAND }).resolve(["c-1"]);

    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toBe(CAMPAIGNS_URL);
    expect(url).not.toContain("limit");
    expect(init.headers["x-org-id"]).toBe(ORG);
    expect(init.headers["X-API-Key"]).toBe("test-campaign-key");
    expect(init.headers["x-brand-id"]).toBe(BRAND);
  });

  it("sends brand-service its own key and the org that owns the configuration", async () => {
    routeFetch(fetchSpy, {
      [CAMPAIGNS_URL]: { campaigns: [campaign("c-1", OFFER)] },
      [offersUrl(BRAND)]: { offers: [] },
    });

    const { createOfferCardResolver } = await load();
    await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"]);

    const [, init] = fetchSpy.mock.calls.find(([url]) => url === offersUrl(BRAND))!;
    expect(init.headers["X-API-Key"]).toBe("test-brand-key");
    expect(init.headers["x-org-id"]).toBe(ORG);
  });

  // Unresolvable is ABSENT — never a guess, and never a failed list.
  it("carries no offer, loudly, when campaign-service cannot answer", async () => {
    fetchSpy.mockRejectedValue(new Error("ECONNREFUSED"));

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"]);

    expect(cards.size).toBe(0);
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining("offer unresolved"));
  });

  it("absorbs an outage once per request rather than retrying it on every chunk", async () => {
    fetchSpy.mockRejectedValue(new Error("ECONNREFUSED"));

    const { createOfferCardResolver } = await load();
    const resolver = createOfferCardResolver({ orgId: ORG });
    await resolver.resolve(["c-1"]);
    await resolver.resolve(["c-2"]);
    await resolver.resolve(["c-3"]);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(console.error).toHaveBeenCalledTimes(1);
  });

  it("keeps the offer id, loudly, when brand-service cannot name it", async () => {
    fetchSpy.mockImplementation(async (url: string) => {
      if (url === CAMPAIGNS_URL) return jsonOnce({ campaigns: [campaign("c-1", OFFER)] });
      return { ok: false, status: 503, text: async () => "down" };
    });

    const { createOfferCardResolver } = await load();
    const cards = await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"]);

    expect(cards.get("c-1")).toEqual({ id: OFFER, name: null });
    expect(console.error).toHaveBeenCalledWith(expect.stringContaining("offer names unresolved"));
  });

  it("treats an unreadable campaign body as unknown, not as an org selling nothing", async () => {
    routeFetch(fetchSpy, { [CAMPAIGNS_URL]: {} });

    const { createOfferCardResolver } = await load();
    expect((await createOfferCardResolver({ orgId: ORG }).resolve(["c-1"])).size).toBe(0);
    expect(console.error).toHaveBeenCalled();
  });
});
