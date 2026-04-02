import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { fetchNextOutlet, fetchOutletsByCampaign } from "../../src/lib/outlet-client.js";

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

const baseContext = {
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  campaignId: "camp-1",
  brandId: "brand-1",
};

describe("outlet-client", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  describe("fetchNextOutlet", () => {
    it("parses outlets array response and returns first outlet as found", async () => {
      const outlet = { outletId: "o-1", outletName: "Test Outlet", outletUrl: "https://test.com", outletDomain: "test.com", campaignId: "camp-1", brandIds: ["brand-1"], relevanceScore: 0.9, whyRelevant: "good", whyNotRelevant: "", overallRelevance: "high", runId: null };
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ outlets: [outlet] }),
      });

      const result = await fetchNextOutlet(baseContext);
      expect(result.found).toBe(true);
      expect(result.outlet).toEqual(outlet);
      expect(mockFetch).toHaveBeenCalledOnce();
    });

    it("returns found=false when outlets array is empty", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ outlets: [] }),
      });

      const result = await fetchNextOutlet(baseContext);
      expect(result.found).toBe(false);
      expect(result.outlet).toBeUndefined();
      expect(mockFetch).toHaveBeenCalledOnce();
    });

    it("retries on 500 and succeeds on second attempt", async () => {
      const outlet = { outletId: "o-1", outletName: "Test Outlet" };

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 500 })
        .mockResolvedValueOnce({
          ok: true,
          json: () => Promise.resolve({ outlets: [outlet] }),
        });

      const promise = fetchNextOutlet(baseContext);
      await vi.advanceTimersByTimeAsync(5_000);

      const result = await promise;
      expect(result.found).toBe(true);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it("retries twice on consecutive 500s and succeeds on third attempt", async () => {
      const outlet = { outletId: "o-1", outletName: "Test Outlet" };

      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 500 })
        .mockResolvedValueOnce({ ok: false, status: 500 })
        .mockResolvedValueOnce({
          ok: true,
          json: () => Promise.resolve({ outlets: [outlet] }),
        });

      const promise = fetchNextOutlet(baseContext);
      await vi.advanceTimersByTimeAsync(20_000);

      const result = await promise;
      expect(result.found).toBe(true);
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    it("throws after exhausting all retries on 500", async () => {
      mockFetch
        .mockResolvedValueOnce({ ok: false, status: 500 })
        .mockResolvedValueOnce({ ok: false, status: 500 })
        .mockResolvedValueOnce({ ok: false, status: 500 });

      // Capture rejection eagerly to avoid unhandled rejection warnings
      const promise = fetchNextOutlet(baseContext).catch((e: Error) => e);
      await vi.advanceTimersByTimeAsync(20_000);

      const error = await promise;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toContain("buffer/next failed");
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });

    it("does not retry on 4xx errors", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 400 });

      const result = await fetchNextOutlet(baseContext);
      expect(result.found).toBe(false);
      expect(mockFetch).toHaveBeenCalledOnce();
    });

    it("retries on network errors (fetch throws)", async () => {
      const outlet = { outletId: "o-1", outletName: "Test Outlet" };

      mockFetch
        .mockImplementationOnce(() => Promise.reject(new Error("fetch failed")))
        .mockResolvedValueOnce({
          ok: true,
          json: () => Promise.resolve({ outlets: [outlet] }),
        });

      // Capture eagerly so the initial rejection doesn't go unhandled during timer wait
      let resolved: { found: boolean; outlet?: unknown } | Error | undefined;
      const promise = fetchNextOutlet(baseContext).then(
        (v) => { resolved = v; },
        (e) => { resolved = e; },
      );
      await vi.advanceTimersByTimeAsync(5_000);
      await promise;

      expect(resolved).not.toBeInstanceOf(Error);
      expect((resolved as { found: boolean }).found).toBe(true);
      expect(mockFetch).toHaveBeenCalledTimes(2);
    });

    it("throws after exhausting retries on network errors", async () => {
      mockFetch
        .mockImplementationOnce(() => Promise.reject(new Error("connection refused")))
        .mockImplementationOnce(() => Promise.reject(new Error("connection refused")))
        .mockImplementationOnce(() => Promise.reject(new Error("connection refused")));

      const promise = fetchNextOutlet(baseContext).catch((e: Error) => e);
      await vi.advanceTimersByTimeAsync(20_000);

      const error = await promise;
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toContain("connection refused");
      expect(mockFetch).toHaveBeenCalledTimes(3);
    });
  });

  describe("fetchOutletsByCampaign", () => {
    it("returns outlets on success", async () => {
      const outlets = [{ id: "o-1", outletName: "Test" }];
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ outlets }),
      });

      const result = await fetchOutletsByCampaign("camp-1", "org-1");
      expect(result).toEqual(outlets);
      expect(mockFetch).toHaveBeenCalledOnce();
    });

    it("throws on 500", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });

      await expect(fetchOutletsByCampaign("camp-1", "org-1")).rejects.toThrow(
        "Failed to fetch outlets"
      );
    });

    it("returns null on 4xx", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });

      const result = await fetchOutletsByCampaign("camp-1", "org-1");
      expect(result).toBeNull();
    });
  });
});
