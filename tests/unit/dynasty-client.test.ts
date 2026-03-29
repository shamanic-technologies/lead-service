import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

// Env vars are set by tests/setup.ts before module imports

const {
  resolveFeatureDynastySlugs,
  resolveWorkflowDynastySlugs,
  fetchFeatureDynastyMap,
  fetchWorkflowDynastyMap,
} = await import("../../src/lib/dynasty-client.js");

describe("dynasty-client", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("resolveFeatureDynastySlugs", () => {
    it("returns slugs from features-service", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["feat-alpha", "feat-alpha-v2", "feat-alpha-v3"] }),
      });

      const result = await resolveFeatureDynastySlugs("feat-alpha");
      expect(result).toEqual(["feat-alpha", "feat-alpha-v2", "feat-alpha-v3"]);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://features:3010/features/dynasty/slugs?dynastySlug=feat-alpha",
        expect.objectContaining({
          headers: expect.objectContaining({ "X-API-Key": process.env.FEATURES_SERVICE_API_KEY }),
        }),
      );
    });

    it("passes context headers when provided", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["s1"] }),
      });

      await resolveFeatureDynastySlugs("s1", { orgId: "org-1", userId: "user-1", runId: "run-1" });
      const headers = mockFetch.mock.calls[0][1].headers;
      expect(headers["x-org-id"]).toBe("org-1");
      expect(headers["x-user-id"]).toBe("user-1");
      expect(headers["x-run-id"]).toBe("run-1");
    });

    it("returns empty array on non-ok response", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
      const result = await resolveFeatureDynastySlugs("unknown");
      expect(result).toEqual([]);
    });

    it("returns empty array on fetch error", async () => {
      mockFetch.mockRejectedValueOnce(new Error("network error"));
      const result = await resolveFeatureDynastySlugs("fail");
      expect(result).toEqual([]);
    });
  });

  describe("resolveWorkflowDynastySlugs", () => {
    it("returns slugs from workflow-service", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ slugs: ["cold-email", "cold-email-v2"] }),
      });

      const result = await resolveWorkflowDynastySlugs("cold-email");
      expect(result).toEqual(["cold-email", "cold-email-v2"]);
      expect(mockFetch).toHaveBeenCalledWith(
        "http://workflows:3002/workflows/dynasty/slugs?dynastySlug=cold-email",
        expect.objectContaining({
          headers: expect.objectContaining({ "X-API-Key": process.env.WORKFLOW_SERVICE_API_KEY }),
        }),
      );
    });

    it("returns empty array on non-ok response", async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });
      const result = await resolveWorkflowDynastySlugs("broken");
      expect(result).toEqual([]);
    });
  });

  describe("fetchFeatureDynastyMap", () => {
    it("builds reverse map from dynasties", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          dynasties: [
            { dynastySlug: "feat-alpha", slugs: ["feat-alpha", "feat-alpha-v2"] },
            { dynastySlug: "feat-beta", slugs: ["feat-beta"] },
          ],
        }),
      });

      const map = await fetchFeatureDynastyMap();
      expect(map.get("feat-alpha")).toBe("feat-alpha");
      expect(map.get("feat-alpha-v2")).toBe("feat-alpha");
      expect(map.get("feat-beta")).toBe("feat-beta");
      expect(map.get("unknown-slug")).toBeUndefined();
    });

    it("returns empty map on error", async () => {
      mockFetch.mockRejectedValueOnce(new Error("fail"));
      const map = await fetchFeatureDynastyMap();
      expect(map.size).toBe(0);
    });
  });

  describe("fetchWorkflowDynastyMap", () => {
    it("builds reverse map from workflow dynasties", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          dynasties: [
            { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2", "cold-email-v3"] },
          ],
        }),
      });

      const map = await fetchWorkflowDynastyMap();
      expect(map.get("cold-email")).toBe("cold-email");
      expect(map.get("cold-email-v2")).toBe("cold-email");
      expect(map.get("cold-email-v3")).toBe("cold-email");
    });
  });
});
