import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Stub global fetch
const fetchSpy = vi.fn();

beforeEach(() => {
  fetchSpy.mockReset();
  vi.stubGlobal("fetch", fetchSpy);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve({
    ok: status >= 200 && status < 300,
    status,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  });
}

describe("service client headers", () => {
  describe("apollo-client", () => {
    it("sends x-org-id header (not x-clerk-org-id) for fetchApolloStats", async () => {
      fetchSpy.mockReturnValue(
        jsonResponse({ stats: { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 } })
      );

      const { fetchApolloStats } = await import("../../src/lib/apollo-client.js");
      await fetchApolloStats({}, "org-uuid-123");

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-123");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });

    it("sends x-org-id header for apolloSearch", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ people: [], pagination: { page: 1, totalPages: 1, totalEntries: 0 } }));

      const { apolloSearch } = await import("../../src/lib/apollo-client.js");
      await apolloSearch({ personTitles: ["CEO"] }, 1, { orgId: "org-uuid-456" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-456");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });

    it("sends x-org-id header for apolloEnrich", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ person: { id: "p1" } }));

      const { apolloEnrich } = await import("../../src/lib/apollo-client.js");
      await apolloEnrich("person-123", { orgId: "org-uuid-789" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-789");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });
  });

  describe("campaign-client", () => {
    it("sends x-org-id header (not x-clerk-org-id) for fetchCampaign", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ campaign: { id: "c1", name: "Test", targetAudience: null, targetOutcome: null, valueForTarget: null } }));

      const { fetchCampaign } = await import("../../src/lib/campaign-client.js");
      await fetchCampaign("campaign-123", "org-uuid-abc");

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-abc");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });
  });

  describe("brand-client", () => {
    it("sends x-org-id header and orgId query param (not clerk variants)", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ brand: { id: "b1", name: "Test", domain: null, elevatorPitch: null, bio: null, mission: null, location: null, categories: null } }));

      const { fetchBrand } = await import("../../src/lib/brand-client.js");
      await fetchBrand("brand-123", "org-uuid-def");

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-def");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
      // Query param should be orgId, not clerkOrgId
      expect(url).toContain("orgId=org-uuid-def");
      expect(url).not.toContain("clerkOrgId");
    });
  });

  describe("runs-client", () => {
    it("sends orgId and userId in body (not clerkOrgId/clerkUserId)", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ id: "run-uuid-1" }, 201));

      const { createRun } = await import("../../src/lib/runs-client.js");
      await createRun({
        orgId: "org-uuid-run",
        appId: "test-app",
        serviceName: "lead-service",
        taskName: "test-task",
        userId: "user-uuid-run",
      });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body.orgId).toBe("org-uuid-run");
      expect(body.userId).toBe("user-uuid-run");
      expect(body).not.toHaveProperty("clerkOrgId");
      expect(body).not.toHaveProperty("clerkUserId");
    });
  });
});
