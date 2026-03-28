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
    it("sends x-org-id and x-user-id headers for fetchApolloStats", async () => {
      fetchSpy.mockReturnValue(
        jsonResponse({ stats: { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 } })
      );

      const { fetchApolloStats } = await import("../../src/lib/apollo-client.js");
      await fetchApolloStats({}, "org-uuid-123", { userId: "user-1", runId: "run-1" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-123");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });

    it("sends x-org-id and x-user-id headers for apolloSearch", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ people: [], pagination: { page: 1, totalPages: 1, totalEntries: 0 } }));

      const { apolloSearch } = await import("../../src/lib/apollo-client.js");
      await apolloSearch({ personTitles: ["CEO"] }, 1, { orgId: "org-uuid-456", userId: "user-456" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-456");
      expect(opts.headers["x-user-id"]).toBe("user-456");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });

    it("sends run context as headers (not body) for apolloSearch", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ people: [], pagination: { page: 1, totalPages: 1, totalEntries: 0 } }));

      const { apolloSearch } = await import("../../src/lib/apollo-client.js");
      await apolloSearch({ personTitles: ["CEO"] }, 1, {
        orgId: "org-1", runId: "run-1", brandId: "brand-1", campaignId: "camp-1", workflowSlug: "wf-1", featureSlug: "feat-1",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
      const body = JSON.parse(opts.body);
      expect(body).not.toHaveProperty("runId");
      expect(body).not.toHaveProperty("brandId");
      expect(body).not.toHaveProperty("campaignId");
      expect(body).not.toHaveProperty("workflowSlug");
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

    it("sends run context as headers (not body) for apolloEnrich", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ person: { id: "p1" } }));

      const { apolloEnrich } = await import("../../src/lib/apollo-client.js");
      await apolloEnrich("person-123", {
        orgId: "org-1", runId: "run-1", brandId: "brand-1", campaignId: "camp-1", workflowSlug: "wf-1", featureSlug: "feat-1",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
      const body = JSON.parse(opts.body);
      expect(body).not.toHaveProperty("runId");
      expect(body).not.toHaveProperty("brandId");
      expect(body).not.toHaveProperty("campaignId");
      expect(body).not.toHaveProperty("workflowSlug");
    });
  });

  describe("email-gateway-client", () => {
    it("forwards x-org-id, x-user-id, x-run-id headers", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ results: [] }));

      const { checkDeliveryStatus } = await import("../../src/lib/email-gateway-client.js");
      await checkDeliveryStatus("brand-1", "camp-1", [{ leadId: "l1", email: "a@b.com" }], {
        orgId: "org-1", userId: "user-1", runId: "run-1",
      });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
    });

    it("forwards x-campaign-id, x-brand-id, x-workflow-slug headers when provided", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ results: [] }));

      const { checkDeliveryStatus } = await import("../../src/lib/email-gateway-client.js");
      await checkDeliveryStatus("brand-1", "camp-1", [{ leadId: "l1", email: "a@b.com" }], {
        orgId: "org-1", userId: "user-1", runId: "run-1",
        campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-test", featureSlug: "feat-test",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-test");
      expect(opts.headers["x-feature-slug"]).toBe("feat-test");
    });
  });

  describe("campaign-client", () => {
    it("sends x-org-id, x-user-id, x-run-id headers for fetchCampaign", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ campaign: { id: "c1", name: "Test", targetAudience: null, targetOutcome: null, valueForTarget: null } }));

      const { fetchCampaign } = await import("../../src/lib/campaign-client.js");
      await fetchCampaign("campaign-123", "org-uuid-abc", { userId: "user-abc", runId: "run-abc" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-abc");
      expect(opts.headers["x-user-id"]).toBe("user-abc");
      expect(opts.headers["x-run-id"]).toBe("run-abc");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
    });

    it("forwards workflow tracking headers when provided", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ campaign: { id: "c1", name: "Test", targetAudience: null, targetOutcome: null, valueForTarget: null } }));

      const { fetchCampaign } = await import("../../src/lib/campaign-client.js");
      await fetchCampaign("campaign-123", "org-1", {
        userId: "user-1", runId: "run-1",
        campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-test", featureSlug: "feat-test",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-test");
      expect(opts.headers["x-feature-slug"]).toBe("feat-test");
    });
  });

  describe("brand-client", () => {
    it("sends x-org-id, x-user-id, x-run-id headers and orgId query param", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ brand: { id: "b1", name: "Test", domain: null, elevatorPitch: null, bio: null, mission: null, location: null, categories: null } }));

      const { fetchBrand } = await import("../../src/lib/brand-client.js");
      await fetchBrand("brand-123", "org-uuid-def", { userId: "user-def", runId: "run-def" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-uuid-def");
      expect(opts.headers["x-user-id"]).toBe("user-def");
      expect(opts.headers["x-run-id"]).toBe("run-def");
      expect(opts.headers).not.toHaveProperty("x-clerk-org-id");
      expect(url).toContain("orgId=org-uuid-def");
      expect(url).not.toContain("clerkOrgId");
    });

    it("forwards workflow tracking headers when provided", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ brand: { id: "b1", name: "Test", domain: null, elevatorPitch: null, bio: null, mission: null, location: null, categories: null } }));

      const { fetchBrand } = await import("../../src/lib/brand-client.js");
      await fetchBrand("brand-123", "org-1", {
        userId: "user-1", runId: "run-1",
        campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-test", featureSlug: "feat-test",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-test");
      expect(opts.headers["x-feature-slug"]).toBe("feat-test");
    });
  });

  describe("brand-client extractBrandFields", () => {
    it("sends correct headers and body for extractBrandFields", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ brandId: "brand-123", results: [] }));

      const { extractBrandFields } = await import("../../src/lib/brand-client.js");
      await extractBrandFields(
        "brand-123",
        [{ key: "elevator_pitch", description: "One-sentence pitch" }],
        "org-1",
        { userId: "user-1", runId: "run-1", campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-1", featureSlug: "feat-1" },
      );

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/brands/brand-123/extract-fields");
      expect(opts.method).toBe("POST");
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
      const body = JSON.parse(opts.body);
      expect(body.fields).toHaveLength(1);
      expect(body.fields[0].key).toBe("elevator_pitch");
    });
  });

  describe("brand-client fetchExtractedFields", () => {
    it("sends correct headers for fetchExtractedFields", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ brandId: "brand-123", fields: [] }));

      const { fetchExtractedFields } = await import("../../src/lib/brand-client.js");
      await fetchExtractedFields("brand-123", "org-1", { userId: "user-1", runId: "run-1" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/brands/brand-123/extracted-fields");
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
    });
  });

  describe("apollo-client apolloMatch", () => {
    it("sends correct headers and body for apolloMatch", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ enrichmentId: "e1", person: { id: "p1", email: "j@test.com" }, cached: false }));

      const { apolloMatch } = await import("../../src/lib/apollo-client.js");
      await apolloMatch(
        { firstName: "Jane", lastName: "Doe", organizationDomain: "test.com" },
        { orgId: "org-1", userId: "user-1", runId: "run-1", brandId: "brand-1", campaignId: "camp-1", workflowSlug: "wf-1", featureSlug: "feat-1" }
      );

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/match");
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
      const body = JSON.parse(opts.body);
      expect(body.firstName).toBe("Jane");
      expect(body.lastName).toBe("Doe");
      expect(body.organizationDomain).toBe("test.com");
    });
  });

  describe("outlet-client", () => {
    it("sends correct URL and headers for fetchOutletsByCampaign", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ outlets: [{ id: "o1", outletName: "TechCrunch", outletUrl: "https://techcrunch.com", outletDomain: "techcrunch.com" }] }));

      const { fetchOutletsByCampaign } = await import("../../src/lib/outlet-client.js");
      await fetchOutletsByCampaign("campaign-123", "org-uuid-1", {
        userId: "user-1", runId: "run-1", campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-1", featureSlug: "feat-1",
      });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/internal/outlets/by-campaign/campaign-123");
      expect(opts.headers["x-org-id"]).toBe("org-uuid-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
    });

    it("returns null on error", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ error: "not found" }, 404));

      const { fetchOutletsByCampaign } = await import("../../src/lib/outlet-client.js");
      const result = await fetchOutletsByCampaign("bad-campaign", "org-1");

      expect(result).toBeNull();
    });

    it("sends POST to /buffer/next with headers and optional idempotencyKey", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: true, outlet: { outletId: "o1", outletName: "TechCrunch" } }));

      const { fetchNextOutlet } = await import("../../src/lib/outlet-client.js");
      const result = await fetchNextOutlet({
        orgId: "org-1", userId: "user-1", runId: "run-1",
        campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-1", featureSlug: "feat-1",
        idempotencyKey: "idem-123",
      });

      expect(result.found).toBe(true);
      expect(result.outlet?.outletId).toBe("o1");

      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/buffer/next");
      expect(opts.method).toBe("POST");
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
      const body = JSON.parse(opts.body);
      expect(body.idempotencyKey).toBe("idem-123");
    });

    it("sends empty body to /buffer/next when no idempotencyKey", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: false }));

      const { fetchNextOutlet } = await import("../../src/lib/outlet-client.js");
      const result = await fetchNextOutlet({
        orgId: "org-1", campaignId: "camp-1", brandId: "brand-1",
      });

      expect(result.found).toBe(false);
      const [, opts] = fetchSpy.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body).toEqual({});
    });

    it("returns found: false on /buffer/next error", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ error: "fail" }, 502));

      const { fetchNextOutlet } = await import("../../src/lib/outlet-client.js");
      const result = await fetchNextOutlet({
        orgId: "org-1", campaignId: "camp-1", brandId: "brand-1",
      });

      expect(result.found).toBe(false);
    });

    it("sends headers to /buffer/next for outlet discovery", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: true, outlet: { outletId: "o1" } }));

      const { fetchNextOutlet } = await import("../../src/lib/outlet-client.js");
      await fetchNextOutlet({
        campaignId: "camp-1", brandId: "brand-1",
        orgId: "org-1", userId: "user-1", runId: "run-1",
      });

      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/buffer/next");
      expect(opts.method).toBe("POST");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-org-id"]).toBe("org-1");
    });
  });

  describe("journalist-client", () => {
    it("sends POST to /buffer/next with outletId in body and headers", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: true, journalist: { id: "j1", journalistName: "Jane Doe" } }));

      const { fetchNextJournalist } = await import("../../src/lib/journalist-client.js");
      await fetchNextJournalist("outlet-uuid-1", {
        campaignId: "camp-1", orgId: "org-1", userId: "user-1", runId: "run-1", brandId: "brand-1", workflowSlug: "wf-1", featureSlug: "feat-1",
      });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [url, opts] = fetchSpy.mock.calls[0];
      expect(url).toContain("/buffer/next");
      expect(opts.method).toBe("POST");
      const body = JSON.parse(opts.body);
      expect(body.outletId).toBe("outlet-uuid-1");
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-1");
      expect(opts.headers["x-feature-slug"]).toBe("feat-1");
    });

    it("forwards idempotencyKey and maxArticles in request body", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: false }));

      const { fetchNextJournalist } = await import("../../src/lib/journalist-client.js");
      await fetchNextJournalist("outlet-uuid-1", {
        orgId: "org-1", idempotencyKey: "idem-123", maxArticles: 10,
      });

      const [, opts] = fetchSpy.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body.outletId).toBe("outlet-uuid-1");
      expect(body.idempotencyKey).toBe("idem-123");
      expect(body.maxArticles).toBe(10);
    });

    it("sends only outletId when no optional params provided", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ found: false }));

      const { fetchNextJournalist } = await import("../../src/lib/journalist-client.js");
      await fetchNextJournalist("outlet-uuid-1", { orgId: "org-1" });

      const [, opts] = fetchSpy.mock.calls[0];
      const body = JSON.parse(opts.body);
      expect(body).toEqual({ outletId: "outlet-uuid-1" });
    });

    it("returns found: false on error", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ error: "not found" }, 404));

      const { fetchNextJournalist } = await import("../../src/lib/journalist-client.js");
      const result = await fetchNextJournalist("bad-outlet");

      expect(result).toEqual({ found: false });
    });
  });

  describe("runs-client", () => {
    it("sends x-org-id, x-user-id, x-run-id headers for updateRun", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ id: "run-1", status: "completed" }));

      const { updateRun } = await import("../../src/lib/runs-client.js");
      await updateRun("run-1", "completed", { orgId: "org-1", userId: "user-1" });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-org-id"]).toBe("org-1");
      expect(opts.headers["x-user-id"]).toBe("user-1");
      expect(opts.headers["x-run-id"]).toBe("run-1");
    });

    it("sends orgId, userId, parentRunId as headers (not body)", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ id: "run-uuid-1" }, 201));

      const { createRun } = await import("../../src/lib/runs-client.js");
      await createRun({
        orgId: "org-uuid-run",
        serviceName: "lead-service",
        taskName: "test-task",
        userId: "user-uuid-run",
        parentRunId: "parent-run-1",
      });

      expect(fetchSpy).toHaveBeenCalledOnce();
      const [, opts] = fetchSpy.mock.calls[0];
      const headers = opts.headers;
      expect(headers["x-org-id"]).toBe("org-uuid-run");
      expect(headers["x-user-id"]).toBe("user-uuid-run");
      expect(headers["x-run-id"]).toBe("parent-run-1");
      const body = JSON.parse(opts.body);
      expect(body).not.toHaveProperty("orgId");
      expect(body).not.toHaveProperty("userId");
      expect(body).not.toHaveProperty("parentRunId");
      expect(body).not.toHaveProperty("clerkOrgId");
      expect(body).not.toHaveProperty("clerkUserId");
      expect(body).not.toHaveProperty("appId");
    });

    it("forwards workflow tracking headers for createRun", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ id: "run-uuid-2" }, 201));

      const { createRun } = await import("../../src/lib/runs-client.js");
      await createRun({
        orgId: "org-1",
        serviceName: "lead-service",
        taskName: "test-task",
        campaignId: "camp-1",
        brandId: "brand-1",
        workflowSlug: "wf-test",
        featureSlug: "feat-test",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-test");
      expect(opts.headers["x-feature-slug"]).toBe("feat-test");
    });

    it("forwards workflow tracking headers for updateRun", async () => {
      fetchSpy.mockReturnValue(jsonResponse({ id: "run-1", status: "completed" }));

      const { updateRun } = await import("../../src/lib/runs-client.js");
      await updateRun("run-1", "completed", {
        orgId: "org-1", userId: "user-1",
        campaignId: "camp-1", brandId: "brand-1", workflowSlug: "wf-test", featureSlug: "feat-test",
      });

      const [, opts] = fetchSpy.mock.calls[0];
      expect(opts.headers["x-campaign-id"]).toBe("camp-1");
      expect(opts.headers["x-brand-id"]).toBe("brand-1");
      expect(opts.headers["x-workflow-slug"]).toBe("wf-test");
      expect(opts.headers["x-feature-slug"]).toBe("feat-test");
    });
  });
});
