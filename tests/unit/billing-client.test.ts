import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { authorizeCredits } from "../../src/lib/billing-client.js";

describe("billing-client", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    process.env.BILLING_SERVICE_URL = "http://billing.test";
    process.env.BILLING_SERVICE_API_KEY = "test-billing-key";
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    vi.restoreAllMocks();
  });

  it("returns sufficient: true when balance is enough", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ sufficient: true, balance_cents: 500, required_cents: 5 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      })
    );

    const result = await authorizeCredits({
      items: [{ costName: "lead-serve", quantity: 1 }],
      description: "lead-serve — apollo-search+enrich",
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    expect(result.sufficient).toBe(true);
    expect(result.balance_cents).toBe(500);
    expect(result.required_cents).toBe(5);
  });

  it("returns sufficient: false when balance is insufficient", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ sufficient: false, balance_cents: 2, required_cents: 5 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      })
    );

    const result = await authorizeCredits({
      items: [{ costName: "lead-serve", quantity: 1 }],
      description: "lead-serve — apollo-search+enrich",
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    expect(result.sufficient).toBe(false);
    expect(result.balance_cents).toBe(2);
    expect(result.required_cents).toBe(5);
  });

  it("forwards all headers including optional campaign/brand/workflow", async () => {
    const fetchSpy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ sufficient: true, balance_cents: 100, required_cents: 5 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      })
    );

    await authorizeCredits({
      items: [{ costName: "lead-serve", quantity: 1 }],
      description: "test",
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
      campaignId: "camp-1",
      brandId: "brand-1",
      workflowName: "cold-email",
    });

    const [url, opts] = fetchSpy.mock.calls[0];
    expect(url).toBe("http://billing.test/v1/credits/authorize");
    const headers = opts!.headers as Record<string, string>;
    expect(headers["x-org-id"]).toBe("org-1");
    expect(headers["x-user-id"]).toBe("user-1");
    expect(headers["x-run-id"]).toBe("run-1");
    expect(headers["x-campaign-id"]).toBe("camp-1");
    expect(headers["x-brand-id"]).toBe("brand-1");
    expect(headers["x-workflow-name"]).toBe("cold-email");
  });

  it("sends items and description in body", async () => {
    const fetchSpy = vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ sufficient: true, balance_cents: 100, required_cents: 5 }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      })
    );

    await authorizeCredits({
      items: [{ costName: "lead-serve", quantity: 1 }],
      description: "lead-serve — apollo-search+enrich",
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    const body = JSON.parse(fetchSpy.mock.calls[0][1]!.body as string);
    expect(body.items).toEqual([{ costName: "lead-serve", quantity: 1 }]);
    expect(body.description).toBe("lead-serve — apollo-search+enrich");
  });

  it("throws when billing service returns non-OK status", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response("Internal Server Error", { status: 500 })
    );

    await expect(
      authorizeCredits({
        items: [{ costName: "lead-serve", quantity: 1 }],
        description: "test",
        orgId: "org-1",
        userId: "user-1",
        runId: "run-1",
      })
    ).rejects.toThrow("Billing service call failed: 500");
  });

  it("throws when BILLING_SERVICE_URL is not configured", async () => {
    process.env.BILLING_SERVICE_URL = "";

    await expect(
      authorizeCredits({
        items: [{ costName: "lead-serve", quantity: 1 }],
        description: "test",
        orgId: "org-1",
        userId: "user-1",
        runId: "run-1",
      })
    ).rejects.toThrow("BILLING_SERVICE_URL not configured");
  });
});
