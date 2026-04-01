import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/config.js", () => ({
  REPLY_QUALIFICATION_SERVICE_URL: "http://reply-qualification:8080",
  REPLY_QUALIFICATION_SERVICE_API_KEY: "test-key",
}));

import { fetchQualificationsByOrg, classifyReply } from "../../src/lib/reply-qualification-client.js";

describe("classifyReply", () => {
  it("maps willing_to_meet to positive", () => {
    expect(classifyReply("willing_to_meet")).toBe("positive");
  });

  it("maps interested to positive", () => {
    expect(classifyReply("interested")).toBe("positive");
  });

  it("maps not_interested to negative", () => {
    expect(classifyReply("not_interested")).toBe("negative");
  });

  it("maps needs_more_info to other", () => {
    expect(classifyReply("needs_more_info")).toBe("other");
  });

  it("maps out_of_office to other", () => {
    expect(classifyReply("out_of_office")).toBe("other");
  });

  it("maps unsubscribe to other", () => {
    expect(classifyReply("unsubscribe")).toBe("other");
  });

  it("maps bounce to other", () => {
    expect(classifyReply("bounce")).toBe("other");
  });

  it("maps unknown classification to other", () => {
    expect(classifyReply("something_new")).toBe("other");
  });
});

describe("fetchQualificationsByOrg", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("fetches qualifications and returns map keyed by email", async () => {
    const qualifications = [
      { id: "q1", fromEmail: "alice@acme.com", classification: "interested", confidence: 0.9, createdAt: "2026-03-29T10:00:00Z" },
      { id: "q2", fromEmail: "bob@acme.com", classification: "not_interested", confidence: 0.8, createdAt: "2026-03-28T10:00:00Z" },
    ];

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
      ok: true,
      json: async () => qualifications,
    } as Response);

    const result = await fetchQualificationsByOrg("org-1");

    expect(result.size).toBe(2);
    expect(result.get("alice@acme.com")?.classification).toBe("interested");
    expect(result.get("bob@acme.com")?.classification).toBe("not_interested");
  });

  it("keeps only the latest qualification per email", async () => {
    const qualifications = [
      { id: "q1", fromEmail: "alice@acme.com", classification: "not_interested", confidence: 0.9, createdAt: "2026-03-28T10:00:00Z" },
      { id: "q2", fromEmail: "alice@acme.com", classification: "interested", confidence: 0.95, createdAt: "2026-03-30T10:00:00Z" },
    ];

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
      ok: true,
      json: async () => qualifications,
    } as Response);

    const result = await fetchQualificationsByOrg("org-1");

    expect(result.size).toBe(1);
    expect(result.get("alice@acme.com")?.classification).toBe("interested");
    expect(result.get("alice@acme.com")?.id).toBe("q2");
  });

  it("throws on non-ok response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: async () => "Internal error",
    } as Response);

    await expect(fetchQualificationsByOrg("org-1")).rejects.toThrow(
      "reply-qualification-service returned 500",
    );
  });

  it("passes correct headers and query params", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce({
      ok: true,
      json: async () => [],
    } as Response);

    await fetchQualificationsByOrg("org-1", {
      runId: "run-1",
      brandId: "b1",
    });

    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toContain("sourceOrgId=org-1");
    expect(url).toContain("limit=10000");
    expect((init as RequestInit).headers).toEqual(
      expect.objectContaining({
        "X-API-Key": "test-key",
        "x-org-id": "org-1",
        "x-run-id": "run-1",
        "x-brand-id": "b1",
      }),
    );
  });
});
