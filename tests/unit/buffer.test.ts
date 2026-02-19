import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock db
vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
      servedLeads: {
        findFirst: vi.fn(),
      },
      leadBuffer: {
        findFirst: vi.fn(),
      },
      enrichments: {
        findFirst: vi.fn(),
      },
    },
    insert: vi.fn(),
    update: vi.fn(),
  },
}));

// Mock apollo-client
vi.mock("../../src/lib/apollo-client.js", () => ({
  apolloSearchNext: vi.fn(),
  apolloSearchParams: vi.fn(),
  apolloEnrich: vi.fn(),
}));

// Mock campaign-client — returns null by default (context enrichment is best-effort)
vi.mock("../../src/lib/campaign-client.js", () => ({
  fetchCampaign: vi.fn().mockResolvedValue(null),
}));

// Mock brand-client — returns null by default (context enrichment is best-effort)
vi.mock("../../src/lib/brand-client.js", () => ({
  fetchBrand: vi.fn().mockResolvedValue(null),
}));

import { db } from "../../src/db/index.js";
import { pushLeads, pullNext } from "../../src/lib/buffer.js";
import { apolloSearchNext, apolloSearchParams, apolloEnrich } from "../../src/lib/apollo-client.js";

describe("buffer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("pushLeads", () => {
    it("buffers leads that are not already served", async () => {
      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // db.insert for leadBuffer
      const valuesMock = vi.fn().mockResolvedValue(undefined);
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await pushLeads({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        leads: [
          { email: "alice@acme.com", externalId: "e-1", data: { name: "Alice" } },
          { email: "bob@acme.com", externalId: "e-2", data: { name: "Bob" } },
        ],
      });

      expect(result.buffered).toBe(2);
      expect(result.skippedAlreadyServed).toBe(0);
    });

    it("skips leads that are already served", async () => {
      // First call: served, second call: not served
      vi.mocked(db.query.servedLeads.findFirst)
        .mockResolvedValueOnce({
          id: "uuid-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          email: "alice@acme.com",
          externalId: null,
          metadata: null,
          parentRunId: null,
          runId: null,
          brandId: "brand-1",
          campaignId: "campaign-1",
          servedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined);

      const valuesMock = vi.fn().mockResolvedValue(undefined);
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await pushLeads({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        leads: [
          { email: "alice@acme.com", externalId: "e-1" },
          { email: "bob@acme.com", externalId: "e-2" },
        ],
      });

      expect(result.buffered).toBe(1);
      expect(result.skippedAlreadyServed).toBe(1);
    });
  });

  describe("pullNext", () => {
    it("returns found: false when buffer is empty", async () => {
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue(undefined);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(false);
    });

    it("returns a lead and marks it served", async () => {
      // Buffer has a row
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        externalId: "e-1",
        data: { name: "Alice" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      });

      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // markServed insert
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer row status
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        parentRunId: "run-1",
        runId: "child-run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("alice@acme.com");
      expect(result.lead?.externalId).toBe("e-1");
      expect(result.lead?.data).toEqual({ name: "Alice" });
    });

    it("passes lead data through as-is without modification", async () => {
      const apolloData = {
        firstName: "Svitlana",
        organizationName: "HashtagWeb3",
        organizationDomain: "hashtagweb3.com",
        organizationIndustry: "information technology & services",
      };

      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "svitlana@hashtagweb3.com",
        externalId: "e-1",
        data: apolloData,
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      });

      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      // Data must pass through exactly as stored — flat camelCase fields, no transformation
      expect(result.lead?.data).toEqual(apolloData);
    });

    it("skips already-served buffer rows and tries next", async () => {
      // First buffer row is already served, second is not
      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce({
          id: "buf-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "alice@acme.com",
          externalId: "e-1",
          data: { name: "Alice" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "buf-2",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "bob@acme.com",
          externalId: "e-2",
          data: { name: "Bob" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        });

      // First isServed check: true (skip), second: false (serve)
      vi.mocked(db.query.servedLeads.findFirst)
        .mockResolvedValueOnce({
          id: "served-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          email: "alice@acme.com",
          externalId: null,
          metadata: null,
          parentRunId: null,
          runId: null,
          brandId: "brand-1",
          campaignId: "campaign-1",
          servedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined);

      // markServed for bob
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-2" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer rows
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("bob@acme.com");
    });

    it("fills buffer from apolloSearchNext when buffer empty and searchParams provided", async () => {
      // leadBuffer.findFirst calls:
      //   1. pullNext buffer check → undefined (empty)
      //   2. isInBuffer check for apollo-1 → undefined (not in buffer)
      //   3. pullNext buffer check → new lead row
      const newLeadRow = {
        id: "buf-new",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "new-lead@example.com",
        externalId: "apollo-1",
        data: { firstName: "New" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      };

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)   // 1: pullNext buffer empty
        .mockResolvedValueOnce(undefined)   // 2: isInBuffer → not in buffer
        .mockResolvedValueOnce(newLeadRow); // 3: pullNext buffer → new lead

      // apolloSearchParams returns validated params
      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      // apolloSearchNext returns 1 person
      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-1", email: "new-lead@example.com", firstName: "New" }],
        done: true,
        totalEntries: 1,
      });

      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // db.insert for buffer row + markServed
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({
        onConflictDoNothing: onConflictMock,
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer row status
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("new-lead@example.com");
      expect(vi.mocked(apolloSearchParams)).toHaveBeenCalledOnce();
      // searchParams always passed on every call
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalledWith(
        expect.objectContaining({
          campaignId: "campaign-1",
          brandId: "brand-1",
          appId: "my-app",
          searchParams: { personTitles: ["CEO"] },
        })
      );
    });

    it("walks pages when first page returns all dupes", async () => {
      // leadBuffer.findFirst calls:
      //   1. pullNext buffer check → undefined (empty)
      //   2. isInBuffer for apollo-1 → undefined (not in buffer)
      //   3. isInBuffer for apollo-2 → undefined (not in buffer)
      //   4. isInBuffer for apollo-3 → undefined (not in buffer)
      //   5. pullNext buffer check → return the new lead
      const freshLeadRow = {
        id: "buf-new",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "fresh@example.com",
        externalId: "apollo-3",
        data: { firstName: "Fresh" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      };

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)   // 1: pullNext buffer empty
        .mockResolvedValueOnce(undefined)   // 2: isInBuffer apollo-1
        .mockResolvedValueOnce(undefined)   // 3: isInBuffer apollo-2
        .mockResolvedValueOnce(undefined)   // 4: isInBuffer apollo-3
        .mockResolvedValueOnce(freshLeadRow); // 5: pullNext buffer → new lead

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      // Page 1: all people are already served → page 2: fresh person
      vi.mocked(apolloSearchNext)
        .mockResolvedValueOnce({
          people: [
            { id: "apollo-1", email: "dupe1@example.com" },
            { id: "apollo-2", email: "dupe2@example.com" },
          ],
          done: false,
          totalEntries: 50,
        })
        .mockResolvedValueOnce({
          people: [
            { id: "apollo-3", email: "fresh@example.com" },
          ],
          done: false,
          totalEntries: 50,
        });

      // isServed: dupe1 and dupe2 are served, fresh is not
      let servedCallCount = 0;
      vi.mocked(db.query.servedLeads.findFirst).mockImplementation(async () => {
        servedCallCount++;
        if (servedCallCount <= 2) {
          return {
            id: `served-${servedCallCount}`,
            organizationId: "org-1",
            namespace: "campaign-1",
            email: `dupe${servedCallCount}@example.com`,
            externalId: null,
            metadata: null,
            parentRunId: null,
            runId: null,
            brandId: "brand-1",
            campaignId: "campaign-1",
            servedAt: new Date(),
          };
        }
        return undefined;
      });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-new" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({
        onConflictDoNothing: onConflictMock,
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("fresh@example.com");
      // apolloSearchNext called multiple times (page walk), always with searchParams
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalledWith(
        expect.objectContaining({ searchParams: { personTitles: ["CEO"] } })
      );
    });

    it("returns found: false when Apollo returns done: true with 0 people", async () => {
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue(undefined);

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [],
        done: true,
        totalEntries: 0,
      });

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "impossible search" },
        appId: "my-app",
      });

      expect(result.found).toBe(false);
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalled();
    });

    it("uses cached enrichment instead of calling apolloEnrich", async () => {
      // Buffer has a row without email
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "apollo-person-1",
        data: { firstName: "Ray" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      });

      // Enrichment cache has a hit for this personId
      vi.mocked(db.query.enrichments.findFirst).mockResolvedValue({
        id: "enrich-1",
        email: "ray@provaliant.com",
        apolloPersonId: "apollo-person-1",
        firstName: "Ray",
        lastName: "Smith",
        title: "Program Director",
        linkedinUrl: "http://linkedin.com/in/ray-smith",
        organizationName: "Provaliant",
        organizationDomain: "provaliant.com",
        organizationIndustry: "IT",
        organizationSize: "27",
        responseRaw: { firstName: "Ray", lastName: "Smith", email: "ray@provaliant.com", title: "Program Director" },
        enrichedAt: new Date(),
      });

      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // markServed insert
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer row
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("ray@provaliant.com");
      // apolloEnrich should NOT have been called — cache hit
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("calls apolloEnrich on cache miss and saves result to cache", async () => {
      // Buffer has a row without email
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "apollo-person-2",
        data: { firstName: "Alice" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      });

      // Enrichment cache miss
      vi.mocked(db.query.enrichments.findFirst).mockResolvedValue(undefined);

      // apolloEnrich returns data
      vi.mocked(apolloEnrich).mockResolvedValue({
        person: {
          id: "apollo-person-2",
          email: "alice@acme.com",
          firstName: "Alice",
          lastName: "Johnson",
          title: "CEO",
        },
      });

      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // db.insert for enrichment cache save + markServed
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer row
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("alice@acme.com");
      // apolloEnrich should have been called
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledWith("apollo-person-2", expect.objectContaining({
        runId: "run-1",
        brandId: "brand-1",
        campaignId: "campaign-1",
      }));
      // db.insert should have been called twice: once for enrichment cache, once for markServed
      expect(vi.mocked(db.insert)).toHaveBeenCalledTimes(2);
    });

    it("skips apolloEnrich for already-cached persons when looping through served leads", async () => {
      // Scenario: 3 email-less leads in buffer, all cached, first 2 are already served
      // Only the 3rd should be served — and apolloEnrich should NEVER be called

      const makeBufferRow = (id: string, externalId: string) => ({
        id,
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId,
        data: { firstName: externalId },
        status: "buffered" as const,
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      });

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(makeBufferRow("buf-1", "person-A"))
        .mockResolvedValueOnce(makeBufferRow("buf-2", "person-B"))
        .mockResolvedValueOnce(makeBufferRow("buf-3", "person-C"));

      // All 3 are in enrichment cache
      vi.mocked(db.query.enrichments.findFirst)
        .mockResolvedValueOnce({
          id: "e-1", email: "a@acme.com", apolloPersonId: "person-A",
          firstName: "A", lastName: null, title: null, linkedinUrl: null,
          organizationName: null, organizationDomain: null,
          organizationIndustry: null, organizationSize: null,
          responseRaw: { email: "a@acme.com" }, enrichedAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "e-2", email: "b@acme.com", apolloPersonId: "person-B",
          firstName: "B", lastName: null, title: null, linkedinUrl: null,
          organizationName: null, organizationDomain: null,
          organizationIndustry: null, organizationSize: null,
          responseRaw: { email: "b@acme.com" }, enrichedAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "e-3", email: "c@acme.com", apolloPersonId: "person-C",
          firstName: "C", lastName: null, title: null, linkedinUrl: null,
          organizationName: null, organizationDomain: null,
          organizationIndustry: null, organizationSize: null,
          responseRaw: { email: "c@acme.com" }, enrichedAt: new Date(),
        });

      // isServed: A and B are served, C is not
      vi.mocked(db.query.servedLeads.findFirst)
        .mockResolvedValueOnce({
          id: "s-1", organizationId: "org-1", namespace: "campaign-1",
          email: "a@acme.com", externalId: null, metadata: null,
          parentRunId: null, runId: null, brandId: "brand-1",
          campaignId: "campaign-1", servedAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "s-2", organizationId: "org-1", namespace: "campaign-1",
          email: "b@acme.com", externalId: null, metadata: null,
          parentRunId: null, runId: null, brandId: "brand-1",
          campaignId: "campaign-1", servedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined); // C is not served

      // markServed for C
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-3" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer rows (skip A, skip B, serve C)
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("c@acme.com");
      // apolloEnrich should NEVER have been called — all 3 were cached
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
      // enrichment cache was checked 3 times
      expect(vi.mocked(db.query.enrichments.findFirst)).toHaveBeenCalledTimes(3);
    });

    it("caches no-email enrichment results and skips on future encounters", async () => {
      // Buffer has 2 rows without email, first person has no email from Apollo
      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce({
          id: "buf-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "no-email-person",
          data: { firstName: "Ghost" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "buf-2",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "has-email-person",
          data: { firstName: "Real" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        });

      // First person: cache miss. Second person: cache miss.
      vi.mocked(db.query.enrichments.findFirst)
        .mockResolvedValueOnce(undefined)
        .mockResolvedValueOnce(undefined);

      // apolloEnrich: first person has no email, second has email
      vi.mocked(apolloEnrich)
        .mockResolvedValueOnce({
          person: { id: "no-email-person", firstName: "Ghost" },
        })
        .mockResolvedValueOnce({
          person: { id: "has-email-person", email: "real@acme.com", firstName: "Real" },
        });

      // isServed returns false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // db.insert for enrichment cache + markServed
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      // Update buffer rows
      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("real@acme.com");
      // apolloEnrich called twice (both were cache misses)
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledTimes(2);
      // db.insert called 3 times: no-email cache save, email cache save, markServed
      expect(vi.mocked(db.insert)).toHaveBeenCalledTimes(3);
    });

    it("skips enrichment when cache has no-email entry for person", async () => {
      // Buffer has a row without email
      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce({
          id: "buf-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "",
          externalId: "known-no-email",
          data: { firstName: "Ghost" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        })
        .mockResolvedValueOnce({
          id: "buf-2",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "bob@acme.com",
          externalId: "e-2",
          data: { name: "Bob" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        });

      // Enrichment cache: no-email entry (email is null)
      vi.mocked(db.query.enrichments.findFirst).mockResolvedValueOnce({
        id: "e-1", email: null, apolloPersonId: "known-no-email",
        firstName: "Ghost", lastName: null, title: null, linkedinUrl: null,
        organizationName: null, organizationDomain: null,
        organizationIndustry: null, organizationSize: null,
        responseRaw: null, enrichedAt: new Date(),
      });

      // isServed for Bob: false
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      // markServed
      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("bob@acme.com");
      // apolloEnrich should NOT have been called — cache hit (no email)
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("fillBufferFromSearch skips people with cached served emails", async () => {
      // Buffer starts empty, then fillBufferFromSearch is triggered
      // Search returns 2 people without emails:
      //   person-A: cached email already served
      //   person-B: no cache (fresh)
      // Only person-B should be inserted into the buffer

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [
          { id: "person-A", firstName: "A" },
          { id: "person-B", firstName: "B" },
        ],
        done: true,
        totalEntries: 2,
      });

      // leadBuffer.findFirst calls:
      //   1. pullNext buffer empty
      //   2. isInBuffer person-A → not in buffer
      //   3. isInBuffer person-B → not in buffer
      //   4. pullNext buffer → new lead (person-B)
      const freshLeadRow = {
        id: "buf-new",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "person-B",
        data: { firstName: "B" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      };

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)    // 1: pullNext buffer empty
        .mockResolvedValueOnce(undefined)    // 2: isInBuffer person-A
        .mockResolvedValueOnce(undefined)    // 3: isInBuffer person-B
        .mockResolvedValueOnce(freshLeadRow); // 4: pullNext → person-B

      // enrichments.findFirst calls (in fillBufferFromSearch):
      //   person-A: cached with email already served
      //   person-B: no cache
      // Then in pullNext for person-B: no cache
      vi.mocked(db.query.enrichments.findFirst)
        .mockResolvedValueOnce({
          id: "e-1", email: "a@acme.com", apolloPersonId: "person-A",
          firstName: "A", lastName: null, title: null, linkedinUrl: null,
          organizationName: null, organizationDomain: null,
          organizationIndustry: null, organizationSize: null,
          responseRaw: { email: "a@acme.com" }, enrichedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined)  // person-B: no cache in fillBuffer
        .mockResolvedValueOnce(undefined); // person-B: no cache in pullNext

      // isServed calls:
      //   fillBuffer: person-A cached email → served
      //   pullNext: person-B enriched email → not served
      vi.mocked(db.query.servedLeads.findFirst)
        .mockResolvedValueOnce({
          id: "s-1", organizationId: "org-1", namespace: "campaign-1",
          email: "a@acme.com", externalId: null, metadata: null,
          parentRunId: null, runId: null, brandId: "brand-1",
          campaignId: "campaign-1", servedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined); // person-B not served

      // apolloEnrich for person-B (not cached)
      vi.mocked(apolloEnrich).mockResolvedValue({
        person: { id: "person-B", email: "b@acme.com", firstName: "B" },
      });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("b@acme.com");
      // person-A was never enriched (filtered at buffer fill via cache)
      // person-B was enriched (cache miss)
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledTimes(1);
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledWith("person-B", expect.anything());
    });

    it("fillBufferFromSearch skips people with cached no-email entries", async () => {
      // Search returns 1 person with no email and no cache → inserted
      // Search also returns 1 person with cached no-email → skipped

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [
          { id: "no-email-cached", firstName: "Ghost" },
          { id: "fresh-person", firstName: "Fresh" },
        ],
        done: true,
        totalEntries: 2,
      });

      const freshLeadRow = {
        id: "buf-new",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "fresh-person",
        data: { firstName: "Fresh" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      };

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)     // pullNext buffer empty
        .mockResolvedValueOnce(undefined)     // isInBuffer no-email-cached
        .mockResolvedValueOnce(undefined)     // isInBuffer fresh-person
        .mockResolvedValueOnce(freshLeadRow); // pullNext → fresh-person

      // enrichments cache:
      //   no-email-cached: cached with null email
      //   fresh-person: no cache (in fillBuffer)
      //   fresh-person: no cache (in pullNext)
      vi.mocked(db.query.enrichments.findFirst)
        .mockResolvedValueOnce({
          id: "e-1", email: null, apolloPersonId: "no-email-cached",
          firstName: "Ghost", lastName: null, title: null, linkedinUrl: null,
          organizationName: null, organizationDomain: null,
          organizationIndustry: null, organizationSize: null,
          responseRaw: null, enrichedAt: new Date(),
        })
        .mockResolvedValueOnce(undefined)  // fresh-person in fillBuffer
        .mockResolvedValueOnce(undefined); // fresh-person in pullNext

      vi.mocked(apolloEnrich).mockResolvedValue({
        person: { id: "fresh-person", email: "fresh@acme.com", firstName: "Fresh" },
      });

      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("fresh@acme.com");
      // Only fresh-person was enriched, no-email-cached was skipped
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledTimes(1);
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledWith("fresh-person", expect.anything());
    });

    it("does not permanently block — always retries Apollo on next call", async () => {
      // First pullNext: Apollo returns 0 people → found: false
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue(undefined);
      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [],
        done: true,
        totalEntries: 0,
      });

      const result1 = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });
      expect(result1.found).toBe(false);

      // Second pullNext: Apollo now returns a person → should succeed
      vi.clearAllMocks();

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)   // 1: buffer empty
        .mockResolvedValueOnce(undefined)   // 2: isInBuffer → not in buffer
        .mockResolvedValueOnce({            // 3: pullNext → new lead
          id: "buf-1",
          organizationId: "org-1",
          namespace: "campaign-1",
          campaignId: "campaign-1",
          email: "new@example.com",
          externalId: "apollo-1",
          data: { firstName: "New" },
          status: "buffered",
          pushRunId: null,
          brandId: "brand-1",
          clerkOrgId: null,
          clerkUserId: null,
          createdAt: new Date(),
        });

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-1", email: "new@example.com", firstName: "New" }],
        done: false,
        totalEntries: 1,
      });

      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({
        onConflictDoNothing: onConflictMock,
      });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const setMock = vi.fn().mockReturnValue({
        where: vi.fn().mockResolvedValue(undefined),
      });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result2 = await pullNext({
        organizationId: "org-1",
        campaignId: "campaign-1",
        brandId: "brand-1",
        keySource: "byok",
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result2.found).toBe(true);
      expect(result2.lead?.email).toBe("new@example.com");
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalled();
    });
  });
});
