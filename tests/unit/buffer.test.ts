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
import { apolloSearchNext, apolloSearchParams } from "../../src/lib/apollo-client.js";

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
        searchParams: { description: "impossible search" },
        appId: "my-app",
      });

      expect(result.found).toBe(false);
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalled();
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
        searchParams: { description: "tech CEOs" },
        appId: "my-app",
      });

      expect(result2.found).toBe(true);
      expect(result2.lead?.email).toBe("new@example.com");
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalled();
    });
  });
});
