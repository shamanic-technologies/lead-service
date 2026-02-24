import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock db
vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
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

// Mock campaign-client
vi.mock("../../src/lib/campaign-client.js", () => ({
  fetchCampaign: vi.fn().mockResolvedValue(null),
}));

// Mock brand-client
vi.mock("../../src/lib/brand-client.js", () => ({
  fetchBrand: vi.fn().mockResolvedValue(null),
}));

// Mock email-gateway-client
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: vi.fn().mockResolvedValue({ results: [] }),
  isDelivered: vi.fn().mockReturnValue(false),
}));

// Mock leads-registry
vi.mock("../../src/lib/leads-registry.js", () => ({
  resolveOrCreateLead: vi.fn().mockResolvedValue({ leadId: "lead-uuid-1", isNew: true }),
  findLeadByApolloPersonId: vi.fn().mockResolvedValue(null),
  findLeadByEmail: vi.fn().mockResolvedValue(null),
}));

import { db } from "../../src/db/index.js";
import { pullNext } from "../../src/lib/buffer.js";
import { apolloSearchNext, apolloSearchParams, apolloEnrich } from "../../src/lib/apollo-client.js";
import { checkDeliveryStatus } from "../../src/lib/email-gateway-client.js";
import { resolveOrCreateLead } from "../../src/lib/leads-registry.js";

describe("buffer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Reset default mocks
    vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });
    vi.mocked(resolveOrCreateLead).mockResolvedValue({ leadId: "lead-uuid-1", isNew: true });
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

    it("returns a lead with leadId and marks it served", async () => {
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

      // email-gateway: not delivered
      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

      // resolveOrCreateLead returns leadId
      vi.mocked(resolveOrCreateLead).mockResolvedValue({ leadId: "lead-abc", isNew: true });

      // markServed insert
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
        parentRunId: "run-1",
        runId: "child-run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.leadId).toBe("lead-abc");
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

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

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
      expect(result.lead?.data).toEqual(apolloData);
    });

    it("skips already-delivered buffer rows and tries next", async () => {
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

      // First lead: delivered, second lead: not delivered
      vi.mocked(checkDeliveryStatus)
        .mockResolvedValueOnce({
          results: [{ email: "alice@acme.com", broadcast: {
            campaign: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
            },
            global: {
              lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
              email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
            },
          }}],
        })
        .mockResolvedValueOnce({ results: [] });

      const { isDelivered } = await import("../../src/lib/email-gateway-client.js");
      vi.mocked(isDelivered)
        .mockReturnValueOnce(true)   // alice: delivered
        .mockReturnValueOnce(false); // bob: not delivered (no results)

      vi.mocked(resolveOrCreateLead)
        .mockResolvedValueOnce({ leadId: "lead-alice", isNew: false })
        .mockResolvedValueOnce({ leadId: "lead-bob", isNew: true });

      const returningMock = vi.fn().mockResolvedValue([{ id: "served-2" }]);
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
      expect(result.lead?.leadId).toBe("lead-bob");
    });

    it("fills buffer from apolloSearchNext when buffer empty and searchParams provided", async () => {
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
        .mockResolvedValueOnce(undefined)   // pullNext buffer empty
        .mockResolvedValueOnce(undefined)   // isInBuffer → not in buffer
        .mockResolvedValueOnce(newLeadRow); // pullNext buffer → new lead

      vi.mocked(apolloSearchParams).mockResolvedValue({ searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1 });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-1", email: "new-lead@example.com", firstName: "New" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

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
      expect(result.lead?.leadId).toBeDefined();
      expect(vi.mocked(apolloSearchParams)).toHaveBeenCalledOnce();
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

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

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
        runId: "run-1",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("ray@provaliant.com");
      expect(result.lead?.leadId).toBeDefined();
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("skips enrichment when cache has no-email entry for person", async () => {
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

      vi.mocked(db.query.enrichments.findFirst).mockResolvedValueOnce({
        id: "e-1", email: null, apolloPersonId: "known-no-email",
        firstName: "Ghost", lastName: null, title: null, linkedinUrl: null,
        organizationName: null, organizationDomain: null,
        organizationIndustry: null, organizationSize: null,
        responseRaw: null, enrichedAt: new Date(),
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

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
      expect(vi.mocked(apolloEnrich)).not.toHaveBeenCalled();
    });

    it("passes workflowName to apolloSearchParams, apolloSearchNext, and apolloEnrich", async () => {
      const newLeadRow = {
        id: "buf-wf",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "",
        externalId: "apollo-wf-1",
        data: { firstName: "Workflow" },
        status: "buffered",
        pushRunId: null,
        brandId: "brand-1",
        clerkOrgId: null,
        clerkUserId: null,
        createdAt: new Date(),
      };

      vi.mocked(db.query.leadBuffer.findFirst)
        .mockResolvedValueOnce(undefined)    // pullNext buffer empty
        .mockResolvedValueOnce(undefined)    // isInBuffer → not in buffer
        .mockResolvedValueOnce(newLeadRow);  // pullNext buffer → new lead

      vi.mocked(apolloSearchParams).mockResolvedValue({
        searchParams: { personTitles: ["CEO"] }, totalResults: 100, attempts: 1,
      });

      vi.mocked(apolloSearchNext).mockResolvedValue({
        people: [{ id: "apollo-wf-1", firstName: "Workflow" }],
        done: true,
        totalEntries: 1,
      });

      vi.mocked(db.query.enrichments.findFirst).mockResolvedValue(undefined);

      vi.mocked(apolloEnrich).mockResolvedValue({
        person: { id: "apollo-wf-1", email: "wf@acme.com", firstName: "Workflow" },
      });

      vi.mocked(checkDeliveryStatus).mockResolvedValue({ results: [] });

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
        workflowName: "cold-email-outreach",
      });

      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("wf@acme.com");

      expect(vi.mocked(apolloSearchParams)).toHaveBeenCalledWith(
        expect.objectContaining({ workflowName: "cold-email-outreach" })
      );
      expect(vi.mocked(apolloSearchNext)).toHaveBeenCalledWith(
        expect.objectContaining({ workflowName: "cold-email-outreach" })
      );
      expect(vi.mocked(apolloEnrich)).toHaveBeenCalledWith(
        "apollo-wf-1",
        expect.objectContaining({ workflowName: "cold-email-outreach" })
      );
    });

    it("continues when email-gateway is unreachable (fallback)", async () => {
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

      // email-gateway is unreachable
      vi.mocked(checkDeliveryStatus).mockResolvedValue(null);

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

      // Should still serve the lead (fallback: not delivered)
      expect(result.found).toBe(true);
      expect(result.lead?.email).toBe("alice@acme.com");
    });
  });
});
