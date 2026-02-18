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

import { db } from "../../src/db/index.js";
import { pushLeads, pullNext } from "../../src/lib/buffer.js";

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
      expect(result.lead?.data).toEqual({ organization: {}, name: "Alice" });
    });

    it("defaults organization to {} when missing from lead data", async () => {
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "svitlana@hashtagweb3.com",
        externalId: "e-1",
        data: { first_name: "Svitlana", organization_name: "HashtagWeb3" },
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
      const data = result.lead?.data as Record<string, unknown>;
      // organization must always be an object so workflows can access organization.primary_domain
      expect(data.organization).toEqual({});
      expect(data.first_name).toBe("Svitlana");
    });

    it("preserves existing organization object in lead data", async () => {
      vi.mocked(db.query.leadBuffer.findFirst).mockResolvedValue({
        id: "buf-1",
        organizationId: "org-1",
        namespace: "campaign-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        externalId: "e-1",
        data: {
          first_name: "Alice",
          organization: { primary_domain: "acme.com", industry: "Software" },
        },
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
      const data = result.lead?.data as Record<string, unknown>;
      // Existing organization data should be preserved, not overwritten with {}
      expect(data.organization).toEqual({ primary_domain: "acme.com", industry: "Software" });
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
  });
});
