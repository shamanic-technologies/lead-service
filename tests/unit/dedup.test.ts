import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the db module
vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
      servedLeads: {
        findFirst: vi.fn(),
      },
    },
    insert: vi.fn(),
  },
}));

import { db } from "../../src/db/index.js";
import { isServed, markServed } from "../../src/lib/dedup.js";

describe("dedup", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("isServed", () => {
    it("returns true when email already served for org+brand", async () => {
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue({
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
      });

      const result = await isServed("org-1", "brand-1", "alice@acme.com");
      expect(result).toBe(true);
    });

    it("returns false when email not served", async () => {
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      const result = await isServed("org-1", "brand-1", "alice@acme.com");
      expect(result).toBe(false);
    });

    it("scopes by brand — same email in different brand is not served", async () => {
      vi.mocked(db.query.servedLeads.findFirst).mockResolvedValue(undefined);

      const result = await isServed("org-1", "brand-2", "alice@acme.com");
      expect(result).toBe(false);
      expect(db.query.servedLeads.findFirst).toHaveBeenCalledOnce();
    });
  });

  describe("markServed", () => {
    it("inserts and returns inserted: true on success", async () => {
      const returningMock = vi.fn().mockResolvedValue([{ id: "uuid-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await markServed({
        organizationId: "org-1",
        namespace: "campaign-1",
        brandId: "brand-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        externalId: "enrich-123",
        parentRunId: "run-1",
        runId: "child-run-1",
      });

      expect(result).toEqual({ inserted: true });
    });

    it("returns inserted: false on conflict (already served)", async () => {
      const returningMock = vi.fn().mockResolvedValue([]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await markServed({
        organizationId: "org-1",
        namespace: "campaign-1",
        brandId: "brand-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
      });

      expect(result).toEqual({ inserted: false });
    });
  });
});
