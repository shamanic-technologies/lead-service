import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the db module
vi.mock("../../src/db/index.js", () => ({
  db: {
    insert: vi.fn(),
  },
  sql: {
    unsafe: vi.fn(),
  },
}));

// Mock the email-gateway-client module
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: vi.fn(),
  isContacted: vi.fn(),
  checkEmailStatus: vi.fn(),
}));

import { db, sql as pgSql } from "../../src/db/index.js";
import { checkContacted, markServed, isAlreadyServedForBrand, checkRaceWindow } from "../../src/lib/dedup.js";
import {
  checkDeliveryStatus,
  checkEmailStatus,
} from "../../src/lib/email-gateway-client.js";

describe("dedup", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("checkContacted", () => {
    it("returns EmailCheckResult for each email from email-gateway", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "alice@acme.com" } as never,
        ],
      });
      vi.mocked(checkEmailStatus).mockReturnValue({ contacted: true, bounced: false, unsubscribed: false });

      const result = await checkContacted(["brand-1"], "campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toEqual({ contacted: true, bounced: false, unsubscribed: false });
      expect(checkDeliveryStatus).toHaveBeenCalledWith("brand-1", "campaign-1", [
        { email: "alice@acme.com" },
      ], undefined);
    });

    it("returns not-contacted result for emails not yet contacted", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "bob@acme.com" } as never,
        ],
      });
      vi.mocked(checkEmailStatus).mockReturnValue({ contacted: false, bounced: false, unsubscribed: false });

      const result = await checkContacted(["brand-1"], "campaign-1", [
        { email: "bob@acme.com" },
      ]);

      expect(result.get("bob@acme.com")).toEqual({ contacted: false, bounced: false, unsubscribed: false });
    });

    it("throws when email-gateway is unreachable (null response)", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue(null);

      await expect(
        checkContacted(["brand-1"], "campaign-1", [
          { email: "alice@acme.com" },
        ])
      ).rejects.toThrow("email-gateway unreachable");
    });

    it("throws when brandIds is empty", async () => {
      await expect(
        checkContacted([], "campaign-1", [
          { email: "alice@acme.com" },
        ])
      ).rejects.toThrow("No brand IDs provided");
    });

    it("handles batch of multiple emails with mixed results", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "alice@acme.com" } as never,
          { email: "bob@acme.com" } as never,
        ],
      });
      vi.mocked(checkEmailStatus)
        .mockReturnValueOnce({ contacted: true, bounced: false, unsubscribed: false })
        .mockReturnValueOnce({ contacted: false, bounced: true, unsubscribed: false });

      const result = await checkContacted(["brand-1"], "campaign-1", [
        { email: "alice@acme.com" },
        { email: "bob@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toEqual({ contacted: true, bounced: false, unsubscribed: false });
      expect(result.get("bob@acme.com")).toEqual({ contacted: false, bounced: true, unsubscribed: false });
    });

    it("uses first brand ID for email-gateway call with multi-brand", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "alice@acme.com" } as never,
        ],
      });
      vi.mocked(checkEmailStatus).mockReturnValue({ contacted: true, bounced: false, unsubscribed: false });

      await checkContacted(["brand-1", "brand-2", "brand-3"], "campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(checkDeliveryStatus).toHaveBeenCalledWith("brand-1", "campaign-1", expect.any(Array), undefined);
    });
  });

  describe("isAlreadyServedForBrand", () => {
    it("returns blocked=false when no match found", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([] as never);

      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
        email: "alice@acme.com",
      });

      expect(result).toEqual({ blocked: false });
    });

    it("returns blocked=true with reason when email matches", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([
        { lead_id: null, email: "alice@acme.com", external_id: null },
      ] as never);

      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
        email: "alice@acme.com",
      });

      expect(result.blocked).toBe(true);
      expect(result.reason).toContain("email");
    });

    it("returns blocked=true when leadId matches", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([
        { lead_id: "lead-1", email: "other@acme.com", external_id: null },
      ] as never);

      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
        leadId: "lead-1",
        email: "alice@acme.com",
      });

      expect(result.blocked).toBe(true);
      expect(result.reason).toContain("lead_id");
    });

    it("returns blocked=true when externalId matches", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([
        { lead_id: null, email: "other@acme.com", external_id: "apollo-123" },
      ] as never);

      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
        externalId: "apollo-123",
      });

      expect(result.blocked).toBe(true);
      expect(result.reason).toContain("external_id");
    });

    it("uses brand_ids && overlap in the query", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([] as never);

      await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1", "brand-2"],
        email: "alice@acme.com",
      });

      const call = vi.mocked(pgSql.unsafe).mock.calls[0];
      expect(call[0]).toContain("brand_ids && $2::text[]");
      expect(call[1]).toEqual(["org-1", "{brand-1,brand-2}", "alice@acme.com"]);
    });

    it("returns blocked=false when brandIds is empty", async () => {
      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: [],
        email: "alice@acme.com",
      });

      expect(result).toEqual({ blocked: false });
      expect(pgSql.unsafe).not.toHaveBeenCalled();
    });

    it("returns blocked=false when no axes provided", async () => {
      const result = await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
      });

      expect(result).toEqual({ blocked: false });
      expect(pgSql.unsafe).not.toHaveBeenCalled();
    });

    it("combines all 3 axes with OR in a single query", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([] as never);

      await isAlreadyServedForBrand({
        orgId: "org-1",
        brandIds: ["brand-1"],
        leadId: "lead-1",
        email: "alice@acme.com",
        externalId: "apollo-123",
      });

      const call = vi.mocked(pgSql.unsafe).mock.calls[0];
      expect(call[0]).toContain("lead_id = $3");
      expect(call[0]).toContain("email = $4");
      expect(call[0]).toContain("external_id = $5");
      expect(call[0]).toContain(" OR ");
    });
  });

  describe("checkRaceWindow", () => {
    it("returns false when no race window conflict", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([] as never);

      const result = await checkRaceWindow({
        orgId: "org-1",
        brandIds: ["brand-1"],
        email: "alice@acme.com",
        excludeBufferId: "buffer-1",
      });

      expect(result).toBe(false);
    });

    it("returns true when a race window conflict exists", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([{ id: "other-buffer" }] as never);

      const result = await checkRaceWindow({
        orgId: "org-1",
        brandIds: ["brand-1"],
        email: "alice@acme.com",
        excludeBufferId: "buffer-1",
      });

      expect(result).toBe(true);
    });

    it("uses brand_ids overlap and excludes current buffer row", async () => {
      vi.mocked(pgSql.unsafe).mockResolvedValue([] as never);

      await checkRaceWindow({
        orgId: "org-1",
        brandIds: ["brand-1", "brand-2"],
        email: "alice@acme.com",
        excludeBufferId: "buffer-1",
      });

      const call = vi.mocked(pgSql.unsafe).mock.calls[0];
      expect(call[0]).toContain("brand_ids && $2::text[]");
      expect(call[0]).toContain("id != $4");
      expect(call[1]).toEqual(["org-1", "{brand-1,brand-2}", "alice@acme.com", "buffer-1"]);
    });

    it("returns false when brandIds is empty", async () => {
      const result = await checkRaceWindow({
        orgId: "org-1",
        brandIds: [],
        email: "alice@acme.com",
        excludeBufferId: "buffer-1",
      });

      expect(result).toBe(false);
      expect(pgSql.unsafe).not.toHaveBeenCalled();
    });
  });

  describe("markServed", () => {
    it("inserts and returns inserted: true on success", async () => {
      const returningMock = vi.fn().mockResolvedValue([{ id: "uuid-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await markServed({
        orgId: "org-1",
        namespace: "campaign-1",
        brandIds: ["brand-1"],
        campaignId: "campaign-1",
        email: "alice@acme.com",
        leadId: "lead-uuid-1",
        externalId: "enrich-123",
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
        orgId: "org-1",
        namespace: "campaign-1",
        brandIds: ["brand-1"],
        campaignId: "campaign-1",
        email: "alice@acme.com",
      });

      expect(result).toEqual({ inserted: false });
    });

    it("passes leadId to the insert when provided", async () => {
      const returningMock = vi.fn().mockResolvedValue([{ id: "uuid-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      await markServed({
        orgId: "org-1",
        namespace: "campaign-1",
        brandIds: ["brand-1"],
        campaignId: "campaign-1",
        email: "alice@acme.com",
        leadId: "lead-uuid-1",
      });

      expect(valuesMock).toHaveBeenCalledWith(
        expect.objectContaining({ leadId: "lead-uuid-1" })
      );
    });

    it("stores multiple brandIds in the insert", async () => {
      const returningMock = vi.fn().mockResolvedValue([{ id: "uuid-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      await markServed({
        orgId: "org-1",
        namespace: "campaign-1",
        brandIds: ["brand-1", "brand-2", "brand-3"],
        campaignId: "campaign-1",
        email: "alice@acme.com",
      });

      expect(valuesMock).toHaveBeenCalledWith(
        expect.objectContaining({ brandIds: ["brand-1", "brand-2", "brand-3"] })
      );
    });
  });
});
