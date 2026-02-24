import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock the db module
vi.mock("../../src/db/index.js", () => ({
  db: {
    insert: vi.fn(),
  },
}));

// Mock the email-gateway-client module
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: vi.fn(),
  isDelivered: vi.fn(),
}));

import { db } from "../../src/db/index.js";
import { checkDelivered, markServed } from "../../src/lib/dedup.js";
import {
  checkDeliveryStatus,
  isDelivered,
} from "../../src/lib/email-gateway-client.js";

describe("dedup", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("checkDelivered", () => {
    it("returns delivered=true for emails that email-gateway reports as delivered", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "alice@acme.com", lead: null, emailResult: null } as never,
        ],
      });
      vi.mocked(isDelivered).mockReturnValue(true);

      const result = await checkDelivered("campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(true);
      expect(checkDeliveryStatus).toHaveBeenCalledWith("campaign-1", [
        { email: "alice@acme.com" },
      ]);
    });

    it("returns delivered=false for emails not yet delivered", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "bob@acme.com", lead: null, emailResult: null } as never,
        ],
      });
      vi.mocked(isDelivered).mockReturnValue(false);

      const result = await checkDelivered("campaign-1", [
        { email: "bob@acme.com" },
      ]);

      expect(result.get("bob@acme.com")).toBe(false);
    });

    it("returns all-false when email-gateway is unreachable (null response)", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue(null);

      const result = await checkDelivered("campaign-1", [
        { email: "alice@acme.com" },
        { email: "bob@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(false);
      expect(result.get("bob@acme.com")).toBe(false);
    });

    it("handles batch of multiple emails with mixed results", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { email: "alice@acme.com" } as never,
          { email: "bob@acme.com" } as never,
        ],
      });
      vi.mocked(isDelivered)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(false);

      const result = await checkDelivered("campaign-1", [
        { email: "alice@acme.com" },
        { email: "bob@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(true);
      expect(result.get("bob@acme.com")).toBe(false);
    });

    it("passes leadId when provided in items", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [{ email: "alice@acme.com" } as never],
      });
      vi.mocked(isDelivered).mockReturnValue(false);

      await checkDelivered("campaign-1", [
        { email: "alice@acme.com", leadId: "lead-uuid-1" },
      ]);

      expect(checkDeliveryStatus).toHaveBeenCalledWith("campaign-1", [
        { email: "alice@acme.com", leadId: "lead-uuid-1" },
      ]);
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
        leadId: "lead-uuid-1",
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

    it("passes leadId to the insert when provided", async () => {
      const returningMock = vi.fn().mockResolvedValue([{ id: "uuid-1" }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      await markServed({
        organizationId: "org-1",
        namespace: "campaign-1",
        brandId: "brand-1",
        campaignId: "campaign-1",
        email: "alice@acme.com",
        leadId: "lead-uuid-1",
      });

      expect(valuesMock).toHaveBeenCalledWith(
        expect.objectContaining({ leadId: "lead-uuid-1" })
      );
    });
  });
});
