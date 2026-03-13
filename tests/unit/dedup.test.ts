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
  isContacted: vi.fn(),
}));

import { db } from "../../src/db/index.js";
import { checkContacted, markServed } from "../../src/lib/dedup.js";
import {
  checkDeliveryStatus,
  isContacted,
} from "../../src/lib/email-gateway-client.js";

describe("dedup", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("checkContacted", () => {
    it("returns contacted=true for emails that email-gateway reports as contacted", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { leadId: "lead-1", email: "alice@acme.com" } as never,
        ],
      });
      vi.mocked(isContacted).mockReturnValue(true);

      const result = await checkContacted("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(true);
      expect(checkDeliveryStatus).toHaveBeenCalledWith("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ], undefined);
    });

    it("returns contacted=false for emails not yet contacted", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { leadId: "lead-1", email: "bob@acme.com" } as never,
        ],
      });
      vi.mocked(isContacted).mockReturnValue(false);

      const result = await checkContacted("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "bob@acme.com" },
      ]);

      expect(result.get("bob@acme.com")).toBe(false);
    });

    it("returns all-false when email-gateway is unreachable (null response)", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue(null);

      const result = await checkContacted("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
        { leadId: "lead-2", email: "bob@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(false);
      expect(result.get("bob@acme.com")).toBe(false);
    });

    it("logs contacted status for each email", async () => {
      const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { leadId: "lead-1", email: "alice@acme.com" } as never,
        ],
      });
      vi.mocked(isContacted).mockReturnValue(true);

      await checkContacted("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(logSpy).toHaveBeenCalledWith(
        "[dedup] contacted check: email=alice@acme.com contacted=true brandId=brand-1 campaignId=campaign-1"
      );
      logSpy.mockRestore();
    });

    it("handles batch of multiple emails with mixed results", async () => {
      vi.mocked(checkDeliveryStatus).mockResolvedValue({
        results: [
          { leadId: "lead-1", email: "alice@acme.com" } as never,
          { leadId: "lead-2", email: "bob@acme.com" } as never,
        ],
      });
      vi.mocked(isContacted)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(false);

      const result = await checkContacted("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
        { leadId: "lead-2", email: "bob@acme.com" },
      ]);

      expect(result.get("alice@acme.com")).toBe(true);
      expect(result.get("bob@acme.com")).toBe(false);
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
        orgId: "org-1",
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
        orgId: "org-1",
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
