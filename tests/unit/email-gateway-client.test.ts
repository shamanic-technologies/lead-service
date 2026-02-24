import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  checkDeliveryStatus,
  isDelivered,
  type StatusResult,
  type ProviderStatus,
} from "../../src/lib/email-gateway-client.js";

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

describe("email-gateway-client", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe("checkDeliveryStatus", () => {
    it("returns status results on success", async () => {
      const responseBody = {
        results: [
          {
            email: "alice@acme.com",
            broadcast: {
              campaign: {
                lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
                email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              },
              global: {
                lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
                email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              },
            },
          },
        ],
      };

      mockFetch.mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(responseBody),
      });

      const result = await checkDeliveryStatus("campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(result).toEqual(responseBody);
      expect(mockFetch).toHaveBeenCalledWith(
        expect.stringContaining("/status"),
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({
            campaignId: "campaign-1",
            items: [{ email: "alice@acme.com" }],
          }),
        })
      );
    });

    it("returns null on non-200 response", async () => {
      mockFetch.mockResolvedValue({
        ok: false,
        status: 500,
        text: () => Promise.resolve("Internal server error"),
      });

      const result = await checkDeliveryStatus("campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(result).toBeNull();
    });

    it("returns null on network error", async () => {
      mockFetch.mockRejectedValue(new Error("ECONNREFUSED"));

      const result = await checkDeliveryStatus("campaign-1", [
        { email: "alice@acme.com" },
      ]);

      expect(result).toBeNull();
    });

    it("passes leadId when provided", async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ results: [] }),
      });

      await checkDeliveryStatus("campaign-1", [
        { leadId: "lead-123", email: "alice@acme.com" },
      ]);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: JSON.stringify({
            campaignId: "campaign-1",
            items: [{ leadId: "lead-123", email: "alice@acme.com" }],
          }),
        })
      );
    });
  });

  describe("isDelivered", () => {
    const emptyScope: ProviderStatus = {
      campaign: {
        lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
        email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
      },
      global: {
        lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
        email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
      },
    };

    it("returns false when nothing is contacted", () => {
      const result: StatusResult = {
        email: "alice@acme.com",
        broadcast: emptyScope,
        transactional: emptyScope,
      };
      expect(isDelivered(result)).toBe(false);
    });

    it("returns false when no providers present", () => {
      const result: StatusResult = { email: "alice@acme.com" };
      expect(isDelivered(result)).toBe(false);
    });

    it("returns true when broadcast campaign lead is contacted", () => {
      const result: StatusResult = {
        email: "alice@acme.com",
        broadcast: {
          ...emptyScope,
          campaign: {
            ...emptyScope.campaign,
            lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });

    it("returns true when transactional global email is contacted", () => {
      const result: StatusResult = {
        email: "alice@acme.com",
        transactional: {
          ...emptyScope,
          global: {
            ...emptyScope.global,
            email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });
  });
});
