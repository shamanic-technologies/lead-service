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
            leadId: "lead-1",
            email: "alice@acme.com",
            broadcast: {
              campaign: {
                lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
                email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              },
              brand: {
                lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
                email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
              },
              global: {
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

      const result = await checkDeliveryStatus("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(result).toEqual(responseBody);
      expect(mockFetch).toHaveBeenCalledOnce();
      const [url, opts] = mockFetch.mock.calls[0];
      expect(url).toContain("/status");
      expect(opts.method).toBe("POST");
      const body = JSON.parse(opts.body);
      expect(body.brandId).toBe("brand-1");
      expect(body.campaignId).toBe("campaign-1");
      expect(body.items).toEqual([{ leadId: "lead-1", email: "alice@acme.com" }]);
    });

    it("sends brandId without campaignId when campaignId is undefined", async () => {
      mockFetch.mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ results: [] }),
      });

      await checkDeliveryStatus("brand-1", undefined, [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(mockFetch).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          body: JSON.stringify({
            brandId: "brand-1",
            items: [{ leadId: "lead-1", email: "alice@acme.com" }],
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

      const result = await checkDeliveryStatus("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(result).toBeNull();
    });

    it("returns null and logs warn on connection error (fetch failed)", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      mockFetch.mockRejectedValue(new TypeError("fetch failed"));

      const result = await checkDeliveryStatus("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(result).toBeNull();
      expect(warnSpy).toHaveBeenCalledWith(
        "[email-gateway-client] email-gateway unreachable, skipping delivery check"
      );
      expect(errorSpy).not.toHaveBeenCalled();
      warnSpy.mockRestore();
      errorSpy.mockRestore();
    });

    it("returns null and logs error on unexpected errors", async () => {
      const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      const unexpectedError = new Error("something unexpected");
      mockFetch.mockRejectedValue(unexpectedError);

      const result = await checkDeliveryStatus("brand-1", "campaign-1", [
        { leadId: "lead-1", email: "alice@acme.com" },
      ]);

      expect(result).toBeNull();
      expect(errorSpy).toHaveBeenCalledWith(
        "[email-gateway-client] Status check error:",
        unexpectedError
      );
      expect(warnSpy).not.toHaveBeenCalled();
      warnSpy.mockRestore();
      errorSpy.mockRestore();
    });
  });

  describe("isDelivered", () => {
    const emptyScoped = {
      lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
      email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
    };

    const emptyGlobal = {
      email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
    };

    const emptyProvider: ProviderStatus = {
      campaign: emptyScoped,
      brand: emptyScoped,
      global: emptyGlobal,
    };

    it("returns false when nothing is contacted", () => {
      const result: StatusResult = {
        leadId: "lead-1",
        email: "alice@acme.com",
        broadcast: emptyProvider,
        transactional: emptyProvider,
      };
      expect(isDelivered(result)).toBe(false);
    });

    it("returns false when no providers present", () => {
      const result: StatusResult = { leadId: "lead-1", email: "alice@acme.com" };
      expect(isDelivered(result)).toBe(false);
    });

    it("returns true when broadcast campaign lead is contacted", () => {
      const result: StatusResult = {
        leadId: "lead-1",
        email: "alice@acme.com",
        broadcast: {
          ...emptyProvider,
          campaign: {
            ...emptyScoped,
            lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });

    it("returns true when broadcast brand lead is contacted", () => {
      const result: StatusResult = {
        leadId: "lead-1",
        email: "alice@acme.com",
        broadcast: {
          ...emptyProvider,
          brand: {
            ...emptyScoped,
            lead: { contacted: true, delivered: true, replied: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });

    it("returns true when transactional global email is contacted", () => {
      const result: StatusResult = {
        leadId: "lead-1",
        email: "alice@acme.com",
        transactional: {
          ...emptyProvider,
          global: {
            email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });

    it("returns true when broadcast global email is contacted", () => {
      const result: StatusResult = {
        leadId: "lead-1",
        email: "alice@acme.com",
        broadcast: {
          ...emptyProvider,
          global: {
            email: { contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2024-01-01" },
          },
        },
      };
      expect(isDelivered(result)).toBe(true);
    });
  });
});
