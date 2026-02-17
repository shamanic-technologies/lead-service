import { describe, it, expect } from "vitest";
import { BufferPushRequestSchema, BufferNextRequestSchema } from "../../src/schemas.js";

describe("schema validation", () => {
  describe("BufferPushRequestSchema", () => {
    it("rejects empty brandId", () => {
      const result = BufferPushRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "",
        parentRunId: "r1",
        leads: [],
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        const fields = result.error.flatten().fieldErrors;
        expect(fields.brandId).toBeDefined();
      }
    });

    it("rejects empty campaignId", () => {
      const result = BufferPushRequestSchema.safeParse({
        campaignId: "",
        brandId: "b1",
        parentRunId: "r1",
        leads: [],
      });
      expect(result.success).toBe(false);
    });

    it("rejects empty parentRunId", () => {
      const result = BufferPushRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "",
        leads: [],
      });
      expect(result.success).toBe(false);
    });

    it("accepts valid request", () => {
      const result = BufferPushRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        leads: [{ email: "test@example.com" }],
      });
      expect(result.success).toBe(true);
    });
  });

  describe("BufferNextRequestSchema", () => {
    it("rejects empty brandId", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "",
        parentRunId: "r1",
      });
      expect(result.success).toBe(false);
      if (!result.success) {
        const fields = result.error.flatten().fieldErrors;
        expect(fields.brandId).toBeDefined();
      }
    });

    it("rejects empty campaignId", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "",
        brandId: "b1",
        parentRunId: "r1",
      });
      expect(result.success).toBe(false);
    });

    it("rejects empty parentRunId", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "",
      });
      expect(result.success).toBe(false);
    });

    it("accepts valid request", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
      });
      expect(result.success).toBe(true);
    });

    it("accepts optional idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        idempotencyKey: "run-123",
      });
      expect(result.success).toBe(true);
    });

    it("rejects empty idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        idempotencyKey: "",
      });
      expect(result.success).toBe(false);
    });
  });
});
