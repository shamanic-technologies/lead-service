import { describe, it, expect } from "vitest";
import { BufferNextRequestSchema } from "../../src/schemas.js";

describe("schema validation", () => {
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
        keySource: "byok",
      });
      expect(result.success).toBe(true);
    });

    it("accepts optional idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        keySource: "byok",
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

    it("accepts optional workflowName", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        keySource: "byok",
        workflowName: "cold-email-outreach",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.workflowName).toBe("cold-email-outreach");
      }
    });

    it("accepts request without workflowName", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        keySource: "app",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.workflowName).toBeUndefined();
      }
    });
  });
});
