import { describe, it, expect } from "vitest";
import { BufferNextRequestSchema, ApolloPersonDataSchema } from "../../src/schemas.js";

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

    it("accepts keySource 'platform'", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        parentRunId: "r1",
        keySource: "platform",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.keySource).toBe("platform");
      }
    });
  });

  describe("ApolloPersonDataSchema", () => {
    const validPerson = {
      firstName: "Sara",
      lastName: "Freshley",
      organizationName: "Casco Bay",
    };

    it("accepts valid person data with required fields", () => {
      const result = ApolloPersonDataSchema.safeParse(validPerson);
      expect(result.success).toBe(true);
    });

    it("rejects missing firstName", () => {
      const { firstName, ...rest } = validPerson;
      const result = ApolloPersonDataSchema.safeParse(rest);
      expect(result.success).toBe(false);
    });

    it("rejects missing lastName", () => {
      const { lastName, ...rest } = validPerson;
      const result = ApolloPersonDataSchema.safeParse(rest);
      expect(result.success).toBe(false);
    });

    it("rejects missing organizationName", () => {
      const { organizationName, ...rest } = validPerson;
      const result = ApolloPersonDataSchema.safeParse(rest);
      expect(result.success).toBe(false);
    });

    it("rejects null firstName", () => {
      const result = ApolloPersonDataSchema.safeParse({ ...validPerson, firstName: null });
      expect(result.success).toBe(false);
    });

    it("rejects null lastName", () => {
      const result = ApolloPersonDataSchema.safeParse({ ...validPerson, lastName: null });
      expect(result.success).toBe(false);
    });

    it("rejects null organizationName", () => {
      const result = ApolloPersonDataSchema.safeParse({ ...validPerson, organizationName: null });
      expect(result.success).toBe(false);
    });
  });
});
