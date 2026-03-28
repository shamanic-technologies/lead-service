import { describe, it, expect } from "vitest";
import { BufferNextRequestSchema, ApolloPersonDataSchema } from "../../src/schemas.js";

describe("schema validation", () => {
  describe("BufferNextRequestSchema", () => {
    it("rejects empty brandId", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "",
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
      });
      expect(result.success).toBe(false);
    });

    it("accepts valid request", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
      });
      expect(result.success).toBe(true);
    });

    it("accepts optional idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        idempotencyKey: "run-123",
      });
      expect(result.success).toBe(true);
    });

    it("rejects empty idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        idempotencyKey: "",
      });
      expect(result.success).toBe(false);
    });

    it("accepts optional workflowSlug", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        workflowSlug: "cold-email-outreach",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.workflowSlug).toBe("cold-email-outreach");
      }
    });

    it("accepts null searchParams", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        searchParams: null,
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.searchParams).toBeNull();
      }
    });

    it("accepts optional featureInput", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        featureInput: { companyContext: "AI startup", industry: "Technology" },
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.featureInput).toEqual({ companyContext: "AI startup", industry: "Technology" });
      }
    });

    it("accepts null featureInput", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
        featureInput: null,
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.featureInput).toBeNull();
      }
    });

    it("accepts request without workflowSlug", () => {
      const result = BufferNextRequestSchema.safeParse({
        campaignId: "c1",
        brandId: "b1",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.workflowSlug).toBeUndefined();
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
