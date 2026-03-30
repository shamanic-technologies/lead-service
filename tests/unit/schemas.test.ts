import { describe, it, expect } from "vitest";
import { BufferNextRequestSchema, ApolloPersonDataSchema } from "../../src/schemas.js";

describe("schema validation", () => {
  describe("BufferNextRequestSchema", () => {
    it("rejects missing sourceType", () => {
      const result = BufferNextRequestSchema.safeParse({});
      expect(result.success).toBe(false);
    });

    it("accepts optional idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "apollo",
        idempotencyKey: "run-123",
      });
      expect(result.success).toBe(true);
    });

    it("rejects empty idempotencyKey", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "apollo",
        idempotencyKey: "",
      });
      expect(result.success).toBe(false);
    });

    it("accepts sourceType apollo", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "apollo",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.sourceType).toBe("apollo");
      }
    });

    it("accepts sourceType journalist", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "journalist",
      });
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.sourceType).toBe("journalist");
      }
    });

    it("rejects invalid sourceType", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "invalid",
      });
      expect(result.success).toBe(false);
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
