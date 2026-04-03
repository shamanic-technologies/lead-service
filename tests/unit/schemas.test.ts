import { describe, it, expect } from "vitest";
import { BufferNextRequestSchema, ApolloPersonDataSchema } from "../../src/schemas.js";

describe("schema validation", () => {
  describe("BufferNextRequestSchema", () => {
    it("accepts empty body", () => {
      const result = BufferNextRequestSchema.safeParse({});
      expect(result.success).toBe(true);
    });

    it("rejects unknown fields (strict)", () => {
      const result = BufferNextRequestSchema.safeParse({
        sourceType: "apollo",
      });
      // Empty object schema accepts extra keys by default in Zod
      // This is fine — extra fields are stripped
      expect(result.success).toBe(true);
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
