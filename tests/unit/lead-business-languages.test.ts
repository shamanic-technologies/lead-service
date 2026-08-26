import { describe, it, expect } from "vitest";
import { PERSON_FIELDS_FOR_TEST } from "../../src/lib/leads-registry.js";
import { leads } from "../../src/db/schema.js";
import { FullLeadSchema } from "../../src/schemas.js";

/**
 * lead-service CARRIES the person's business languages; it never derives them.
 * human-service owns the derivation and the vocabulary. What must hold here is
 * only that the value survives the trip intact — above all its ORDER, because
 * the end consumer selects by position.
 */
describe("businessLanguages is carried, not derived", () => {
  it("is written from the person by the field registry", () => {
    // Presence in the registry IS the write path — pickPersonFields copies every
    // listed key straight off the neutral Person onto the lead row.
    expect(PERSON_FIELDS_FOR_TEST).toContain("businessLanguages");
  });

  it("is stored in an ORDER-PRESERVING column", () => {
    // A Postgres array keeps insertion order; a set or a sorted column would not,
    // and the consumer reads index 0 as "the most plausible language".
    const col = (leads as unknown as Record<string, { columnType?: string }>)
      .businessLanguages;
    expect(col).toBeDefined();
    expect(col.columnType).toMatch(/Array/i);
  });

  it("does not derive anything — no language table lives in this repo", async () => {
    // Guard against the tempting-but-wrong fix of re-deriving from lead geography.
    const registry = await import("../../src/lib/leads-registry.js");
    expect(Object.keys(registry).join(" ")).not.toMatch(/derive.*language/i);
  });
});

describe("FullLead exposes businessLanguages faithfully", () => {
  const base = {
    leadId: "lead-1",
    firstName: null,
    lastName: null,
    name: null,
    headline: null,
    linkedinUrl: null,
    photoUrl: null,
    city: null,
    state: null,
    country: null,
    timezone: null,
    seniority: null,
    departments: null,
    subdepartments: null,
    functions: null,
    twitterUrl: null,
    githubUrl: null,
    facebookUrl: null,
  };

  function parse(businessLanguages: unknown) {
    return FullLeadSchema.partial()
      .pick({ businessLanguages: true })
      .parse({ ...base, businessLanguages });
  }

  it("accepts an ordered list and preserves the order exactly", () => {
    expect(parse(["de", "en"]).businessLanguages).toEqual(["de", "en"]);
    // Same set, other order — must come back different, or the guarantee is dead.
    expect(parse(["en", "de"]).businessLanguages).toEqual(["en", "de"]);
  });

  it("accepts the empty array — the producer's honest 'unknown'", () => {
    expect(parse([]).businessLanguages).toEqual([]);
  });

  it("accepts null for a lead that predates the field being carried", () => {
    expect(parse(null).businessLanguages).toBeNull();
  });

  it("keeps unknown distinguishable from known-English", () => {
    // The two must never collapse: [] means "no signal", ["en"] means "English".
    expect(parse([]).businessLanguages).not.toEqual(parse(["en"]).businessLanguages);
  });
});
