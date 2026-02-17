import { describe, it, expect, vi } from "vitest";

// Mock db to avoid needing LEAD_SERVICE_DATABASE_URL
vi.mock("../../src/db/index.js", () => ({
  db: {},
}));

import { extractEnrichment } from "../../src/routes/leads.js";

describe("extractEnrichment", () => {
  it("returns null for null metadata", () => {
    expect(extractEnrichment(null)).toBeNull();
  });

  it("returns null for undefined metadata", () => {
    expect(extractEnrichment(undefined)).toBeNull();
  });

  it("returns null for non-object metadata", () => {
    expect(extractEnrichment("string")).toBeNull();
    expect(extractEnrichment(42)).toBeNull();
  });

  it("returns null when no person identifiers exist", () => {
    expect(extractEnrichment({ organizationName: "Acme" })).toBeNull();
  });

  it("passes through ALL fields from metadata without filtering", () => {
    const metadata = {
      firstName: "Diana",
      lastName: "Prince",
      email: "diana@example.com",
      title: "CEO",
      linkedinUrl: "https://linkedin.com/in/diana",
      organizationName: "Themyscira Inc",
      organizationDomain: "themyscira.com",
      organizationIndustry: "Defense",
      organizationSize: "501-1000",
      // Extra fields that should NOT be filtered
      headline: "CEO & Founder at Themyscira Inc",
      city: "Gateway City",
      state: "CA",
      country: "United States",
      organizationShortDescription: "Leading defense tech company",
      organizationFoundedYear: 2010,
      organizationRevenueUsd: "50000000",
      seniority: "founder",
      departments: ["executive"],
      photoUrl: "https://example.com/diana.jpg",
      twitterUrl: "https://twitter.com/diana",
      facebookUrl: "https://facebook.com/diana",
      organizationLogoUrl: "https://example.com/logo.png",
      organizationTotalFunding: 25000000,
      organizationLatestFundingRound: "Series C",
      organizationTechnologies: ["React", "Node.js", "PostgreSQL"],
    };

    const result = extractEnrichment(metadata);
    expect(result).not.toBeNull();

    // Standard fields
    expect(result!.firstName).toBe("Diana");
    expect(result!.lastName).toBe("Prince");
    expect(result!.title).toBe("CEO");
    expect(result!.organizationName).toBe("Themyscira Inc");

    // Extra fields — must all pass through
    expect(result!.headline).toBe("CEO & Founder at Themyscira Inc");
    expect(result!.city).toBe("Gateway City");
    expect(result!.state).toBe("CA");
    expect(result!.country).toBe("United States");
    expect(result!.organizationShortDescription).toBe("Leading defense tech company");
    expect(result!.organizationFoundedYear).toBe(2010);
    expect(result!.organizationRevenueUsd).toBe("50000000");
    expect(result!.seniority).toBe("founder");
    expect(result!.departments).toEqual(["executive"]);
    expect(result!.photoUrl).toBe("https://example.com/diana.jpg");
    expect(result!.twitterUrl).toBe("https://twitter.com/diana");
    expect(result!.organizationLogoUrl).toBe("https://example.com/logo.png");
    expect(result!.organizationTotalFunding).toBe(25000000);
    expect(result!.organizationLatestFundingRound).toBe("Series C");
    expect(result!.organizationTechnologies).toEqual(["React", "Node.js", "PostgreSQL"]);
  });

  it("works with minimal metadata (just firstName)", () => {
    const result = extractEnrichment({ firstName: "Alice" });
    expect(result).not.toBeNull();
    expect(result!.firstName).toBe("Alice");
  });
});
