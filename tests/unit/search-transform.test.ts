import { describe, it, expect } from "vitest";
import { buildSystemPrompt } from "../../src/lib/search-transform.js";

const sampleIndustries = [
  { id: "1", name: "Computer Software" },
  { id: "2", name: "Information Technology and Services" },
  { id: "3", name: "Non-Profit Organization Management" },
];

const sampleRanges = [
  { value: "1,10", label: "1-10" },
  { value: "11,20", label: "11-20" },
  { value: "51,100", label: "51-100" },
];

describe("buildSystemPrompt", () => {
  const prompt = buildSystemPrompt(sampleIndustries, sampleRanges);

  it("documents that filters combine with AND between fields", () => {
    expect(prompt).toContain("BETWEEN different fields: **AND**");
  });

  it("documents that values within a field combine with OR", () => {
    expect(prompt).toContain("WITHIN a single field: **OR**");
  });

  it("warns against using too many filters", () => {
    expect(prompt).toContain("NEVER use more than 3 filters at once");
  });

  it("warns against combining keyword tags with industry tags", () => {
    expect(prompt).toContain("Do NOT combine qOrganizationKeywordTags with qOrganizationIndustryTagIds");
  });

  it("includes a bad example showing the 0-results problem", () => {
    expect(prompt).toContain("BAD example");
    expect(prompt).toContain("0 results");
  });

  it("includes good examples with broad queries", () => {
    expect(prompt).toContain("GOOD example");
    expect(prompt).toContain("Only 2 AND'd filters");
  });

  it("lists all available search fields", () => {
    // Person filters
    expect(prompt).toContain("personTitles");
    expect(prompt).toContain("personLocations");
    expect(prompt).toContain("personSeniorities");
    expect(prompt).toContain("contactEmailStatus");
    // Organization filters
    expect(prompt).toContain("organizationLocations");
    expect(prompt).toContain("qOrganizationIndustryTagIds");
    expect(prompt).toContain("organizationNumEmployeesRanges");
    expect(prompt).toContain("qOrganizationKeywordTags");
    expect(prompt).toContain("qOrganizationDomains");
    expect(prompt).toContain("organizationIds");
    expect(prompt).toContain("revenueRange");
    expect(prompt).toContain("currentlyUsingAnyOfTechnologyUids");
    // General
    expect(prompt).toContain("qKeywords");
  });

  it("documents valid enum values for personSeniorities", () => {
    expect(prompt).toContain('"entry"');
    expect(prompt).toContain('"director"');
    expect(prompt).toContain('"c_suite"');
    expect(prompt).toContain('"founder"');
  });

  it("documents valid enum values for contactEmailStatus", () => {
    expect(prompt).toContain('"verified"');
    expect(prompt).toContain('"guessed"');
    expect(prompt).toContain('"unavailable"');
  });

  it("includes the provided industries", () => {
    expect(prompt).toContain('"Computer Software"');
    expect(prompt).toContain('"Non-Profit Organization Management"');
  });

  it("includes the provided employee ranges", () => {
    expect(prompt).toContain('"1,10"');
    expect(prompt).toContain('"51,100"');
  });
});
