import { describe, it, expect } from "vitest";
import {
  LEAD_EXPORT_COLUMNS,
  formatExportCell,
  leadExportHeader,
  leadExportLine,
} from "../../src/lib/lead-export.js";

/** The shape the JSON list emits, as far as the file is concerned. */
const item = {
  id: "row-1",
  leadId: "lead-1",
  campaignId: "camp-1",
  email: "sara@cascobay.com",
  emailStatus: "verified",
  status: "served",
  servedAt: "2026-08-01T09:15:30.123Z",
  standing: { state: "engaged", signal: "negative_reply" },
  audience: { id: "aud-1", name: "Boatyard owners" },
  offer: { id: "off-1", name: "Winter refit" },
  lead: {
    firstName: "Sara",
    lastName: "Quinn",
    currentTitle: "Owner",
    headline: "Owner at Casco Bay",
    seniority: "owner",
    departments: ["operations", "sales"],
    functions: ["business_development"],
    linkedinUrl: "https://linkedin.com/in/saraquinn",
    city: "Portland",
    state: "Maine",
    country: "United States",
    organization: {
      name: "Casco Bay Boatworks",
      primaryDomain: "cascobay.com",
      industry: "maritime",
      annualRevenue: "4000000",
      estimatedNumEmployees: 42,
      foundedYear: 1998,
      city: "Portland",
      state: "Maine",
      country: "United States",
    },
  },
  contacted: true,
  sent: true,
  delivered: true,
  opened: false,
  clicked: true,
  replied: true,
  replyClassification: "negative",
  bounced: false,
  unsubscribed: false,
  firstContactedAt: "2026-08-01T10:00:00.000Z",
  firstSentAt: "2026-08-01T10:00:00.000Z",
  firstDeliveredAt: "2026-08-01T10:00:05.000Z",
  firstOpenedAt: null,
  firstClickedAt: "2026-08-02T14:22:00.000Z",
  firstRepliedAt: "2026-08-03T08:00:00.000Z",
  firstBouncedAt: null,
  firstUnsubscribedAt: null,
};

function cells(line: string): string[] {
  return line.trim().split('","').map((c) => c.replace(/^"|"$/g, ""));
}

function cellFor(header: string): string {
  const index = LEAD_EXPORT_COLUMNS.findIndex((c) => c.header === header);
  expect(index).toBeGreaterThanOrEqual(0);
  return cells(leadExportLine(item))[index];
}

describe("the export a customer opens", () => {
  it("heads its columns in the words the Leads page uses", () => {
    const headers = cells(leadExportHeader());
    expect(headers).toEqual([
      "First name", "Last name", "Email", "Email status", "Title", "Headline", "Seniority",
      "Departments", "Functions", "LinkedIn", "City", "State", "Country", "Company",
      "Company domain", "Company industry", "Company revenue", "Company employees",
      "Company founded", "Company city", "Company state", "Company country", "Audience", "Offer",
      "Status", "Standing", "Standing signal", "Contacted", "Sent", "Delivered", "Opened",
      "Website visit", "Replied", "Reply sentiment", "Bounced", "Unsubscribed", "Served at",
      "First contacted at", "First sent at", "First delivered at", "First opened at",
      "First website visit at", "First replied at", "First bounced at", "First unsubscribed at",
    ]);
  });

  it("names no API field and no internal identifier", () => {
    const header = leadExportHeader();
    for (const apiWord of ["clicked", "replyClassification", "leadId", "campaignId", "firstClickedAt", "emailStatus"]) {
      expect(header).not.toContain(apiWord);
    }
  });

  it("reads a yes/no fact as a word, never as a raw boolean token", () => {
    expect(cellFor("Contacted")).toBe("Yes");
    expect(cellFor("Opened")).toBe("No");
    expect(cellFor("Website visit")).toBe("Yes");
    expect(leadExportLine(item)).not.toContain('"true"');
    expect(leadExportLine(item)).not.toContain('"false"');
  });

  it("reads an instant as a date a spreadsheet parses, and an absent one as empty", () => {
    expect(cellFor("Served at")).toBe("2026-08-01 09:15:30");
    expect(cellFor("First website visit at")).toBe("2026-08-02 14:22:00");
    expect(cellFor("First opened at")).toBe("");
  });

  it("carries the person, the company, the audience and where they stand", () => {
    expect(cellFor("First name")).toBe("Sara");
    expect(cellFor("Email")).toBe("sara@cascobay.com");
    expect(cellFor("Title")).toBe("Owner");
    expect(cellFor("Departments")).toBe("operations, sales");
    expect(cellFor("Company")).toBe("Casco Bay Boatworks");
    expect(cellFor("Company employees")).toBe("42");
    expect(cellFor("Audience")).toBe("Boatyard owners");
    expect(cellFor("Standing")).toBe("engaged");
    expect(cellFor("Reply sentiment")).toBe("negative");
  });

  it("writes empty cells for a row that carries nothing, never the word null", () => {
    const line = leadExportLine({ email: "", lead: null, standing: null, audience: null, offer: null });
    const values = cells(line);
    expect(values).toHaveLength(LEAD_EXPORT_COLUMNS.length);
    expect(line).not.toContain("null");
    expect(line).not.toContain("undefined");
    // A yes/no fact with no evidence behind it is "No"; everything else is blank.
    expect(new Set(values)).toEqual(new Set(["", "No"]));
  });

  it("escapes a quote the only way RFC 4180 asks for", () => {
    const line = leadExportLine({ ...item, lead: { ...item.lead, firstName: 'Sa"ra' } });
    expect(line.startsWith('"Sa""ra"')).toBe(true);
  });

  it("refuses a timestamp that is not one rather than writing it through as text", () => {
    expect(() => formatExportCell("datetime", "not-a-date")).toThrow(/invalid export timestamp/);
  });
});
