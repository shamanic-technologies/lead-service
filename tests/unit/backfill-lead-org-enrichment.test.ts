import { describe, it, expect } from "vitest";
import {
  parseArgs,
  parseInput,
  indexRecords,
  coerceOrgValue,
  columnsToFill,
  normalizeEmployment,
  toIsoDate,
  ORG_COLUMNS,
  DEFAULT_BATCH_SIZE,
  DEFAULT_CONCURRENCY,
} from "../../scripts/backfill-lead-org-enrichment.js";

/** One line as `\copy … CSV` writes a single JSON column: quoted, quotes doubled. */
function csvLine(obj: unknown): string {
  return `"${JSON.stringify(obj).replace(/"/g, '""')}"`;
}

describe("parseArgs", () => {
  it("defaults to a real batch size and no limit", () => {
    const args = parseArgs(["--input", "/tmp/x.jsonl"]);
    expect(args).toMatchObject({
      dryRun: false,
      inputPath: "/tmp/x.jsonl",
      reportPath: null,
      batchSize: DEFAULT_BATCH_SIZE,
      limit: null,
      concurrency: DEFAULT_CONCURRENCY,
    });
  });

  it("refuses a batch size that is not a positive integer", () => {
    expect(() => parseArgs(["--batch-size", "0"])).toThrow(/positive integer/);
    expect(() => parseArgs(["--batch-size", "abc"])).toThrow(/positive integer/);
  });

  it("refuses a concurrency that is not a positive integer", () => {
    expect(() => parseArgs(["--concurrency", "0"])).toThrow(/positive integer/);
    expect(() => parseArgs(["--concurrency", "-2"])).toThrow(/positive integer/);
  });

  it("stays well under the pool ceiling so the service keeps serving", () => {
    expect(DEFAULT_CONCURRENCY).toBeLessThan(20);
  });
});

describe("parseInput", () => {
  const record = {
    apolloPersonId: "p-1",
    email: "Ada@Example.com",
    name: 'The "Big" Co',
    shortDescription: "Sells, mostly.",
    keywords: ["a", "b"],
    employmentHistory: [
      { organization_name: "The Big Co", title: "CTO", start_date: "2020-02-01", current: true },
      { organization_name: "Old Co", title: "Dev", start_date: "2015-01-01", end_date: "2020-01-31" },
    ],
  };

  it("reads a CSV-quoted JSON line, un-doubling the quotes", () => {
    const [parsed] = parseInput(csvLine(record));
    expect(parsed.apolloPersonId).toBe("p-1");
    expect(parsed.org.name).toBe('The "Big" Co');
  });

  it("lower-cases the email so a lead can be matched on it", () => {
    const [parsed] = parseInput(csvLine(record));
    expect(parsed.email).toBe("ada@example.com");
  });

  it("normalizes every employment entry, snake_case included", () => {
    const [parsed] = parseInput(csvLine(record));
    expect(parsed.employmentHistory).toEqual([
      {
        organizationName: "The Big Co",
        title: "CTO",
        startDate: "2020-02-01",
        endDate: null,
        current: true,
        description: null,
      },
      {
        organizationName: "Old Co",
        title: "Dev",
        startDate: "2015-01-01",
        endDate: "2020-01-31",
        current: false,
        description: null,
      },
    ]);
  });

  it("drops a role naming no employer — there is no organization to key it on", () => {
    const [parsed] = parseInput(
      csvLine({ apolloPersonId: "p-2", employmentHistory: [{ title: "Something" }] }),
    );
    expect(parsed.employmentHistory).toEqual([]);
  });

  it("aborts on a malformed line rather than silently shrinking the repair", () => {
    expect(() => parseInput("not json at all")).toThrow(/not JSON/);
    expect(() => parseInput(csvLine([1, 2, 3]))).toThrow(/not a JSON object/);
    expect(() => parseInput(csvLine({ name: "keyless" }))).toThrow(
      /neither apolloPersonId nor email/,
    );
  });

  it("skips blank lines", () => {
    expect(parseInput(`\n${csvLine(record)}\n\n`)).toHaveLength(1);
  });
});

describe("indexRecords", () => {
  it("indexes by both keys a lead can be matched on", () => {
    const records = parseInput(
      [
        csvLine({ apolloPersonId: "p-1", email: "a@x.com" }),
        csvLine({ apolloPersonId: null, email: "b@x.com" }),
      ].join("\n"),
    );
    const { byPersonId, byEmail } = indexRecords(records);
    expect([...byPersonId.keys()]).toEqual(["p-1"]);
    expect([...byEmail.keys()].sort()).toEqual(["a@x.com", "b@x.com"]);
  });
});

describe("coerceOrgValue", () => {
  it("keeps a stated zero and refuses an empty array", () => {
    expect(coerceOrgValue("int", 0)).toBe(0);
    expect(coerceOrgValue("textArray", [])).toBeNull();
    expect(coerceOrgValue("json", [])).toBeNull();
  });

  it("reads a numeric out of the enrichment's text spelling", () => {
    expect(coerceOrgValue("numeric", "1500000.00")).toBe("1500000");
    expect(coerceOrgValue("int", "2018")).toBe(2018);
  });

  it("refuses a value that is not the shape the column stores", () => {
    expect(coerceOrgValue("textArray", "not-an-array")).toBeNull();
    expect(coerceOrgValue("int", "not-a-number")).toBeNull();
    expect(coerceOrgValue("text", "   ")).toBeNull();
    expect(coerceOrgValue("date", "circa 2011")).toBeNull();
  });
});

describe("columnsToFill", () => {
  const [record] = parseInput(
    csvLine({
      apolloPersonId: "p-1",
      shortDescription: "Sells, mostly.",
      keywords: ["a", "b"],
      foundedYear: 2018,
      latestFundingStage: "Series A",
    }),
  );

  it("fills only the columns the row currently holds NULL", () => {
    const filled = columnsToFill(
      { short_description: "already there", keywords: null, founded_year: null },
      record,
    );
    expect(filled.map((f) => f.column).sort()).toEqual([
      "founded_year",
      "keywords",
      "latest_funding_stage",
    ]);
  });

  it("writes nothing when the enrichment has nothing for a null column", () => {
    const [empty] = parseInput(csvLine({ apolloPersonId: "p-1" }));
    expect(columnsToFill({ short_description: null, keywords: null }, empty)).toEqual([]);
  });

  it("re-running fills nothing once every column carries a value", () => {
    const existing = Object.fromEntries(ORG_COLUMNS.map((c) => [c.column, "filled"]));
    expect(columnsToFill(existing, record)).toEqual([]);
  });
});

describe("normalizeEmployment / toIsoDate", () => {
  it("keeps a calendar date and refuses anything else", () => {
    expect(toIsoDate("2018-04-01T00:00:00.000Z")).toBe("2018-04-01");
    expect(toIsoDate("April 2018")).toBeNull();
    expect(toIsoDate(2018)).toBeNull();
  });

  it("treats a non-object entry as nothing", () => {
    expect(normalizeEmployment(null)).toBeNull();
    expect(normalizeEmployment("Old Co")).toBeNull();
  });
});
