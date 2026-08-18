import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * The backfill is SQL-shaped, so what is worth pinning here is the contract the
 * operator relies on: the flags, the input it refuses, and that each statement
 * carries the predicates that make it safe — match on the apollo person id, fill
 * only a null timezone, and never invent a zone.
 */

const queries: string[] = [];
const params: unknown[][] = [];
let results: unknown[][] = [];

// postgres.js tagged template: capture the composed SQL text and the bound
// params, and hand back whatever the case queued as the next result set.
function fakeSql(strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown[]> {
  queries.push(strings.join("?"));
  params.push(values);
  return Promise.resolve(results.shift() ?? []);
}

vi.mock("../../src/db/index.js", () => ({
  sql: (...args: unknown[]) =>
    fakeSql(args[0] as TemplateStringsArray, ...(args.slice(1) as unknown[])),
}));

beforeEach(() => {
  queries.length = 0;
  params.length = 0;
  results = [];
});

describe("backfill-lead-timezone", () => {
  it("parses the flags and defaults the batch size", async () => {
    const { parseArgs, DEFAULT_BATCH_SIZE } = await import(
      "../../scripts/backfill-lead-timezone.js"
    );
    expect(parseArgs(["--input", "/tmp/x.csv", "--dry-run"])).toEqual({
      dryRun: true,
      inputPath: "/tmp/x.csv",
      reportPath: null,
      batchSize: DEFAULT_BATCH_SIZE,
    });
    expect(parseArgs(["--input", "/tmp/x.csv", "--report", "/tmp/r.txt", "--batch-size", "5"]))
      .toEqual({
        dryRun: false,
        inputPath: "/tmp/x.csv",
        reportPath: "/tmp/r.txt",
        batchSize: 5,
      });
  });

  it("refuses a batch size that is not a positive integer", async () => {
    const { parseArgs } = await import("../../scripts/backfill-lead-timezone.js");
    expect(() => parseArgs(["--batch-size", "0"])).toThrow(/positive integer/);
    expect(() => parseArgs(["--batch-size", "abc"])).toThrow(/positive integer/);
  });

  it("parses the mapping CSV, tolerating a header and blank lines", async () => {
    const { parseInput } = await import("../../scripts/backfill-lead-timezone.js");
    const rows = parseInput(
      [
        "apollo_person_id,timezone",
        "5d676aa7f651258b56710b64,America/Chicago",
        "",
        '"abc123","Europe/Paris"',
        // a re-run of the query concatenated onto the same file: first wins
        "5d676aa7f651258b56710b64,America/New_York",
      ].join("\n"),
    );
    expect(rows).toEqual([
      { apolloPersonId: "5d676aa7f651258b56710b64", timezone: "America/Chicago" },
      { apolloPersonId: "abc123", timezone: "Europe/Paris" },
    ]);
  });

  it("aborts on a malformed line rather than shrinking the repaired set", async () => {
    const { parseInput } = await import("../../scripts/backfill-lead-timezone.js");
    expect(() => parseInput("abc123")).toThrow(/malformed input line/);
    expect(() => parseInput(",America/Chicago")).toThrow(/no apollo person id/);
  });

  it("refuses anything that is not a plausible IANA zone", async () => {
    const { parseInput, isPlausibleIanaZone } = await import(
      "../../scripts/backfill-lead-timezone.js"
    );
    expect(isPlausibleIanaZone("America/Indiana/Indianapolis")).toBe(true);
    expect(isPlausibleIanaZone("UTC")).toBe(true);
    // a country, an offset or an empty cell would be written onto the lead as a
    // timezone the send chain rejects all over again
    expect(isPlausibleIanaZone("United States")).toBe(false);
    expect(isPlausibleIanaZone("-05:00")).toBe(false);
    expect(isPlausibleIanaZone("")).toBe(false);
    expect(() => parseInput("abc123,United States")).toThrow(/implausible IANA timezone/);
  });

  it("counts the buckets off the same person-id key the fill uses", async () => {
    const { buildPlan } = await import("../../scripts/backfill-lead-timezone.js");
    results = [
      [
        {
          null_timezone: "86738",
          recoverable: "28212",
          no_location: "55349",
          unresolved_with_location: "3177",
        },
      ],
    ];

    const plan = await buildPlan([{ apolloPersonId: "abc123", timezone: "America/Chicago" }]);

    expect(plan).toEqual({
      nullTimezone: 86738,
      recoverable: 28212,
      noLocation: 55349,
      unresolvedWithLocation: 3177,
    });

    const all = queries.join("\n");
    expect(all).toContain("l.timezone IS NULL");
    // hashed array probe, not a join against unnest() — the join form plans as a
    // nested loop over a function scan and does not finish at production size
    expect(all).toContain("l.apollo_person_id = ANY(");
    // a lead with no apollo person id must stay in the complement, not vanish
    expect(all).toContain("coalesce(l.apollo_person_id = ANY(");
    // "no location" is what stays null on purpose — it is counted, not guessed
    expect(all).toContain("coalesce(l.city, '') = '' AND coalesce(l.country, '') = ''");
    expect(params[0]?.[0]).toEqual(["abc123"]);
  });

  it("fills only leads whose timezone is still null, matched on the apollo person id", async () => {
    const { fillBatch } = await import("../../scripts/backfill-lead-timezone.js");
    results = [[{ id: "lead-1" }, { id: "lead-2" }]];

    const filled = await fillBatch([
      { apolloPersonId: "abc123", timezone: "America/Chicago" },
      { apolloPersonId: "def456", timezone: "Europe/Paris" },
    ]);

    expect(filled).toEqual(["lead-1", "lead-2"]);
    const all = queries.join("\n");
    expect(all).toContain("UPDATE leads l");
    expect(all).toContain("SET timezone = v.timezone");
    expect(all).toContain("l.apollo_person_id = v.apollo_person_id");
    // idempotence + "the live path always wins" live in this one predicate
    expect(all).toContain("AND l.timezone IS NULL");
    // person ids and zones are bound positionally, so they must stay aligned
    expect(params[0]?.[0]).toEqual(["abc123", "def456"]);
    expect(params[0]?.[1]).toEqual(["America/Chicago", "Europe/Paris"]);
  });

  it("writes nothing when there is nothing to fill", async () => {
    const { fillBatch } = await import("../../scripts/backfill-lead-timezone.js");
    expect(await fillBatch([])).toEqual([]);
    expect(queries).toEqual([]);
  });

  it("splits the mapping into batches", async () => {
    const { chunk } = await import("../../scripts/backfill-lead-timezone.js");
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
    expect(chunk([], 2)).toEqual([]);
  });
});
