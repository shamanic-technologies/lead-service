/**
 * ONE-TIME backfill of `leads.timezone` from the enrichment we already bought.
 *
 * Why: a lead handed to the send chain carries the recipient's IANA timezone so
 * the cold email is scheduled in the prospect's local business hours. A lead with
 * no timezone is refused at the send step — after we have already paid to find
 * the person, enrich them and generate their email. Apollo returns `time_zone` on
 * the ENRICHMENT response (never on search) and has since February 2026, but the
 * reader that persists it onto the lead only landed in August, and nothing
 * backfilled what came before. On 2026-08-18 production held 86,738 leads with a
 * null timezone, 28,212 of which we already hold the answer for.
 *
 * Zero Apollo spend, by construction: this script never talks to apollo-service.
 * Its input is a CSV produced by scripts/sql/apollo-lead-timezones.sql, a plain
 * SELECT against apollo-service's own `apollo_people_enrichments` table — no
 * Apollo API call, no key decryption, no credit authorization anywhere in the
 * path. Driving the repair through apollo-service `POST /enrich` instead would
 * have bought a credit on every cache miss, and that route's cache hit also
 * requires `email IS NOT NULL AND email_status = 'verified' AND created_at > 12
 * months ago`, so everyone those predicates exclude would have been re-purchased.
 *
 * A lead with no recoverable timezone keeps a null one. 55,349 of the affected
 * leads have neither a city nor a country, so no timezone exists for them at any
 * price; inventing one would schedule sends in a timezone the prospect is not in.
 * Null is the honest answer and the send chain accepts it. Nothing here derives a
 * timezone from a country, a city, or any other heuristic — the value comes out
 * of the stored enrichment verbatim or the lead stays null.
 *
 * Properties:
 *   - Dry-runnable   — `--dry-run` reports every bucket and writes nothing.
 *   - Idempotent     — the UPDATE carries `timezone IS NULL`, so a lead that has
 *                      one is never overwritten and a re-run converges to 0.
 *   - Traceable      — `--report <path>` writes the id of every lead it filled,
 *                      one per line.
 *   - Reversible     — UPDATE leads SET timezone = NULL WHERE id = ANY(<the ids
 *                      in the report file>);
 *   - Fail-loud      — a malformed input line, or a value that is not a plausible
 *                      IANA zone, aborts before any write rather than shrinking
 *                      the repaired set silently. Everything left unresolved is
 *                      counted and reported at the end.
 *
 * Usage:
 *   # 1. derive the mapping from apollo-service (see scripts/sql/…)
 *   psql -U postgres -d apollo_service -f scripts/sql/apollo-lead-timezones.sql
 *   # 2. preview
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/backfill-lead-timezone.ts \
 *     --input /tmp/apollo-lead-timezones.csv --dry-run
 *   # 3. apply
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/backfill-lead-timezone.ts \
 *     --input /tmp/apollo-lead-timezones.csv --report /tmp/filled-lead-ids.txt
 */

import { readFileSync, writeFileSync } from "node:fs";
import { sql as leadSql } from "../src/db/index.js";

/** How many (person id, timezone) pairs go into one UPDATE. */
export const DEFAULT_BATCH_SIZE = 1_000;

export interface Args {
  dryRun: boolean;
  inputPath: string | null;
  reportPath: string | null;
  batchSize: number;
}

export function parseArgs(argv: string[]): Args {
  const value = (flag: string): string | null => {
    const i = argv.indexOf(flag);
    return i >= 0 ? (argv[i + 1] ?? null) : null;
  };
  const rawBatch = value("--batch-size");
  const batchSize = rawBatch === null ? DEFAULT_BATCH_SIZE : Number(rawBatch);
  if (!Number.isInteger(batchSize) || batchSize <= 0) {
    throw new Error(`[lead-service] --batch-size must be a positive integer, got: ${rawBatch}`);
  }
  return {
    dryRun: argv.includes("--dry-run"),
    inputPath: value("--input"),
    reportPath: value("--report"),
    batchSize,
  };
}

export interface Mapping {
  apolloPersonId: string;
  timezone: string;
}

/**
 * A plausible IANA zone name: `Area/Location`, optionally with a further segment
 * (`America/Indiana/Indianapolis`), plus the two zone names that legitimately
 * carry no slash. This is a shape check, not a registry lookup — its only job is
 * to make sure a mangled CSV cell (a stray country name, a numeric offset, an
 * empty string) aborts the run instead of being written onto a lead as a
 * timezone the send chain will then reject all over again.
 */
const IANA_ZONE = /^(?:UTC|GMT|[A-Za-z][A-Za-z0-9+_-]*(?:\/[A-Za-z0-9+._-]+)+)$/;

export function isPlausibleIanaZone(value: string): boolean {
  return IANA_ZONE.test(value);
}

/**
 * `apollo_person_id,timezone` CSV as produced by
 * scripts/sql/apollo-lead-timezones.sql. A header row is tolerated; blank lines
 * are skipped; anything else that is not exactly two non-empty fields, or whose
 * timezone is not a plausible IANA zone, is an error — a malformed input must not
 * silently shrink the repaired set, and must never write a bad zone.
 *
 * The query emits one row per person already, so a duplicate person id means the
 * input was concatenated or edited; the first occurrence wins and the rest are
 * ignored rather than fighting over the same lead inside one UPDATE.
 */
export function parseInput(text: string): Mapping[] {
  const out: Mapping[] = [];
  const seen = new Set<string>();
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (line.length === 0) continue;
    const parts = line.split(",").map((p) => p.trim().replace(/^"|"$/g, ""));
    if (parts.length !== 2) {
      throw new Error(
        `[lead-service] malformed input line (expected apollo_person_id,timezone): ${line}`,
      );
    }
    const [apolloPersonId, timezone] = parts;
    if (apolloPersonId.toLowerCase() === "apollo_person_id") continue; // header
    if (apolloPersonId.length === 0) {
      throw new Error(`[lead-service] input line has no apollo person id: ${line}`);
    }
    if (!isPlausibleIanaZone(timezone)) {
      throw new Error(`[lead-service] input line has an implausible IANA timezone: ${line}`);
    }
    if (seen.has(apolloPersonId)) continue;
    seen.add(apolloPersonId);
    out.push({ apolloPersonId, timezone });
  }
  return out;
}

export interface BackfillPlan {
  /** leads carrying no timezone at all */
  nullTimezone: number;
  /** ...of which the input holds a timezone -> will be filled */
  recoverable: number;
  /** ...of which none is recoverable AND the person has no city and no country */
  noLocation: number;
  /** ...of which none is recoverable even though the person HAS a location */
  unresolvedWithLocation: number;
}

/**
 * Count every bucket without writing. The recoverable count is the same join the
 * UPDATE performs, so the dry-run and the apply can never disagree about scope.
 */
export async function buildPlan(mappings: Mapping[]): Promise<BackfillPlan> {
  const ids = mappings.map((m) => m.apolloPersonId);
  const rows = await leadSql<
    {
      null_timezone: string;
      recoverable: string;
      no_location: string;
      unresolved_with_location: string;
    }[]
  >`
    WITH known AS (SELECT unnest(${ids}::text[]) AS apollo_person_id),
    scoped AS (
      SELECT l.id,
             (l.apollo_person_id IS NOT NULL
               AND EXISTS (SELECT 1 FROM known k WHERE k.apollo_person_id = l.apollo_person_id)
             ) AS recoverable,
             (coalesce(l.city, '') = '' AND coalesce(l.country, '') = '') AS no_location
      FROM leads l
      WHERE l.timezone IS NULL
    )
    SELECT count(*)::text AS null_timezone,
           count(*) FILTER (WHERE recoverable)::text AS recoverable,
           count(*) FILTER (WHERE NOT recoverable AND no_location)::text AS no_location,
           count(*) FILTER (WHERE NOT recoverable AND NOT no_location)::text
             AS unresolved_with_location
    FROM scoped`;

  const r = rows[0];
  return {
    nullTimezone: Number(r?.null_timezone ?? "0"),
    recoverable: Number(r?.recoverable ?? "0"),
    noLocation: Number(r?.no_location ?? "0"),
    unresolvedWithLocation: Number(r?.unresolved_with_location ?? "0"),
  };
}

/**
 * Fill one batch. `timezone IS NULL` is what makes this idempotent and safe to
 * re-run: a lead that already carries a timezone — because the live path wrote
 * it, or because a previous run of this script did — is never touched, so the
 * newest live value always wins over the stored enrichment.
 */
export async function fillBatch(batch: Mapping[]): Promise<string[]> {
  if (batch.length === 0) return [];
  const ids = batch.map((m) => m.apolloPersonId);
  const zones = batch.map((m) => m.timezone);

  const updated = await leadSql<{ id: string }[]>`
    UPDATE leads l
    SET timezone = v.timezone
    FROM (
      SELECT unnest(${ids}::text[]) AS apollo_person_id,
             unnest(${zones}::text[]) AS timezone
    ) v
    WHERE l.apollo_person_id = v.apollo_person_id
      AND l.timezone IS NULL
    RETURNING l.id`;

  return updated.map((r) => r.id);
}

export function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

function logPlan(label: string, plan: BackfillPlan): void {
  console.log(
    `[lead-service] ${label}: nullTimezone=${plan.nullTimezone} recoverable=${plan.recoverable} ` +
      `noLocation=${plan.noLocation} unresolvedWithLocation=${plan.unresolvedWithLocation}`,
  );
}

async function main(): Promise<void> {
  const { dryRun, inputPath, reportPath, batchSize } = parseArgs(process.argv.slice(2));
  if (!inputPath) {
    throw new Error(
      "[lead-service] --input <csv> is required (produce it with scripts/sql/apollo-lead-timezones.sql)",
    );
  }

  const mappings = parseInput(readFileSync(inputPath, "utf8"));
  console.log(
    `[lead-service] timezone backfill start dryRun=${dryRun} mappings=${mappings.length} batchSize=${batchSize}`,
  );

  try {
    const plan = await buildPlan(mappings);
    logPlan("before", plan);

    if (dryRun) {
      console.log("[lead-service] DRY RUN — no writes performed");
      return;
    }

    const filled: string[] = [];
    const batches = chunk(mappings, batchSize);
    for (const [i, batch] of batches.entries()) {
      const ids = await fillBatch(batch);
      filled.push(...ids);
      console.log(`[lead-service] batch ${i + 1}/${batches.length} filled=${ids.length}`);
    }
    console.log(`[lead-service] backfill done — leadsFilled=${filled.length}`);

    if (reportPath) {
      writeFileSync(reportPath, filled.length ? `${filled.join("\n")}\n` : "", "utf8");
      console.log(`[lead-service] wrote ${filled.length} filled lead ids to ${reportPath}`);
    }

    const after = await buildPlan(mappings);
    logPlan("after", after);
    if (after.recoverable > 0) {
      console.warn(
        `[lead-service] WARNING: ${after.recoverable} leads still hold a null timezone we have ` +
          `an answer for — investigate before re-running`,
      );
    } else {
      console.log("[lead-service] verified: every recoverable lead now carries a timezone");
    }
    console.log(
      `[lead-service] left null on purpose: noLocation=${after.noLocation} ` +
        `unresolvedWithLocation=${after.unresolvedWithLocation} ` +
        `(no stored enrichment timezone — never guessed from a location)`,
    );
  } finally {
    await leadSql.end();
  }
}

// Only auto-run when invoked directly (not when imported by tests).
const invokedDirectly = process.argv[1]?.includes("backfill-lead-timezone");
if (invokedDirectly) {
  main().catch((err) => {
    console.error("[lead-service] timezone backfill failed:", err);
    process.exit(1);
  });
}
