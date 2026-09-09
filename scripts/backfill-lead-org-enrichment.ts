/**
 * ONE-TIME backfill of the lead's ORGANIZATION facts and CAREER HISTORY from the
 * enrichment we already bought.
 *
 * Why: the email a workflow sends is written by an LLM from the person and
 * company facts the served lead payload carries. apollo-service has held the rich
 * company record and the full employment list for every person it enriched, but
 * human-service's neutral person was slim on the organization and carried no
 * career history at all, so lead-service could never persist them. Measured in
 * production on 2026-09-09 over the last 30 days: apollo-service holds a company
 * short description for 94% of enriched people, keywords and technology names for
 * ~100%, a founded year for 79%, a funding stage for 21% and a full employment
 * list for 100%, while the lead rows for the same people carried 3.1% / 3.2% /
 * 3.2% / 2.8% / 0.6% and exactly one employment row each (avg 1.00). Downstream,
 * the company description, keywords and tech-stack prompt variables rendered
 * EMPTY in 87% of the 11,070 sales emails generated in that window.
 *
 * The live path is fixed separately (human-service carries the material, and
 * `recordEmploymentHistory` / `pickOrgFields` persist it at write time). This
 * script repairs what was already bought and dropped.
 *
 * Zero Apollo spend, by construction: this script never talks to apollo-service.
 * Its input is produced by scripts/sql/apollo-lead-org-enrichment.sql, a plain
 * SELECT against apollo-service's own `apollo_people_enrichments` — no Apollo API
 * call, no key decryption, no credit authorization anywhere in the path. Driving
 * the repair through apollo-service `POST /enrich` would have bought a credit on
 * every cache miss, and that route's cache hit additionally requires a verified
 * email newer than 12 months, so everyone those predicates exclude would have
 * been re-purchased.
 *
 * Properties:
 *   - Dry-runnable — `--dry-run` reports every bucket and writes nothing.
 *   - Idempotent   — an organization column is filled ONLY when it is currently
 *                    NULL, so a re-run converges to 0 and a value the live path
 *                    (or a later, better enrichment) wrote is never overwritten.
 *                    An employment row is matched on (organization, start date)
 *                    before being inserted, and a pre-existing row carrying no
 *                    start date is ADOPTED rather than duplicated — the same rule
 *                    the live path uses, so the two agree.
 *   - Traceable /
 *     reversible   — `--report <path>` writes one JSON line per write: the
 *                    organization id plus the exact columns filled, and the id of
 *                    every employment row inserted. Reversal is therefore exact:
 *                    NULL those columns on those organizations, DELETE those
 *                    leads_organizations ids.
 *   - Fail-loud    — a malformed input line aborts before any write rather than
 *                    silently shrinking the repaired set. Nothing is derived,
 *                    inferred or defaulted: a fact absent from the enrichment
 *                    leaves the column NULL.
 *
 * Usage:
 *   # 1. derive the input from apollo-service (see scripts/sql/…)
 *   psql -U postgres -d apollo_service -f scripts/sql/apollo-lead-org-enrichment.sql
 *   # 2. preview
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/backfill-lead-org-enrichment.ts \
 *     --input /tmp/apollo-lead-org-enrichment.jsonl --dry-run
 *   # 3. apply
 *   LEAD_SERVICE_DATABASE_URL=... npx tsx scripts/backfill-lead-org-enrichment.ts \
 *     --input /tmp/apollo-lead-org-enrichment.jsonl --report /tmp/org-backfill.jsonl
 */

import { appendFileSync, writeFileSync } from "node:fs";

/** How many leads are read out of lead-service in one page. */
export const DEFAULT_BATCH_SIZE = 2_000;

export interface Args {
  dryRun: boolean;
  inputPath: string | null;
  reportPath: string | null;
  batchSize: number;
  limit: number | null;
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
  const rawLimit = value("--limit");
  const limit = rawLimit === null ? null : Number(rawLimit);
  if (limit !== null && (!Number.isInteger(limit) || limit <= 0)) {
    throw new Error(`[lead-service] --limit must be a positive integer, got: ${rawLimit}`);
  }
  return {
    dryRun: argv.includes("--dry-run"),
    inputPath: value("--input"),
    reportPath: value("--report"),
    batchSize,
    limit,
  };
}

export interface EmploymentEntry {
  organizationName: string | null;
  title: string | null;
  startDate: string | null;
  endDate: string | null;
  current: boolean;
  description: string | null;
}

export interface EnrichmentRecord {
  apolloPersonId: string | null;
  email: string | null;
  org: Record<string, unknown>;
  employmentHistory: EmploymentEntry[];
}

/**
 * Organization columns this repair can fill, keyed by the JSON field the SQL
 * emits. The value is the Postgres column name; the kind drives the coercion —
 * the enrichment store types some of these as text and some as json, and a
 * column is only ever written with the shape it declares.
 */
export const ORG_COLUMNS: Array<{
  key: string;
  column: string;
  kind: "text" | "int" | "numeric" | "textArray" | "json" | "date";
}> = [
  { key: "providerOrganizationId", column: "apollo_organization_id", kind: "text" },
  { key: "name", column: "name", kind: "text" },
  { key: "domain", column: "primary_domain", kind: "text" },
  { key: "websiteUrl", column: "website_url", kind: "text" },
  { key: "industry", column: "industry", kind: "text" },
  { key: "logoUrl", column: "logo_url", kind: "text" },
  { key: "linkedinUrl", column: "linkedin_url", kind: "text" },
  { key: "twitterUrl", column: "twitter_url", kind: "text" },
  { key: "facebookUrl", column: "facebook_url", kind: "text" },
  { key: "blogUrl", column: "blog_url", kind: "text" },
  { key: "crunchbaseUrl", column: "crunchbase_url", kind: "text" },
  { key: "angellistUrl", column: "angellist_url", kind: "text" },
  { key: "shortDescription", column: "short_description", kind: "text" },
  { key: "seoDescription", column: "seo_description", kind: "text" },
  { key: "keywords", column: "keywords", kind: "textArray" },
  { key: "technologyNames", column: "technology_names", kind: "textArray" },
  { key: "industries", column: "industries", kind: "textArray" },
  { key: "secondaryIndustries", column: "secondary_industries", kind: "textArray" },
  { key: "latestFundingStage", column: "latest_funding_stage", kind: "text" },
  { key: "latestFundingRoundDate", column: "latest_funding_round_date", kind: "date" },
  { key: "totalFunding", column: "total_funding", kind: "numeric" },
  { key: "totalFundingPrinted", column: "total_funding_printed", kind: "text" },
  { key: "fundingEvents", column: "funding_events", kind: "json" },
  { key: "foundedYear", column: "founded_year", kind: "int" },
  { key: "annualRevenue", column: "annual_revenue", kind: "numeric" },
  { key: "estimatedNumEmployees", column: "estimated_num_employees", kind: "int" },
  { key: "city", column: "city", kind: "text" },
  { key: "state", column: "state", kind: "text" },
  { key: "country", column: "country", kind: "text" },
  { key: "streetAddress", column: "street_address", kind: "text" },
  { key: "postalCode", column: "postal_code", kind: "text" },
  { key: "primaryPhone", column: "primary_phone", kind: "text" },
  { key: "publiclyTradedSymbol", column: "publicly_traded_symbol", kind: "text" },
  { key: "publiclyTradedExchange", column: "publicly_traded_exchange", kind: "text" },
  { key: "numSuborganizations", column: "num_suborganizations", kind: "int" },
  { key: "retailLocationCount", column: "retail_location_count", kind: "int" },
  { key: "alexaRanking", column: "alexa_ranking", kind: "int" },
];

const ISO_DATE = /^\d{4}-\d{2}-\d{2}/;

/** `YYYY-MM-DD`, or null. Never a guess — an unreadable date is simply not one. */
export function toIsoDate(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!ISO_DATE.test(trimmed)) return null;
  return trimmed.slice(0, 10);
}

/**
 * Coerce one enrichment value to what its column stores. Anything that is not a
 * usable value of that shape yields null, which means "leave the column alone" —
 * never a substituted default.
 */
export function coerceOrgValue(
  kind: (typeof ORG_COLUMNS)[number]["kind"],
  raw: unknown,
): string | number | string[] | null {
  if (raw === null || raw === undefined) return null;
  switch (kind) {
    case "text": {
      const s = typeof raw === "string" ? raw.trim() : String(raw);
      return s.length > 0 ? s : null;
    }
    case "date":
      return toIsoDate(raw);
    case "int": {
      const n = typeof raw === "number" ? raw : Number(String(raw).replace(/[^0-9.-]/g, ""));
      return Number.isFinite(n) ? Math.trunc(n) : null;
    }
    case "numeric": {
      const n = typeof raw === "number" ? raw : Number(String(raw).replace(/[^0-9.-]/g, ""));
      return Number.isFinite(n) ? String(n) : null;
    }
    case "textArray": {
      if (!Array.isArray(raw)) return null;
      const list = raw.filter((v): v is string => typeof v === "string" && v.trim().length > 0);
      return list.length > 0 ? list : null;
    }
    case "json":
      return Array.isArray(raw) && raw.length > 0 ? JSON.stringify(raw) : null;
  }
}

/** One employment entry as the enrichment stores it, normalized. */
export function normalizeEmployment(raw: unknown): EmploymentEntry | null {
  if (!raw || typeof raw !== "object") return null;
  const e = raw as Record<string, unknown>;
  const name =
    (typeof e.organizationName === "string" && e.organizationName.trim()) ||
    (typeof e.organization_name === "string" && e.organization_name.trim()) ||
    null;
  const title =
    (typeof e.title === "string" && e.title.trim()) || null;
  const description =
    (typeof e.description === "string" && e.description.trim()) || null;
  return {
    organizationName: name || null,
    title: title || null,
    startDate: toIsoDate(e.startDate ?? e.start_date),
    endDate: toIsoDate(e.endDate ?? e.end_date),
    current: (e.current ?? e.is_current) === true,
    description: description || null,
  };
}

/**
 * One record from one line, as `\copy ... CSV` writes a single JSON column: the
 * field is quoted and any embedded quote is doubled. A blank line yields null; a
 * line that is not a JSON object, or that carries neither key a lead can be
 * matched on, is an ERROR — a mangled input must abort, never silently shrink
 * the repaired set.
 *
 * The record keeps ONLY the columns this repair can write plus the normalized
 * career history. The input is ~380 MB across ~57k people, so carrying the whole
 * parsed object per person is the difference between a run and an out-of-memory
 * abort.
 */
export function parseLine(line: string, lineNo: number): EnrichmentRecord | null {
  const trimmed = line.trim();
  if (trimmed.length === 0) return null;
  const json = trimmed.startsWith('"')
    ? trimmed.slice(1, trimmed.endsWith('"') ? -1 : undefined).replace(/""/g, '"')
    : trimmed;
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch (err) {
    throw new Error(`[lead-service] input line ${lineNo} is not JSON: ${String(err)}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`[lead-service] input line ${lineNo} is not a JSON object`);
  }
  const rec = parsed as Record<string, unknown>;
  const apolloPersonId =
    typeof rec.apolloPersonId === "string" && rec.apolloPersonId.trim().length > 0
      ? rec.apolloPersonId.trim()
      : null;
  const email =
    typeof rec.email === "string" && rec.email.trim().length > 0
      ? rec.email.trim().toLowerCase()
      : null;
  if (!apolloPersonId && !email) {
    throw new Error(`[lead-service] input line ${lineNo} has neither apolloPersonId nor email`);
  }
  const org: Record<string, unknown> = {};
  for (const spec of ORG_COLUMNS) {
    const v = rec[spec.key];
    if (v !== null && v !== undefined) org[spec.key] = v;
  }
  const employmentHistory = Array.isArray(rec.employmentHistory)
    ? rec.employmentHistory
        .map(normalizeEmployment)
        .filter((e): e is EmploymentEntry => e !== null && e.organizationName !== null)
    : [];
  return { apolloPersonId, email, org, employmentHistory };
}

/** Whole-text convenience over `parseLine` — used by the tests and small inputs. */
export function parseInput(text: string): EnrichmentRecord[] {
  const out: EnrichmentRecord[] = [];
  let lineNo = 0;
  for (const raw of text.split("\n")) {
    lineNo += 1;
    const rec = parseLine(raw, lineNo);
    if (rec) out.push(rec);
  }
  return out;
}

/** Index the records by both keys a lead can be matched on. Person id wins. */
export function indexRecords(records: EnrichmentRecord[]): {
  byPersonId: Map<string, EnrichmentRecord>;
  byEmail: Map<string, EnrichmentRecord>;
} {
  const byPersonId = new Map<string, EnrichmentRecord>();
  const byEmail = new Map<string, EnrichmentRecord>();
  for (const r of records) {
    if (r.apolloPersonId && !byPersonId.has(r.apolloPersonId)) byPersonId.set(r.apolloPersonId, r);
    if (r.email && !byEmail.has(r.email)) byEmail.set(r.email, r);
  }
  return { byPersonId, byEmail };
}

/**
 * The columns to fill on an organization row: every column the enrichment has a
 * value for AND the row currently holds NULL. Filling only NULLs is what makes
 * the repair idempotent and keeps it from overwriting a live write.
 */
export function columnsToFill(
  existing: Record<string, unknown>,
  record: EnrichmentRecord,
): Array<{ column: string; value: string | number | string[]; kind: string }> {
  const out: Array<{ column: string; value: string | number | string[]; kind: string }> = [];
  for (const spec of ORG_COLUMNS) {
    if (existing[spec.column] !== null && existing[spec.column] !== undefined) continue;
    const value = coerceOrgValue(spec.kind, record.org[spec.key]);
    if (value === null) continue;
    out.push({ column: spec.column, value, kind: spec.kind });
  }
  return out;
}

// ─── The run (DB I/O) ────────────────────────────────────────────────────────

interface Report {
  write(line: string): void;
}

function openReport(path: string | null): Report {
  if (!path) return { write: () => {} };
  writeFileSync(path, "");
  return { write: (line: string) => appendFileSync(path, `${line}\n`) };
}

async function run(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (!args.inputPath) {
    throw new Error("[lead-service] --input <path> is required");
  }
  // Streamed, not slurped: the production input is ~380 MB and reading it as one
  // string then splitting it needs several times that in heap.
  const byPersonId = new Map<string, EnrichmentRecord>();
  const byEmail = new Map<string, EnrichmentRecord>();
  let loaded = 0;
  {
    const { createReadStream } = await import("node:fs");
    const { createInterface } = await import("node:readline");
    const rl = createInterface({
      input: createReadStream(args.inputPath, "utf8"),
      crlfDelay: Infinity,
    });
    let lineNo = 0;
    for await (const line of rl) {
      lineNo += 1;
      const rec = parseLine(line, lineNo);
      if (!rec) continue;
      loaded += 1;
      if (rec.apolloPersonId && !byPersonId.has(rec.apolloPersonId))
        byPersonId.set(rec.apolloPersonId, rec);
      if (rec.email && !byEmail.has(rec.email)) byEmail.set(rec.email, rec);
    }
  }
  console.log(
    `[lead-service] loaded ${loaded} enrichment records (${byPersonId.size} by person id, ${byEmail.size} by email)`,
  );

  const { sql } = await import("../src/db/index.js");
  const report = openReport(args.reportPath);

  const stats = {
    leadsScanned: 0,
    leadsMatched: 0,
    orgsFilled: 0,
    columnsFilled: 0,
    orgsCreated: 0,
    employmentInserted: 0,
    employmentAdopted: 0,
    leadsWithoutOrg: 0,
  };

  let offset = 0;
  for (;;) {
    const page = (await sql`
      SELECT l.id,
             l.apollo_person_id,
             lower(lcm.value) AS email
      FROM leads l
      LEFT JOIN lead_contact_methods lcm
        ON lcm.lead_id = l.id AND lcm.channel = 'email'
      ORDER BY l.created_at DESC, l.id
      LIMIT ${args.batchSize} OFFSET ${offset}
    `) as Array<{ id: string; apollo_person_id: string | null; email: string | null }>;
    if (page.length === 0) break;
    offset += page.length;

    for (const lead of page) {
      stats.leadsScanned += 1;
      if (args.limit !== null && stats.leadsMatched >= args.limit) break;
      const record =
        (lead.apollo_person_id ? byPersonId.get(lead.apollo_person_id) : undefined) ??
        (lead.email ? byEmail.get(lead.email) : undefined);
      if (!record) continue;
      stats.leadsMatched += 1;

      // --- The current employer's organization row -------------------------
      const currentRows = (await sql`
        SELECT lo.id AS link_id, o.*
        FROM leads_organizations lo
        JOIN organizations o ON o.id = lo.organization_id
        WHERE lo.lead_id = ${lead.id} AND lo.current = true
        LIMIT 1
      `) as Array<Record<string, unknown>>;

      let organizationId: string | null = (currentRows[0]?.id as string | undefined) ?? null;
      let existing: Record<string, unknown> = currentRows[0] ?? {};

      if (!organizationId) {
        stats.leadsWithoutOrg += 1;
        const domain = coerceOrgValue("text", record.org.domain);
        const name = coerceOrgValue("text", record.org.name);
        if (!domain && !name) continue;
        if (args.dryRun) continue;
        const found = domain
          ? ((await sql`SELECT * FROM organizations WHERE primary_domain = ${domain as string} LIMIT 1`) as Array<
              Record<string, unknown>
            >)
          : ((await sql`SELECT * FROM organizations WHERE name = ${name as string} LIMIT 1`) as Array<
              Record<string, unknown>
            >);
        if (found[0]) {
          organizationId = found[0].id as string;
          existing = found[0];
        } else {
          const created = (await sql`
            INSERT INTO organizations (name, primary_domain) VALUES (${name}, ${domain}) RETURNING *
          `) as Array<Record<string, unknown>>;
          organizationId = created[0].id as string;
          existing = created[0];
          stats.orgsCreated += 1;
          report.write(JSON.stringify({ createdOrganizationId: organizationId }));
        }
        await sql`
          INSERT INTO leads_organizations (lead_id, organization_id, current)
          VALUES (${lead.id}, ${organizationId}, true)
          ON CONFLICT DO NOTHING
        `;
      }

      const fills = columnsToFill(existing, record);
      if (fills.length > 0) {
        stats.orgsFilled += 1;
        stats.columnsFilled += fills.length;
        if (!args.dryRun) {
          for (const fill of fills) {
            const value = fill.kind === "json" ? sql`${fill.value}::jsonb` : sql`${fill.value}`;
            await sql`
              UPDATE organizations
              SET ${sql(fill.column)} = ${value}, updated_at = now()
              WHERE id = ${organizationId} AND ${sql(fill.column)} IS NULL
            `;
          }
          report.write(
            JSON.stringify({
              organizationId,
              columns: fills.map((f) => f.column),
            }),
          );
        }
      }

      // --- The career history ----------------------------------------------
      if (record.employmentHistory.length === 0) continue;
      const links = (await sql`
        SELECT lo.id, lo.organization_id, lo.start_date, o.name AS organization_name
        FROM leads_organizations lo
        JOIN organizations o ON o.id = lo.organization_id
        WHERE lo.lead_id = ${lead.id}
      `) as Array<{
        id: string;
        organization_id: string;
        start_date: string | null;
        organization_name: string | null;
      }>;
      const consumed = new Set<string>();

      for (const entry of record.employmentHistory) {
        const name = entry.organizationName as string;
        const byName = links.filter(
          (l) =>
            !consumed.has(l.id) &&
            (l.organization_name ?? "").trim().toLowerCase() === name.trim().toLowerCase(),
        );
        const match =
          byName.find((l) => (l.start_date ?? null) === entry.startDate) ??
          byName.find((l) => l.start_date === null);
        if (match) {
          consumed.add(match.id);
          stats.employmentAdopted += 1;
          continue;
        }
        if (args.dryRun) {
          stats.employmentInserted += 1;
          continue;
        }
        const orgRows = (await sql`SELECT id FROM organizations WHERE name = ${name} LIMIT 1`) as Array<{
          id: string;
        }>;
        let entryOrgId = orgRows[0]?.id ?? null;
        if (!entryOrgId) {
          const created = (await sql`
            INSERT INTO organizations (name) VALUES (${name}) RETURNING id
          `) as Array<{ id: string }>;
          entryOrgId = created[0].id;
          stats.orgsCreated += 1;
          report.write(JSON.stringify({ createdOrganizationId: entryOrgId }));
        }
        const inserted = (await sql`
          INSERT INTO leads_organizations
            (lead_id, organization_id, title, start_date, end_date, current, description)
          VALUES (${lead.id}, ${entryOrgId}, ${entry.title}, ${entry.startDate},
                  ${entry.endDate}, false, ${entry.description})
          ON CONFLICT DO NOTHING
          RETURNING id
        `) as Array<{ id: string }>;
        if (inserted[0]) {
          stats.employmentInserted += 1;
          report.write(JSON.stringify({ employmentRowId: inserted[0].id }));
        }
      }
    }

    console.log(
      `[lead-service] scanned=${stats.leadsScanned} matched=${stats.leadsMatched} orgsFilled=${stats.orgsFilled} columns=${stats.columnsFilled} employmentInserted=${stats.employmentInserted}`,
    );
    if (args.limit !== null && stats.leadsMatched >= args.limit) break;
  }

  console.log(`[lead-service] ${args.dryRun ? "DRY RUN — nothing written" : "done"}`);
  console.log(JSON.stringify(stats, null, 2));
  await sql.end({ timeout: 5 });
}

const invokedDirectly = process.argv[1]?.includes("backfill-lead-org-enrichment");
if (invokedDirectly) {
  run().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
