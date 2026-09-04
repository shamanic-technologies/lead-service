/**
 * Which rows a filtered / searched / re-ordered read returns, and in what order.
 *
 * A page whose ORDER depends on evidence held by another service (the reply that dates a lead) or
 * whose FILTER does (the bucket it is in) cannot be expressed as a SQL keyset over
 * `leads_campaigns` alone. So the population is indexed first (see lead-index.ts / lead-engagement.ts),
 * the order and the filter are applied to that index, and only the page's own ids are hydrated.
 * The expensive half — the full lead graph, the audience, the offer, the standing, the JSON — is
 * paid for the rows that come back and for nothing else.
 *
 * The order is TOTAL in both modes: `(created_at, id)` ascending, or `(activityAt, id)` descending,
 * and `id` is unique. That is what makes two consecutive page reads neither repeat a lead nor skip
 * one — the same guarantee the plain keyset walk gives, over a different key.
 */
import { encodeLeadCursor, type LeadListCursor, type LeadListPage } from "./lead-list-query.js";
import type { LeadBucket } from "./lead-buckets.js";
import type { EnrichedLeadIndexRow } from "./lead-engagement.js";

export const LEAD_SORT_ORDERS = ["created", "activity"] as const;
export type LeadSortOrder = (typeof LEAD_SORT_ORDERS)[number];

/**
 * Resolve the `sort` query param. Absent → `created`: `(created_at, id)` ascending, the order this
 * endpoint has always answered in and the one every existing caller walks. `activity` is
 * newest-first on the timestamp that proves each lead's most advanced status. Anything else is a
 * 400 (throws) — an ignored sort is a list that silently disagrees with the order it was asked for.
 */
export function parseLeadSort(raw: unknown): LeadSortOrder {
  if (raw === undefined) return "created";
  if (typeof raw !== "string") throw new Error("sort must be a single sort name");
  const trimmed = raw.trim();
  if ((LEAD_SORT_ORDERS as readonly string[]).includes(trimmed)) return trimmed as LeadSortOrder;
  throw new Error(`Unknown sort '${raw}'. Valid: ${LEAD_SORT_ORDERS.join(", ")}`);
}

export interface LeadPagePlan {
  /** How many rows match the filter in total — the number the page is a window onto. */
  total: number;
  /** The `leads_campaigns` ids to hydrate, in the order they must be emitted. */
  ids: string[];
  /** Where to resume, or null when this page reached the end (or the read is unbounded). */
  nextCursor: string | null;
}

/** Milliseconds of an activity timestamp. Every enriched row has one, so this never sees null. */
function activityMs(value: string): number {
  const ms = new Date(value).getTime();
  if (Number.isNaN(ms)) throw new Error(`[lead-service] invalid activity timestamp: ${value}`);
  return ms;
}

/**
 * Compare two rows in the read's order. Returns <0 when `a` comes first.
 *
 * `created` compares the `created_at::text` Postgres gave us, byte for byte, so it agrees with the
 * SQL `ORDER BY` to the microsecond. `activity` compares instants, because those timestamps come
 * from another service and are only guaranteed to be timestamps, not to share one spelling.
 */
function compareRows(a: EnrichedLeadIndexRow, b: EnrichedLeadIndexRow, sort: LeadSortOrder): number {
  if (sort === "created") {
    if (a.createdAtText !== b.createdAtText) return a.createdAtText < b.createdAtText ? -1 : 1;
    return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
  }
  const left = activityMs(a.activityAt);
  const right = activityMs(b.activityAt);
  if (left !== right) return left > right ? -1 : 1;
  return a.id > b.id ? -1 : a.id < b.id ? 1 : 0;
}

/** Is `row` strictly after `cursor` in the read's order — i.e. does the resumed page include it? */
function isAfterCursor(
  row: EnrichedLeadIndexRow,
  cursor: LeadListCursor,
  sort: LeadSortOrder,
): boolean {
  if (sort === "created") {
    if (row.createdAtText !== cursor.createdAt) return row.createdAtText > cursor.createdAt;
    return row.id > cursor.id;
  }
  const rowMs = activityMs(row.activityAt);
  const cursorMs = activityMs(cursor.createdAt);
  if (rowMs !== cursorMs) return rowMs < cursorMs;
  return row.id < cursor.id;
}

/** The position value a cursor carries for this row, in this order. */
function positionOf(row: EnrichedLeadIndexRow, sort: LeadSortOrder): string {
  return sort === "created" ? row.createdAtText : row.activityAt;
}

/**
 * Filter, order and window the index into the page a caller asked for.
 *
 * `total` is counted AFTER the bucket filter and BEFORE the window, so it is what the caller is
 * paging through — the number that labels "1-50 of N", not the size of the brand.
 */
export function planLeadPage(
  rows: readonly EnrichedLeadIndexRow[],
  bucket: LeadBucket | null,
  sort: LeadSortOrder,
  page: LeadListPage,
): LeadPagePlan {
  const matching = bucket ? rows.filter((row) => row.buckets.has(bucket)) : [...rows];
  const total = matching.length;

  const ordered = sort === "created" ? matching : matching.sort((a, b) => compareRows(a, b, sort));

  let windowed = page.cursor
    ? ordered.filter((row) => isAfterCursor(row, page.cursor!, sort))
    : ordered;
  if (page.offset !== null && page.offset > 0) windowed = windowed.slice(page.offset);

  if (page.limit === null) {
    return { total, ids: windowed.map((row) => row.id), nextCursor: null };
  }

  const pageRows = windowed.slice(0, page.limit);
  const last = pageRows[pageRows.length - 1];
  const nextCursor =
    last && windowed.length > pageRows.length
      ? encodeLeadCursor({ createdAt: positionOf(last, sort), id: last.id })
      : null;

  return { total, ids: pageRows.map((row) => row.id), nextCursor };
}
