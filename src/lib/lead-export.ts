/**
 * The spreadsheet a customer downloads from the Leads page.
 *
 * A file somebody opens is something they READ, and its column headings are the only labels on
 * it. So this file is headed in the words the page uses for those facts — "Contacted", "Website
 * visit", "Reply sentiment" — never in the field names the JSON carries, and its cells hold
 * values a spreadsheet can display: Yes/No rather than a raw boolean token, a readable date
 * rather than an ISO instant. The JSON list is untouched: its field names are a contract that
 * features-service and the staff console read, and this is about the file only.
 *
 * Every column is taken off the SAME serialized item the JSON list emits — there is no second
 * projection of a lead anywhere, and there must never be one.
 */

/** How a cell is rendered, decided per column rather than sniffed off the value. */
type CellKind = "text" | "yesno" | "datetime";

interface LeadExportColumn {
  /** The heading a customer reads, in the page's own words. */
  header: string;
  kind: CellKind;
  /** The fact, read off the serialized list item. */
  read: (item: LeadExportItem) => unknown;
}

/** The serialized list item, as far as an export is concerned. */
type LeadExportItem = Record<string, unknown>;

function nested(item: LeadExportItem, key: string): Record<string, unknown> | null {
  const value = item[key];
  return value && typeof value === "object" ? (value as Record<string, unknown>) : null;
}

function lead(item: LeadExportItem): Record<string, unknown> | null {
  return nested(item, "lead");
}

function organization(item: LeadExportItem): Record<string, unknown> | null {
  const l = lead(item);
  const org = l?.organization;
  return org && typeof org === "object" ? (org as Record<string, unknown>) : null;
}

/**
 * The columns an export carries, in the order a person reads them: who they are, where they
 * work, which audience they came from, where they stand, what happened, and when.
 *
 * Deliberately NOT the whole record — an export nobody can open in a spreadsheet is not an
 * export — and deliberately without the internal row/lead/campaign identifiers the JSON carries:
 * those are join keys for a consumer, not facts a customer reads.
 */
export const LEAD_EXPORT_COLUMNS: readonly LeadExportColumn[] = [
  { header: "First name", kind: "text", read: (i) => lead(i)?.firstName },
  { header: "Last name", kind: "text", read: (i) => lead(i)?.lastName },
  { header: "Email", kind: "text", read: (i) => i.email },
  { header: "Email status", kind: "text", read: (i) => i.emailStatus },
  { header: "Title", kind: "text", read: (i) => lead(i)?.currentTitle },
  { header: "Headline", kind: "text", read: (i) => lead(i)?.headline },
  { header: "Seniority", kind: "text", read: (i) => lead(i)?.seniority },
  { header: "Departments", kind: "text", read: (i) => lead(i)?.departments },
  { header: "Functions", kind: "text", read: (i) => lead(i)?.functions },
  { header: "LinkedIn", kind: "text", read: (i) => lead(i)?.linkedinUrl },
  { header: "City", kind: "text", read: (i) => lead(i)?.city },
  { header: "State", kind: "text", read: (i) => lead(i)?.state },
  { header: "Country", kind: "text", read: (i) => lead(i)?.country },
  { header: "Company", kind: "text", read: (i) => organization(i)?.name },
  { header: "Company domain", kind: "text", read: (i) => organization(i)?.primaryDomain },
  { header: "Company industry", kind: "text", read: (i) => organization(i)?.industry },
  { header: "Company revenue", kind: "text", read: (i) => organization(i)?.annualRevenue },
  { header: "Company employees", kind: "text", read: (i) => organization(i)?.estimatedNumEmployees },
  { header: "Company founded", kind: "text", read: (i) => organization(i)?.foundedYear },
  { header: "Company city", kind: "text", read: (i) => organization(i)?.city },
  { header: "Company state", kind: "text", read: (i) => organization(i)?.state },
  { header: "Company country", kind: "text", read: (i) => organization(i)?.country },
  { header: "Audience", kind: "text", read: (i) => nested(i, "audience")?.name },
  { header: "Offer", kind: "text", read: (i) => nested(i, "offer")?.name },
  { header: "Status", kind: "text", read: (i) => i.status },
  { header: "Standing", kind: "text", read: (i) => nested(i, "standing")?.state },
  { header: "Standing signal", kind: "text", read: (i) => nested(i, "standing")?.signal },
  { header: "Contacted", kind: "yesno", read: (i) => i.contacted },
  { header: "Sent", kind: "yesno", read: (i) => i.sent },
  { header: "Delivered", kind: "yesno", read: (i) => i.delivered },
  { header: "Opened", kind: "yesno", read: (i) => i.opened },
  // The page calls a measured click on the brand's site a website visit; the wire calls it a
  // click. Same fact, and the file agrees with the screen it came from.
  { header: "Website visit", kind: "yesno", read: (i) => i.clicked },
  { header: "Replied", kind: "yesno", read: (i) => i.replied },
  { header: "Reply sentiment", kind: "text", read: (i) => i.replyClassification },
  { header: "Bounced", kind: "yesno", read: (i) => i.bounced },
  { header: "Unsubscribed", kind: "yesno", read: (i) => i.unsubscribed },
  { header: "Served at", kind: "datetime", read: (i) => i.servedAt },
  { header: "First contacted at", kind: "datetime", read: (i) => i.firstContactedAt },
  { header: "First sent at", kind: "datetime", read: (i) => i.firstSentAt },
  { header: "First delivered at", kind: "datetime", read: (i) => i.firstDeliveredAt },
  { header: "First opened at", kind: "datetime", read: (i) => i.firstOpenedAt },
  { header: "First website visit at", kind: "datetime", read: (i) => i.firstClickedAt },
  { header: "First replied at", kind: "datetime", read: (i) => i.firstRepliedAt },
  { header: "First bounced at", kind: "datetime", read: (i) => i.firstBouncedAt },
  { header: "First unsubscribed at", kind: "datetime", read: (i) => i.firstUnsubscribedAt },
  // Suppression ACROSS the org, not on this brand: somebody cleaning a list needs to tell
  // "never write to this person again, under any brand" from "this brand stopped writing to
  // them". Same two facts the JSON carries as `global.bounced` / `global.unsubscribed`, read
  // off the SAME item. They are appended rather than placed beside their per-brand namesakes
  // because a customer may already have a sheet built on the column positions this file has
  // today, and the headings carry the distinction instead.
  { header: "Bounced (any brand)", kind: "yesno", read: (i) => nested(i, "global")?.bounced },
  { header: "Unsubscribed (any brand)", kind: "yesno", read: (i) => nested(i, "global")?.unsubscribed },
] as const;

/**
 * A readable instant: `YYYY-MM-DD HH:MM:SS`, UTC, which every spreadsheet parses as a date.
 *
 * A value that is not a timestamp throws rather than being written through as text: a cell that
 * silently says something other than a date is the same class of bug as a filter that is accepted
 * and dropped. The instants reaching here are already normalized (see `toIsoTimestamp`).
 */
function formatExportDate(value: unknown): string {
  if (value === null || value === undefined || value === "") return "";
  const parsed = value instanceof Date ? value : new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`[lead-service] invalid export timestamp: ${String(value)}`);
  }
  return parsed.toISOString().slice(0, 19).replace("T", " ");
}

function formatExportText(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.filter((v) => v !== null && v !== undefined).join(", ");
  return String(value);
}

/** The cell a reader sees, per the column's own kind. */
export function formatExportCell(kind: CellKind, value: unknown): string {
  if (kind === "yesno") return value ? "Yes" : "No";
  if (kind === "datetime") return formatExportDate(value);
  return formatExportText(value);
}

/** One CSV field: quoted always, embedded quotes doubled — the only escaping RFC 4180 asks for. */
function csvCell(text: string): string {
  return '"' + text.replace(/"/g, '""') + '"';
}

/** The heading row, in the words the Leads page uses. */
export function leadExportHeader(): string {
  return LEAD_EXPORT_COLUMNS.map((column) => csvCell(column.header)).join(",") + "\n";
}

/** One export line, built from the SAME object the JSON list emits — never a second projection. */
export function leadExportLine(item: LeadExportItem): string {
  return (
    LEAD_EXPORT_COLUMNS.map((column) => csvCell(formatExportCell(column.kind, column.read(item)))).join(",") +
    "\n"
  );
}
