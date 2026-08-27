/**
 * A step outcome a HUMAN states about one lead, and its negative twin.
 *
 * The website tracker reports what it can see. It sees roughly one conversion in ten: 26 of the
 * 29 conversion events ever received are `unmatched`, almost always because the person signed up
 * with an address we never emailed. Everything the tracker cannot see — a meeting somebody took,
 * a deal that closed on a call — has, until now, lived as typed notes in whichever tool the person
 * happened to be looking at, where nothing that computes the funnel, the ROI or the cost per
 * acquisition can read it.
 *
 * So a person looking at ONE lead states the fact themselves, and it counts exactly like a tracked
 * one. Two statements are possible about a step, and they are NOT the same kind of thing:
 *
 *   - an OUTCOME — "this happened". Stored in `conversion_events`, the ledger every consumer
 *     already counts, tagged `source = 'manual'`. Nothing downstream had to change for a
 *     hand-stated signup to move the brand's signup count, and `source` keeps it distinguishable
 *     from a tracker-reported one after the fact.
 *   - a NEVER — "this will not happen". NOT an outcome, so it deliberately does not live in that
 *     ledger at all: it is a `lead_step_disqualifications` row, which nothing counts. Its only job
 *     is to let a consumer tell a lead that is DEAD at a step from one still PENDING, which
 *     nothing could do before — the difference between a cost-per-acquisition denominator that is
 *     still waiting and one that never will be.
 *
 * The identity is the lead row itself (`leads_campaigns.id`, the id a list row already carries),
 * so the caller re-supplies nothing and the statement is attributable to the campaign the row
 * belongs to, not merely to the brand.
 */
import {
  CONVERSION_EVENTS,
  canonicalizeConversionEvent,
  type ConversionEventName,
} from "./conversions.js";

/**
 * A meeting somebody actually ATTENDED. Sales funnels have carried this step all along and
 * brand-service prices with a booked-to-attended rate, but no event of the kind existed anywhere
 * in the fleet, so that rate could never be measured against reality.
 *
 * It is deliberately NOT accepted by the website tracker: attendance happens off the client's
 * website, so a page-load tag has nothing to observe. The tracker's ingest vocabulary is
 * unchanged; this name is statable by hand only.
 */
export const MEETING_ATTENDED = "meeting_attended" as const;

/**
 * The lead LANDED ON the brand's website. Two of the four sales funnels START here, and until now
 * it was the one step of those funnels nobody could state: the panel showed a row it could only
 * read above three it could act on, and there was no way at all to correct the first step when the
 * automatic signal missed it.
 *
 * The automatic signal is a CLICK measured by the delivery layer (email-gateway), and it misses
 * for the same family of reasons the tracker's identity waterfall misses. A hand-stated visit ADDS
 * to that; it never replaces or suppresses it, and the delivery layer is not touched. It is
 * deliberately NOT accepted by the website tracker's ingest either — the tracker already reports
 * what it can see, and its vocabulary is unchanged — so, like `meeting_attended`, this name is
 * statable by hand only.
 */
export const WEBSITE_VISIT = "website_visit" as const;

export type LeadStepOutcomeName =
  | ConversionEventName
  | typeof MEETING_ATTENDED
  | typeof WEBSITE_VISIT;

/**
 * Every outcome a step of a sales funnel can carry — the four the tracker reports plus the two
 * only a human can state. This is the vocabulary the COUNT contracts answer for; `CONVERSION_EVENTS`
 * stays the (narrower) set the public tracker ingest accepts.
 */
export const LEAD_STEP_OUTCOMES: readonly LeadStepOutcomeName[] = [
  ...CONVERSION_EVENTS,
  MEETING_ATTENDED,
  WEBSITE_VISIT,
];

/**
 * Canonicalize a step name for the OUTCOME vocabulary: the four tracker events (legacy
 * "purchase" folded to "sale", exactly as the ingest folds it) plus "meeting_attended".
 * Anything else — "ping", garbage, a non-string — is null, and every caller 400s on null.
 */
export function canonicalizeStepOutcome(value: unknown): LeadStepOutcomeName | null {
  if (value === MEETING_ATTENDED) return MEETING_ATTENDED;
  if (value === WEBSITE_VISIT) return WEBSITE_VISIT;
  return canonicalizeConversionEvent(value);
}

/** Where an outcome came from. Frozen on the row at write, never inferred on read. */
export type StatementSource = "tracker" | "manual";
export const STATEMENT_SOURCES: readonly StatementSource[] = ["tracker", "manual"];

/** What a human stated about a step: it happened, or it never will. */
export type StatementKind = "outcome" | "never";
export const STATEMENT_KINDS: readonly StatementKind[] = ["outcome", "never"];

/** What a consumer reads back per step. `pending` is the absence of both statements. */
export type StepState = "outcome" | "never" | "pending";

/**
 * The dedupe signature a hand-stated OUTCOME carries in `conversion_events`.
 *
 * Keyed on the lead ROW and the step, so restating the same step for the same lead corrects the
 * first statement (value, note, when) instead of counting a second time — the same guarantee the
 * tracker's own (brand, event, identity, day) signature gives, expressed for a caller that names
 * the lead outright. The `m:` prefix keeps it disjoint from the tracker's `k:`/`a:` signatures.
 */
export function manualOutcomeSignature(leadCampaignId: string, step: LeadStepOutcomeName): string {
  return `m:${leadCampaignId}:${step}`;
}
