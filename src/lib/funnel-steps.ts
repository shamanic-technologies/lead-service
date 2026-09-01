/**
 * A sales funnel is a FUNNEL, and what is known about one of its steps constrains its neighbours.
 *
 * The hand-stated statements (v0.57.0, v0.58.0) enforced two rules between a step and ITSELF — an
 * outcome retracts an earlier "never", a "never" over a step that already happened is refused —
 * and nothing at all BETWEEN steps. So this was reachable, and is nonsense:
 *
 *     Meeting booked     never
 *     Meeting attended   outcome
 *     Paid client        outcome
 *
 * Nobody attends a meeting that was never booked. Two rules follow from the funnel, and they run in
 * opposite directions:
 *
 *   - a NEVER constrains everything AFTER it. A lead that will never book has, by the same
 *     statement, never attended and never paid.
 *   - an OUTCOME constrains everything BEFORE it. A lead that paid necessarily got through the
 *     steps that lead to paying.
 *
 * A step neither rule reaches, and nobody spoke about, stays PENDING — the honest "still on its
 * way".
 *
 * "Before" and "after" mean nothing without knowing WHICH funnel the lead is on: a campaign selling
 * meetings off replies runs reply -> booked -> attended -> paid, one selling off the website runs
 * visit -> signup -> paid. So the order is per FUNNEL. The funnel is stated by the CAMPAIGN
 * (campaign-service owns `funnelKey`, and lead rows carry their campaign), and the catalogue below
 * is the funnel key -> funnel mapping expressed in THIS service's step vocabulary.
 *
 * Why the mapping lives here rather than being read off brand-service: brand-service publishes a
 * funnel's `steps` as DISPLAY LABELS ("Website visit", "Signup", "Paid client"), which is a
 * different vocabulary from the outcome names a statement is made in ("website_visit", "signup",
 * "sale"). Something has to translate between the two, and the translation is a property of this
 * service's vocabulary, not of any brand's configuration. The funnel KEY is read from the producer
 * and never inferred (never from a goal — two funnels answer to the same goal), which is the part
 * that must not be re-derived.
 *
 * A funnel deliberately contains ONLY steps a statement can be made about. `conversation_reply` and
 * `ad_click` start two of the catalogue's funnels and are not conversion outcomes here, so they are
 * absent — which changes nothing about the order of the steps that remain.
 */
import type { LeadStepOutcomeName } from "./step-statements.js";

/** The funnel catalogue, exactly as brand-service publishes it. */
export const FUNNEL_KEYS = [
  "sales_meetings_from_conversation",
  "sales_meetings_from_website",
  "website_purchases",
  "form_magnet",
  "sales_from_conversation",
  "sales_meetings_from_ads",
  "lead_forms_from_ads",
] as const;

export type FunnelKey = (typeof FUNNEL_KEYS)[number];

/**
 * The pre-retirement spellings. brand-service accepts them on write forever and never emits them;
 * a campaign row stored before the rename can still carry one, so they resolve here too rather
 * than reading as an unknown funnel.
 */
const LEGACY_FUNNEL_KEYS: Readonly<Record<string, FunnelKey>> = {
  reply_meeting: "sales_meetings_from_conversation",
  visit_meeting: "sales_meetings_from_website",
  visit_signup: "website_purchases",
  visit_form: "form_magnet",
};

/**
 * Each funnel's funnel, in order, in this service's step vocabulary.
 *
 * `sales_from_conversation` closes inside the conversation: no meeting is ever booked, so its only
 * statable step is the sale. A one-step funnel is a real funnel — it simply implies nothing.
 */
export const FUNNEL_STEPS: Readonly<Record<FunnelKey, readonly LeadStepOutcomeName[]>> = {
  // Positive reply -> Meeting booked -> Meeting attended -> Paid client
  sales_meetings_from_conversation: ["meeting_booked", "meeting_attended", "sale"],
  // Website visit -> Meeting booked -> Meeting attended -> Paid client
  sales_meetings_from_website: ["website_visit", "meeting_booked", "meeting_attended", "sale"],
  // Website visit -> Signup -> Paid client
  website_purchases: ["website_visit", "signup", "sale"],
  // Website visit -> Form filled -> Paid client
  form_magnet: ["website_visit", "form_submission", "sale"],
  // Positive reply -> Paid client
  sales_from_conversation: ["sale"],
  // Ad click -> Meeting booked -> Meeting attended -> Paid client
  sales_meetings_from_ads: ["meeting_booked", "meeting_attended", "sale"],
  // Ad click -> Lead form submitted -> Paid client
  lead_forms_from_ads: ["form_submission", "sale"],
};

/** The canonical funnel key a stored spelling names, or null when it names none. */
export function canonicalizeFunnelKey(value: unknown): FunnelKey | null {
  if (typeof value !== "string") return null;
  if ((FUNNEL_KEYS as readonly string[]).includes(value)) return value as FunnelKey;
  return LEGACY_FUNNEL_KEYS[value] ?? null;
}

/** The ordered funnel a funnel key names, or null when it names no funnel this service knows. */
export function stepsForFunnelKey(value: unknown): readonly LeadStepOutcomeName[] | null {
  const key = canonicalizeFunnelKey(value);
  return key ? FUNNEL_STEPS[key] : null;
}

/** Where a step sits on a funnel, or -1 when the funnel does not contain it. */
export function stepIndexOf(
  funnelSteps: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): number {
  return funnelSteps.indexOf(step);
}

/**
 * Every step a "never" on `step` also makes never — `step` itself and everything AFTER it on the
 * funnel. A step the funnel does not contain constrains nothing but itself.
 */
export function stepAndLater(
  funnelSteps: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): LeadStepOutcomeName[] {
  const i = stepIndexOf(funnelSteps, step);
  return i < 0 ? [step] : [...funnelSteps.slice(i)];
}

/**
 * Every step an OUTCOME on `step` also makes reached — `step` itself and everything BEFORE it on
 * the funnel. A step the funnel does not contain constrains nothing but itself.
 */
export function stepAndEarlier(
  funnelSteps: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): LeadStepOutcomeName[] {
  const i = stepIndexOf(funnelSteps, step);
  return i < 0 ? [step] : [...funnelSteps.slice(0, i + 1)];
}

/**
 * HOW a lead gets ONTO a funnel, and whether this service can observe it.
 *
 * A funnel's first STATABLE step is not always its first step: `sales_meetings_from_conversation`
 * runs reply -> booked -> attended -> paid, and the reply is not a conversion outcome here, so
 * `FUNNEL_STEPS` starts it at `meeting_booked`. The entry is what the campaign is actually selling
 * its way into, and it is what decides whether a signal means buying intent ON THIS CAMPAIGN:
 *
 *   - a visit-led funnel is entered by LANDING ON the site, which the delivery layer measures as a
 *     CLICK on the email we sent (`delivery_click`).
 *   - a conversation-led funnel is entered by the person REPLYING with interest, which the
 *     delivery layer classifies (`positive_reply`).
 *   - an ads-led funnel is entered by a click on an AD, which no signal this service holds can
 *     observe (`null`). Not a gap to paper over: it is stated as unresolved rather than guessed.
 *
 * This is why the same click means different things on different campaigns. On `form_magnet` it is
 * the funnel's own first step. On `sales_meetings_from_conversation` it is a person visiting a site
 * the campaign does not price a visit to, so it is engagement and nothing more.
 */
export type FunnelEntryMeasure = "delivery_click" | "positive_reply";

export interface FunnelEntry {
  /** The step somebody takes to get onto the funnel, in brand-service's funnel vocabulary. */
  step: string;
  /** The signal this service reads it off, or null when it holds no signal for it. */
  measure: FunnelEntryMeasure | null;
}

export const FUNNEL_ENTRY: Readonly<Record<FunnelKey, FunnelEntry>> = {
  sales_meetings_from_conversation: { step: "conversation_reply", measure: "positive_reply" },
  sales_meetings_from_website: { step: "website_visit", measure: "delivery_click" },
  website_purchases: { step: "website_visit", measure: "delivery_click" },
  form_magnet: { step: "website_visit", measure: "delivery_click" },
  sales_from_conversation: { step: "conversation_reply", measure: "positive_reply" },
  sales_meetings_from_ads: { step: "ad_click", measure: null },
  lead_forms_from_ads: { step: "ad_click", measure: null },
};

/** How a funnel key's funnel is entered, or null when it names no funnel this service knows. */
export function entryForFunnelKey(value: unknown): FunnelEntry | null {
  const key = canonicalizeFunnelKey(value);
  return key ? FUNNEL_ENTRY[key] : null;
}
