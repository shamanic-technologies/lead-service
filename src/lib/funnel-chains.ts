/**
 * A sales funnel is a CHAIN, and what is known about one of its steps constrains its neighbours.
 *
 * The hand-stated statements (v0.57.0, v0.58.0) enforced two rules between a step and ITSELF — an
 * outcome retracts an earlier "never", a "never" over a step that already happened is refused —
 * and nothing at all BETWEEN steps. So this was reachable, and is nonsense:
 *
 *     Meeting booked     never
 *     Meeting attended   outcome
 *     Paid client        outcome
 *
 * Nobody attends a meeting that was never booked. Two rules follow from the chain, and they run in
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
 * "Before" and "after" mean nothing without knowing WHICH chain the lead is on: a campaign selling
 * meetings off replies runs reply -> booked -> attended -> paid, one selling off the website runs
 * visit -> signup -> paid. So the order is per FUNNEL. The funnel is stated by the CAMPAIGN
 * (campaign-service owns `funnelKey`, and lead rows carry their campaign), and the catalogue below
 * is the funnel key -> chain mapping expressed in THIS service's step vocabulary.
 *
 * Why the mapping lives here rather than being read off brand-service: brand-service publishes a
 * funnel's `steps` as DISPLAY LABELS ("Website visit", "Signup", "Paid client"), which is a
 * different vocabulary from the outcome names a statement is made in ("website_visit", "signup",
 * "sale"). Something has to translate between the two, and the translation is a property of this
 * service's vocabulary, not of any brand's configuration. The funnel KEY is read from the producer
 * and never inferred (never from a goal — two funnels answer to the same goal), which is the part
 * that must not be re-derived.
 *
 * A chain deliberately contains ONLY steps a statement can be made about. `conversation_reply` and
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
 * Each funnel's chain, in order, in this service's step vocabulary.
 *
 * `sales_from_conversation` closes inside the conversation: no meeting is ever booked, so its only
 * statable step is the sale. A one-step chain is a real chain — it simply implies nothing.
 */
export const FUNNEL_STEP_CHAINS: Readonly<Record<FunnelKey, readonly LeadStepOutcomeName[]>> = {
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

/** The ordered chain a funnel key names, or null when it names no funnel this service knows. */
export function chainForFunnelKey(value: unknown): readonly LeadStepOutcomeName[] | null {
  const key = canonicalizeFunnelKey(value);
  return key ? FUNNEL_STEP_CHAINS[key] : null;
}

/** Where a step sits on a chain, or -1 when the chain does not contain it. */
export function chainIndexOf(
  chain: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): number {
  return chain.indexOf(step);
}

/**
 * Every step a "never" on `step` also makes never — `step` itself and everything AFTER it on the
 * chain. A step the chain does not contain constrains nothing but itself.
 */
export function stepAndLater(
  chain: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): LeadStepOutcomeName[] {
  const i = chainIndexOf(chain, step);
  return i < 0 ? [step] : [...chain.slice(i)];
}

/**
 * Every step an OUTCOME on `step` also makes reached — `step` itself and everything BEFORE it on
 * the chain. A step the chain does not contain constrains nothing but itself.
 */
export function stepAndEarlier(
  chain: readonly LeadStepOutcomeName[],
  step: LeadStepOutcomeName,
): LeadStepOutcomeName[] {
  const i = chainIndexOf(chain, step);
  return i < 0 ? [step] : [...chain.slice(0, i + 1)];
}
