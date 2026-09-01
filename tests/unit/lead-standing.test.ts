import { describe, it, expect } from "vitest";

// Where a lead stands is COMMERCIAL POLICY, and this is the whole of it. Everything here is about
// the ladder in src/lib/lead-standing.ts: which evidence outranks which, what a click means on a
// campaign that sells a visit versus one that sells a reply, and what is answered when a signal
// cannot be resolved at all.

import { FUNNEL_ENTRY, FUNNEL_STEPS, FUNNEL_KEYS, entryForFunnelKey } from "../../src/lib/funnel-steps.js";
import { resolveStepStates } from "../../src/lib/step-funnel-state.js";
import {
  resolveLeadStanding,
  type LeadStandingDelivery,
  type ResolvedLeadFunnel,
} from "../../src/lib/lead-standing.js";
import {
  LEAD_STEP_OUTCOMES,
  type LeadStepOutcomeName,
} from "../../src/lib/step-statements.js";

const QUIET: LeadStandingDelivery = {
  contacted: false,
  opened: false,
  clicked: false,
  replied: false,
  replyClassification: null,
  bounced: false,
  unsubscribed: false,
  globalBounced: false,
  globalUnsubscribed: false,
};

function funnel(key: (typeof FUNNEL_KEYS)[number]): ResolvedLeadFunnel {
  return { key, steps: FUNNEL_STEPS[key], entry: FUNNEL_ENTRY[key] };
}

interface StandArgs {
  funnel?: ResolvedLeadFunnel | null;
  delivery?: Partial<LeadStandingDelivery>;
  status?: string;
  deliveryQueried?: boolean;
  outcomes?: Partial<Record<LeadStepOutcomeName, "manual" | "tracker">>;
  nevers?: LeadStepOutcomeName[];
}

function stand(args: StandArgs = {}) {
  const f = args.funnel === undefined ? funnel("form_magnet") : args.funnel;
  const outcomes = new Map(
    Object.entries(args.outcomes ?? {}).map(([step, source]) => [
      step as LeadStepOutcomeName,
      {
        source: source as "manual" | "tracker",
        valueCents: null,
        costCents: null,
        note: null,
        statedByUserId: null,
        at: "2026-02-02T00:00:00.000Z",
      },
    ]),
  );
  const nevers = new Map(
    (args.nevers ?? []).map((step) => [
      step,
      { costCents: null, note: null, statedByUserId: null, at: "2026-02-02T00:00:00.000Z" },
    ]),
  );
  return resolveLeadStanding({
    lifecycleStatus: args.status ?? "served",
    deliveryQueried: args.deliveryQueried ?? true,
    delivery: { ...QUIET, ...args.delivery },
    funnel: f,
    funnelUnresolvedReason: f ? null : "funnel_unstated",
    steps: resolveStepStates({
      allSteps: LEAD_STEP_OUTCOMES,
      funnelSteps: f?.steps ?? [],
      outcomes,
      nevers,
    }),
  });
}

describe("where a lead stands on the campaign it was served under", () => {
  // THE CASE. A Form Magnet campaign sells visit -> form -> paid; 67 people clicked through to the
  // customer's site and the board showed nobody under Sales interest, because the only per-lead
  // triage anyone had read a positive REPLY and this campaign prices no reply.
  it("counts a website visit on a campaign whose funnel is entered by one", () => {
    const s = stand({ funnel: funnel("form_magnet"), delivery: { contacted: true, clicked: true } });
    expect(s.state).toBe("sales_interest");
    expect(s.reachedEntryStep).toBe(true);
    expect(s.entryStep).toBe("website_visit");
    expect(s.entryMeasure).toBe("delivery_click");
    expect(s.funnelKey).toBe("form_magnet");
    expect(s.reason).toBeNull();
  });

  // The other half of funnel-awareness, and the reason it is not simply "a click is intent".
  it("does NOT count the same click on a campaign whose funnel prices a reply, not a visit", () => {
    const s = stand({
      funnel: funnel("sales_meetings_from_conversation"),
      delivery: { contacted: true, clicked: true },
    });
    expect(s.state).toBe("engaged");
    expect(s.signal).toBe("click");
    expect(s.reachedEntryStep).toBe(false);
    expect(s.entryStep).toBe("conversation_reply");
  });

  it("counts a positive reply on a conversation-led funnel, and not on a visit-led one", () => {
    const conversation = stand({
      funnel: funnel("sales_meetings_from_conversation"),
      delivery: { contacted: true, replied: true, replyClassification: "positive" },
    });
    expect(conversation.state).toBe("sales_interest");
    expect(conversation.signal).toBe("positive_reply");

    const visit = stand({
      funnel: funnel("form_magnet"),
      delivery: { contacted: true, replied: true, replyClassification: "positive" },
    });
    expect(visit.state).toBe("engaged");
    expect(visit.reachedEntryStep).toBe(false);
  });

  // The same person, two campaigns: the (lead, campaign) grain is what lets both answers be true.
  it("lets the same signals stand differently under two campaigns", () => {
    const delivery = { contacted: true, clicked: true };
    expect(stand({ funnel: funnel("website_purchases"), delivery }).state).toBe("sales_interest");
    expect(stand({ funnel: funnel("sales_from_conversation"), delivery }).state).toBe("engaged");
  });

  describe("precedence", () => {
    // No-go, and it is the one rule with no exception anywhere in the ladder.
    it("lets NOTHING override an unsubscribe — not a click, not a stated sale", () => {
      const s = stand({
        delivery: { contacted: true, clicked: true, unsubscribed: true },
        outcomes: { sale: "manual" },
      });
      expect(s.state).toBe("disqualified");
      expect(s.signal).toBe("unsubscribed");
      // Both facts stay true and readable: they DID reach the entry step, and they DID opt out.
      expect(s.reachedEntryStep).toBe(true);
    });

    it("honours a global unsubscribe the same as a scoped one", () => {
      expect(stand({ delivery: { contacted: true, globalUnsubscribed: true } }).state).toBe(
        "disqualified",
      );
    });

    it("puts a human statement above every machine signal", () => {
      const s = stand({
        delivery: { contacted: true, replied: true, replyClassification: "negative" },
        outcomes: { form_submission: "manual" },
      });
      expect(s.state).toBe("sales_interest");
      expect(s.signal).toBe("stated_outcome");
      expect(s.origin).toBe("stated");
      expect(s.deepestStep).toBe("form_submission");
      expect(s.at).toBe("2026-02-02T00:00:00.000Z");
    });

    it("reads the funnel's last step reached as a customer", () => {
      const s = stand({ delivery: { contacted: true }, outcomes: { sale: "manual" } });
      expect(s.state).toBe("customer");
      expect(s.deepestStep).toBe("sale");
    });

    // The funnel's own rule, applied to the standing: an outcome implies the steps before it.
    it("reads an implied step exactly as a stated one, and says it was implied", () => {
      const s = stand({ delivery: { contacted: true }, outcomes: { form_submission: "manual" } });
      expect(s.state).toBe("sales_interest");
      // website_visit is implied by the form submission; the deepest reached step is still the form.
      expect(s.deepestStep).toBe("form_submission");
    });

    it("reads a never as disqualified, whichever step it was stated on", () => {
      const early = stand({ delivery: { contacted: true }, nevers: ["website_visit"] });
      expect(early.state).toBe("disqualified");
      expect(early.signal).toBe("stated_never");
      // Stated on the first step, so the last step is dead by implication, not by statement.
      expect(early.origin).toBe("implied");

      const late = stand({ delivery: { contacted: true }, nevers: ["sale"] });
      expect(late.state).toBe("disqualified");
      expect(late.origin).toBe("stated");
    });

    // A click on the campaign that sells a visit is a fact about the funnel; a classification is a
    // judgement about a message. The fact wins.
    it("puts the funnel's own entry step above a negative reply classification", () => {
      const s = stand({
        funnel: funnel("form_magnet"),
        delivery: { contacted: true, clicked: true, replied: true, replyClassification: "negative" },
      });
      expect(s.state).toBe("sales_interest");
      expect(s.signal).toBe("measured_visit");
    });

    // Disqualified means ONE thing: we realised this person is not our target — the wrong
    // contact, or gone from the role. It is ordinary sales qualification, and it is the only
    // reading of a reply that takes a lead out of play.
    it("disqualifies a reply the provider reads as permanently about the PERSON", () => {
      const s = stand({
        funnel: funnel("form_magnet"),
        delivery: {
          contacted: true,
          replied: true,
          replyClassification: "negative",
          disqualified: true,
        },
      });
      expect(s.state).toBe("disqualified");
      expect(s.signal).toBe("disqualifying_reply");
      expect(s.origin).toBe("measured");
    });

    // A decline is a judgement about the MOMENT. The person is still reachable and the lead is
    // still recyclable, so they stay in play and the "no" is named rather than used as a verdict.
    it("does NOT disqualify a decline about the moment — it stays in play", () => {
      const s = stand({
        funnel: funnel("form_magnet"),
        delivery: {
          contacted: true,
          replied: true,
          replyClassification: "negative",
          disqualified: false,
        },
      });
      expect(s.state).toBe("engaged");
      expect(s.signal).toBe("negative_reply");
    });

    // Absent is a third state and it is neither of the other two: "no" would be a claim this
    // service makes on the provider's behalf, "yes" is the bug this closes.
    it("states that it cannot tell when the provider serves no disqualification reading", () => {
      const s = stand({
        funnel: funnel("form_magnet"),
        delivery: { contacted: true, replied: true, replyClassification: "negative" },
      });
      expect(s.state).toBe("unresolved");
      expect(s.reason).toBe("reply_disqualification_unknown");
      expect(s.signal).toBe("negative_reply");
      expect(s.origin).toBeNull();
    });

    // A disqualifying reply is still a machine reading, so it sits exactly where the negative
    // reply always sat: below a human statement, and below the funnel's own entry step.
    it("keeps a permanent disqualification below a human statement and below the entry step", () => {
      const stated = stand({
        delivery: {
          contacted: true,
          replied: true,
          replyClassification: "negative",
          disqualified: true,
        },
        outcomes: { form_submission: "manual" },
      });
      expect(stated.state).toBe("sales_interest");
      expect(stated.signal).toBe("stated_outcome");

      const clicked = stand({
        funnel: funnel("form_magnet"),
        delivery: {
          contacted: true,
          clicked: true,
          replied: true,
          replyClassification: "negative",
          disqualified: true,
        },
      });
      expect(clicked.state).toBe("sales_interest");
      expect(clicked.signal).toBe("measured_visit");
    });

    // The opt-out column is drawn off `signal`, so being out of play by our judgement and being
    // out of play by the prospect's own act must never collapse into one answer.
    it("keeps an opt-out distinguishable from a disqualification", () => {
      const out = stand({
        delivery: {
          contacted: true,
          unsubscribed: true,
          replied: true,
          replyClassification: "negative",
          disqualified: true,
        },
      });
      expect(out.state).toBe("disqualified");
      expect(out.signal).toBe("unsubscribed");
    });

    it("does NOT disqualify a bounce — it names it and leaves the person in play", () => {
      // A bad address says nothing about whether the human behind it would buy. It is a
      // failure of DELIVERY, so the lead stays contacted and the bounce is the evidence
      // rather than the verdict; the address is the thing to repair.
      const s = stand({ delivery: { contacted: true, bounced: true } });
      expect(s.state).toBe("contacted");
      expect(s.signal).toBe("bounced");
      // A global bounce reads the same way — it is the same fact about the address.
      const g = stand({ delivery: { contacted: true, globalBounced: true } });
      expect(g.state).toBe("contacted");
      expect(g.signal).toBe("bounced");
    });

    it("lets every signal above it outrank a bounce, so a later one cannot demote a lead", () => {
      // A bounce on a follow-up must not take back a visit the person already made, nor
      // a "no" they already said.
      const reached = stand({
        funnel: funnel("form_magnet"),
        delivery: { contacted: true, clicked: true, bounced: true },
      });
      expect(reached.state).toBe("sales_interest");
      const said = stand({
        funnel: funnel("form_magnet"),
        delivery: {
          contacted: true,
          replied: true,
          replyClassification: "negative",
          disqualified: true,
          bounced: true,
        },
      });
      expect(said.state).toBe("disqualified");
      expect(said.signal).toBe("disqualifying_reply");
    });

    it("still disqualifies an OPT-OUT, which is the prospect's own binding act", () => {
      const s = stand({ delivery: { contacted: true, unsubscribed: true, bounced: true } });
      expect(s.state).toBe("disqualified");
      expect(s.signal).toBe("unsubscribed");
    });

    it("walks down through engaged and contacted", () => {
      expect(stand({ delivery: { contacted: true, replied: true } }).signal).toBe("reply");
      expect(stand({ delivery: { contacted: true, replied: true } }).state).toBe("engaged");
      expect(stand({ delivery: { contacted: true, opened: true } }).signal).toBe("open");
      expect(stand({ delivery: { contacted: true } }).state).toBe("contacted");
      expect(stand({ delivery: {} }).state).toBe("not_contacted");
    });
  });

  describe("what is NOT answered", () => {
    // No-go: do not fabricate an answer for a lead nobody ever contacted.
    it("answers not_contacted for a row that was never served, and judges nothing further", () => {
      for (const status of ["buffered", "claimed", "skipped"]) {
        const s = stand({ status, delivery: { contacted: true, clicked: true } });
        expect(s.state).toBe("not_contacted");
        expect(s.signal).toBe("not_served");
      }
    });

    // No-go: a signal that cannot be resolved is stated as unresolved, never defaulted.
    it("says unresolved when the delivery layer was never asked", () => {
      const s = stand({ deliveryQueried: false });
      expect(s.state).toBe("unresolved");
      expect(s.reason).toBe("delivery_not_queried");
      expect(s.reachedEntryStep).toBeNull();
    });

    it("still answers from a stated outcome when the delivery layer was never asked", () => {
      const s = stand({ deliveryQueried: false, outcomes: { sale: "manual" } });
      expect(s.state).toBe("customer");
    });

    it("says unresolved when the campaign's funnel could not be resolved", () => {
      const s = stand({ funnel: null, delivery: { contacted: true, clicked: true } });
      expect(s.state).toBe("unresolved");
      expect(s.reason).toBe("funnel_unstated");
      expect(s.funnelKey).toBeNull();
      expect(s.reachedEntryStep).toBeNull();
    });

    // An ad click is a real entry step this service holds no signal for. `false` would be a claim.
    it("answers null — never false — for a funnel entered by something it cannot observe", () => {
      const s = stand({
        funnel: funnel("sales_meetings_from_ads"),
        delivery: { contacted: true, clicked: true },
      });
      expect(s.entryStep).toBe("ad_click");
      expect(s.entryMeasure).toBeNull();
      expect(s.reachedEntryStep).toBeNull();
      // The standing itself still says what IS known: they clicked, which is not the step sold.
      expect(s.state).toBe("engaged");
    });

    it("still says the entry step was reached on an ads funnel when a later step is stated", () => {
      const s = stand({
        funnel: funnel("sales_meetings_from_ads"),
        delivery: { contacted: true },
        outcomes: { meeting_booked: "manual" },
      });
      expect(s.reachedEntryStep).toBe(true);
      expect(s.state).toBe("sales_interest");
    });
  });

  describe("the funnel entry catalogue", () => {
    it("names an entry for every funnel this service knows", () => {
      for (const key of FUNNEL_KEYS) {
        const entry = FUNNEL_ENTRY[key];
        expect(entry.step.length).toBeGreaterThan(0);
        // A visit-led funnel's entry IS its first statable step; the others start before theirs.
        if (entry.measure === "delivery_click") expect(FUNNEL_STEPS[key][0]).toBe("website_visit");
      }
    });

    it("resolves a pre-retirement funnel spelling to the same entry", () => {
      expect(entryForFunnelKey("visit_form")).toEqual(FUNNEL_ENTRY.form_magnet);
      expect(entryForFunnelKey("reply_meeting")).toEqual(
        FUNNEL_ENTRY.sales_meetings_from_conversation,
      );
      expect(entryForFunnelKey("not_a_funnel")).toBeNull();
    });
  });
});
