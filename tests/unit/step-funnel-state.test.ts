import { describe, it, expect } from "vitest";
import { resolveStepStates, type StatedNever, type StatedOutcome } from "../../src/lib/step-funnel-state.js";
import { FUNNEL_STEPS, stepAndEarlier, stepAndLater, canonicalizeFunnelKey } from "../../src/lib/funnel-steps.js";
import { LEAD_STEP_OUTCOMES, type LeadStepOutcomeName } from "../../src/lib/step-statements.js";

const REPLY_MEETINGS = FUNNEL_STEPS.sales_meetings_from_conversation;
const WEBSITE = FUNNEL_STEPS.website_purchases;

function outcome(partial: Partial<StatedOutcome> = {}): StatedOutcome {
  return {
    source: "manual",
    valueCents: null,
    note: null,
    statedByUserId: "user-1",
    at: "2026-08-20T10:00:00.000Z",
    ...partial,
  };
}
function never(partial: Partial<StatedNever> = {}): StatedNever {
  return { note: null, statedByUserId: "user-1", at: "2026-08-20T10:00:00.000Z", ...partial };
}

function states(
  funnelSteps: readonly LeadStepOutcomeName[],
  outcomes: Array<[LeadStepOutcomeName, StatedOutcome]> = [],
  nevers: Array<[LeadStepOutcomeName, StatedNever]> = [],
) {
  const resolved = resolveStepStates({
    allSteps: LEAD_STEP_OUTCOMES,
    funnelSteps,
    outcomes: new Map(outcomes),
    nevers: new Map(nevers),
  });
  return Object.fromEntries(resolved.map((s) => [s.step, s]));
}

describe("a never constrains everything AFTER it on the funnel", () => {
  it("a lead that will never book has never attended and never paid", () => {
    const s = states(REPLY_MEETINGS, [], [["meeting_booked", never()]]);
    expect(s.meeting_booked).toMatchObject({ state: "never", origin: "stated" });
    expect(s.meeting_attended).toMatchObject({
      state: "never",
      origin: "implied",
      impliedBy: "meeting_booked",
      statedByUserId: null,
      note: null,
      at: null,
    });
    expect(s.sale).toMatchObject({ state: "never", origin: "implied", impliedBy: "meeting_booked" });
  });

  it("reaches nothing BEFORE it — an earlier step is still pending", () => {
    const s = states(WEBSITE, [], [["signup", never()]]);
    expect(s.website_visit.state).toBe("pending");
    expect(s.sale).toMatchObject({ state: "never", origin: "implied" });
  });
});

describe("an outcome constrains everything BEFORE it on the funnel", () => {
  it("a lead that paid necessarily got through the steps that lead to paying", () => {
    const s = states(REPLY_MEETINGS, [["sale", outcome({ valueCents: 490000 })]]);
    expect(s.sale).toMatchObject({ state: "outcome", origin: "stated", valueCents: 490000 });
    expect(s.meeting_attended).toMatchObject({
      state: "outcome",
      origin: "implied",
      impliedBy: "sale",
      source: null,
      valueCents: null,
      statedByUserId: null,
    });
    expect(s.meeting_booked).toMatchObject({ state: "outcome", origin: "implied", impliedBy: "sale" });
  });

  it("says nothing about what comes AFTER — a later step stays pending", () => {
    const s = states(REPLY_MEETINGS, [["meeting_booked", outcome()]]);
    expect(s.meeting_attended.state).toBe("pending");
    expect(s.sale.state).toBe("pending");
  });
});

describe("the two directions agree whichever order the statements arrived in", () => {
  it("a stated never earlier than a stated outcome reads as reached, and the statement survives", () => {
    const s = states(REPLY_MEETINGS, [["sale", outcome()]], [["meeting_booked", never({ note: "no budget" })]]);
    expect(s.meeting_booked).toMatchObject({
      state: "outcome",
      origin: "implied",
      impliedBy: "sale",
      // what the person really said is still readable — nothing is lost to satisfy the funnel
      statedState: "never",
    });
    // ... and it stops propagating: nothing downstream reads as never
    expect(s.meeting_attended.state).toBe("outcome");
    expect(s.sale.state).toBe("outcome");
  });

  it("a never AFTER the deepest outcome still constrains what follows it", () => {
    const s = states(REPLY_MEETINGS, [["meeting_booked", outcome()]], [["meeting_attended", never()]]);
    expect(s.meeting_booked).toMatchObject({ state: "outcome", origin: "stated" });
    expect(s.meeting_attended).toMatchObject({ state: "never", origin: "stated" });
    expect(s.sale).toMatchObject({ state: "never", origin: "implied", impliedBy: "meeting_attended" });
  });
});

describe("a step neither rule reaches stays pending", () => {
  it("nobody spoke about it and no funnel rule touches it", () => {
    const s = states(REPLY_MEETINGS);
    expect(LEAD_STEP_OUTCOMES.every((step) => s[step].state === "pending")).toBe(true);
    expect(s.sale.origin).toBeNull();
  });

  it("a step off this campaign's funnel reads from statements alone", () => {
    const s = states(REPLY_MEETINGS, [], [["meeting_booked", never()]]);
    // signup / form_submission / website_visit are not on the reply-meetings funnel
    expect(s.signup).toMatchObject({ state: "pending", inFunnel: false, funnelStepIndex: null });
    expect(s.form_submission.state).toBe("pending");
    expect(s.website_visit.state).toBe("pending");
  });
});

describe("two campaigns on different funnels get their own step order", () => {
  it("the same statement implies different steps on a different funnel", () => {
    const reply = states(REPLY_MEETINGS, [["sale", outcome()]]);
    const website = states(WEBSITE, [["sale", outcome()]]);
    expect(reply.meeting_booked.state).toBe("outcome");
    expect(reply.signup.state).toBe("pending");
    expect(website.signup.state).toBe("outcome");
    expect(website.website_visit.state).toBe("outcome");
    expect(website.meeting_booked.state).toBe("pending");
  });
});

describe("the funnel slices", () => {
  it("a never covers its own step and everything after", () => {
    expect(stepAndLater(REPLY_MEETINGS, "meeting_booked")).toEqual([
      "meeting_booked",
      "meeting_attended",
      "sale",
    ]);
  });
  it("an outcome covers its own step and everything before", () => {
    expect(stepAndEarlier(REPLY_MEETINGS, "sale")).toEqual([
      "meeting_booked",
      "meeting_attended",
      "sale",
    ]);
  });
  it("a step off the funnel constrains only itself", () => {
    expect(stepAndLater(REPLY_MEETINGS, "signup")).toEqual(["signup"]);
    expect(stepAndEarlier(REPLY_MEETINGS, "signup")).toEqual(["signup"]);
  });
  it("every pre-retirement funnel spelling still resolves", () => {
    expect(canonicalizeFunnelKey("visit_signup")).toBe("website_purchases");
    expect(canonicalizeFunnelKey("reply_meeting")).toBe("sales_meetings_from_conversation");
    expect(canonicalizeFunnelKey("nonsense")).toBeNull();
  });
  it("every funnel is made of steps this service can answer for, and ends at the sale", () => {
    for (const funnelSteps of Object.values(FUNNEL_STEPS)) {
      expect(funnelSteps.length).toBeGreaterThan(0);
      expect(funnelSteps[funnelSteps.length - 1]).toBe("sale");
      for (const step of funnelSteps) expect(LEAD_STEP_OUTCOMES).toContain(step);
      expect(new Set(funnelSteps).size).toBe(funnelSteps.length);
    }
  });
});
