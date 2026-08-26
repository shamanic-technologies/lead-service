import { describe, it, expect } from "vitest";
import { CONVERSION_EVENTS, canonicalizeConversionEvent } from "../../src/lib/conversions.js";
import {
  LEAD_STEP_OUTCOMES,
  MEETING_ATTENDED,
  WEBSITE_VISIT,
  canonicalizeStepOutcome,
  manualOutcomeSignature,
} from "../../src/lib/step-statements.js";

describe("the step-outcome vocabulary", () => {
  it("is the tracker's four plus the two only a human can state", () => {
    expect(LEAD_STEP_OUTCOMES).toEqual([
      ...CONVERSION_EVENTS,
      "meeting_attended",
      "website_visit",
    ]);
  });

  it("keeps website_visit OUT of what the website tracker accepts", () => {
    // A visit is measured automatically as a CLICK by the delivery layer, not by the page tag,
    // and the tracker's ingest vocabulary is deliberately unchanged by this feature. A human
    // states the visits that signal missed.
    expect(canonicalizeConversionEvent(WEBSITE_VISIT)).toBeNull();
    expect(canonicalizeStepOutcome(WEBSITE_VISIT)).toBe("website_visit");
  });

  it("leaves the five steps that already shipped exactly as they were", () => {
    for (const step of ["signup", "meeting_booked", "form_submission", "sale", "meeting_attended"]) {
      expect(canonicalizeStepOutcome(step)).toBe(step);
    }
    expect(LEAD_STEP_OUTCOMES.slice(0, 5)).toEqual([
      ...CONVERSION_EVENTS,
      "meeting_attended",
    ]);
  });

  it("keeps meeting_attended OUT of what the website tracker accepts", () => {
    // A page-load tag cannot observe somebody showing up to a meeting, and the tracker's ingest
    // contract is deliberately unchanged by this feature.
    expect(canonicalizeConversionEvent(MEETING_ATTENDED)).toBeNull();
    expect(canonicalizeStepOutcome(MEETING_ATTENDED)).toBe("meeting_attended");
  });

  it("folds the legacy purchase spelling onto sale, exactly as ingest does", () => {
    expect(canonicalizeStepOutcome("purchase")).toBe("sale");
    expect(canonicalizeStepOutcome("sale")).toBe("sale");
  });

  it("refuses the liveness heartbeat and anything else", () => {
    expect(canonicalizeStepOutcome("ping")).toBeNull();
    expect(canonicalizeStepOutcome("")).toBeNull();
    expect(canonicalizeStepOutcome(undefined)).toBeNull();
    expect(canonicalizeStepOutcome(42)).toBeNull();
  });
});

describe("manualOutcomeSignature", () => {
  it("keys on the lead ROW and the step, so restating corrects instead of counting twice", () => {
    expect(manualOutcomeSignature("row-1", "sale")).toBe(manualOutcomeSignature("row-1", "sale"));
    expect(manualOutcomeSignature("row-1", "sale")).not.toBe(
      manualOutcomeSignature("row-1", "signup"),
    );
    expect(manualOutcomeSignature("row-1", "sale")).not.toBe(
      manualOutcomeSignature("row-2", "sale"),
    );
  });

  it("stays disjoint from the tracker's own dedupe signatures", () => {
    // Tracker signatures are "k:<dedupeKey>" and "a:<event>:<identity>:<day>".
    expect(manualOutcomeSignature("row-1", "sale").startsWith("m:")).toBe(true);
  });
});
