import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { resolveStepStates } from "../../src/lib/step-funnel-state.js";
import { FUNNEL_STEPS } from "../../src/lib/funnel-steps.js";
import { LEAD_STEP_OUTCOMES } from "../../src/lib/step-statements.js";

/**
 * A sales funnel is a SALES FUNNEL on the wire, and nowhere is it a "chain".
 *
 * The word was the fleet's second name for the same thing, and it is retired everywhere else. It
 * survived longest here, which is why the two consumer apps had to read a spelling they did not
 * choose. Nothing about the DATA changed with the rename — same steps, same order, same meaning —
 * so the only thing that can bring the word back is a new field or a new description, and this is
 * what fails the build when one does.
 *
 * The one carve-out is the word used for something that genuinely is not a sales funnel: a CALL
 * chain, a promise chain, a cause chain. Those are allowed by exact phrase, never by proximity.
 */
const ALLOWED_PHRASES = ["call chain"];

function stripAllowed(text: string): string {
  let out = text;
  for (const phrase of ALLOWED_PHRASES) {
    out = out.split(phrase).join("");
  }
  return out;
}

function walk(node: unknown, path: string, visit: (path: string, key: string, value: unknown) => void) {
  if (Array.isArray(node)) {
    node.forEach((child, i) => walk(child, `${path}[${i}]`, visit));
    return;
  }
  if (node && typeof node === "object") {
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      visit(path, key, value);
      walk(value, `${path}.${key}`, visit);
    }
  }
}

describe("the published contract never calls a sales funnel a chain", () => {
  const spec = JSON.parse(readFileSync(new URL("../../openapi.json", import.meta.url), "utf8"));

  it("names no property with the retired word", () => {
    const offenders: string[] = [];
    walk(spec, "$", (path, key, value) => {
      if (path.endsWith(".properties") && /chain/i.test(key)) {
        offenders.push(`${path}.${key}`);
      }
      void value;
    });
    expect(offenders).toEqual([]);
  });

  it("uses the word in no description that is not about a call chain", () => {
    const offenders: string[] = [];
    walk(spec, "$", (path, key, value) => {
      if (key !== "description" && key !== "summary") return;
      if (typeof value !== "string") return;
      if (/chain/i.test(stripAllowed(value))) offenders.push(`${path}.${key}`);
    });
    expect(offenders).toEqual([]);
  });

  it("serves the funnel's steps as funnelSteps, and a step's membership as inFunnel", () => {
    const listed = spec.components.schemas.LeadStepStatementsResponse.properties;
    expect(Object.keys(listed)).toContain("funnelSteps");
    expect(Object.keys(listed)).toContain("funnelKey");

    const step = spec.components.schemas.LeadStepState.properties;
    expect(Object.keys(step)).toContain("inFunnel");
    expect(Object.keys(step)).toContain("funnelStepIndex");
  });
});

describe("the rename moved no data", () => {
  it("reads a step's position and membership off the campaign's own funnel", () => {
    const funnelSteps = FUNNEL_STEPS.sales_meetings_from_conversation;
    const states = resolveStepStates({
      allSteps: LEAD_STEP_OUTCOMES,
      funnelSteps,
      outcomes: new Map(),
      nevers: new Map(),
    });
    const byStep = Object.fromEntries(states.map((s) => [s.step, s]));

    expect(byStep.meeting_booked).toMatchObject({ inFunnel: true, funnelStepIndex: 0 });
    expect(byStep.meeting_attended).toMatchObject({ inFunnel: true, funnelStepIndex: 1 });
    expect(byStep.sale).toMatchObject({ inFunnel: true, funnelStepIndex: 2 });
    // Off this campaign's funnel: constrained by nothing, and it says so.
    expect(byStep.signup).toMatchObject({ inFunnel: false, funnelStepIndex: null });
  });
});
