import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { resolveStepStates } from "../../src/lib/step-funnel-state.js";
import { FUNNEL_STEPS } from "../../src/lib/funnel-steps.js";
import { LEAD_STEP_OUTCOMES } from "../../src/lib/step-statements.js";

/**
 * A sales funnel is a SALES FUNNEL, on the wire and inside, and nowhere is it a "chain".
 *
 * The word was this fleet's second name for the same thing. features-service, api-service and all
 * three dashboard apps dropped it first; this service was the last producer still SERVING it,
 * which is the only reason the two consumer apps ever read it. v0.62.0 renamed the payload
 * (`chain` -> `funnelSteps`, `inChain` -> `inFunnel`, `chainIndex` -> `stepIndex`) without moving
 * any data — same steps, same order, same semantics.
 *
 * A sweep is only as durable as the guard behind it, and this is the guard. It fails the build on
 * the word returning to a property name, to a published description, or to any source file — which
 * is what makes "no alias, and it does not come back" a property of the repo rather than a promise
 * in a commit message.
 *
 * The carve-out is the word used for something that genuinely is NOT a sales funnel: a CALL chain,
 * a CAUSE chain, the SEND chain a lead is handed to. Those are allowed by exact phrase, never by
 * proximity, so "sales chain" can never slip through on the back of one.
 */
const BANNED = ["ch", "ain"].join("");
const ALLOWED_PHRASES = ["call chain", "cause chain", "send chain", "promise chain"];
const REPO = fileURLToPath(new URL("../..", import.meta.url));

/** This file names what it forbids, so it is the one file exempt from its own scan. */
const SELF = "tests/unit/funnel-vocabulary.test.ts";

function stripAllowed(text: string): string {
  let out = text.toLowerCase();
  for (const phrase of ALLOWED_PHRASES) out = out.split(phrase).join("");
  return out;
}

function walk(dir: string, out: string[] = []): string[] {
  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return out;
  }
  for (const entry of entries) {
    if (entry === "node_modules" || entry === "dist" || entry === "meta") continue;
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) walk(full, out);
    else if (/\.(ts|tsx|js|mjs|sql|json)$/.test(entry)) out.push(full);
  }
  return out;
}

function walkSpec(
  node: unknown,
  path: string,
  visit: (path: string, key: string, value: unknown) => void,
) {
  if (Array.isArray(node)) {
    node.forEach((child, i) => walkSpec(child, `${path}[${i}]`, visit));
    return;
  }
  if (node && typeof node === "object") {
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      visit(path, key, value);
      walkSpec(value, `${path}.${key}`, visit);
    }
  }
}

describe("the published contract never calls a sales funnel a chain", () => {
  const spec = JSON.parse(readFileSync(join(REPO, "openapi.json"), "utf8"));

  it("names no property with the retired word", () => {
    const offenders: string[] = [];
    walkSpec(spec, "$", (path, key) => {
      if (path.endsWith(".properties") && key.toLowerCase().includes(BANNED)) {
        offenders.push(`${path}.${key}`);
      }
    });
    expect(offenders).toEqual([]);
  });

  it("uses the word in no description that is not about one of the allowed things", () => {
    const offenders: string[] = [];
    walkSpec(spec, "$", (path, key, value) => {
      if (key !== "description" && key !== "summary") return;
      if (typeof value !== "string") return;
      if (stripAllowed(value).includes(BANNED)) offenders.push(`${path}.${key}`);
    });
    expect(offenders).toEqual([]);
  });

  it("serves the funnel's steps as funnelSteps, and a step's membership as inFunnel", () => {
    const listed = spec.components.schemas.LeadStepStatementsResponse.properties;
    expect(Object.keys(listed)).toContain("funnelSteps");
    expect(Object.keys(listed)).toContain("funnelKey");

    const step = spec.components.schemas.LeadStepState.properties;
    expect(Object.keys(step)).toContain("inFunnel");
    expect(Object.keys(step)).toContain("stepIndex");
  });
});

describe("no source file calls a sales funnel a chain either", () => {
  it("carries the word only where the thing is genuinely not a sales funnel", () => {
    const offenders: string[] = [];
    for (const dir of ["src", "tests", "scripts", "drizzle"]) {
      for (const file of walk(join(REPO, dir))) {
        const rel = file.slice(REPO.length);
        if (rel === SELF) continue;
        if (stripAllowed(readFileSync(file, "utf8")).includes(BANNED)) offenders.push(rel);
      }
    }
    expect(offenders).toEqual([]);
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

    expect(byStep.meeting_booked).toMatchObject({ inFunnel: true, stepIndex: 0 });
    expect(byStep.meeting_attended).toMatchObject({ inFunnel: true, stepIndex: 1 });
    expect(byStep.sale).toMatchObject({ inFunnel: true, stepIndex: 2 });
    // Off this campaign's funnel: constrained by nothing, and it says so.
    expect(byStep.signup).toMatchObject({ inFunnel: false, stepIndex: null });
  });
});
