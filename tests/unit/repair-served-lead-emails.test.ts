import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * The repair is SQL-shaped, so what is worth pinning here is the contract the
 * operator relies on: the dry-run flag, and that each statement carries the
 * predicates that make it safe — orphan scope, conflict skip, and the
 * traceability write.
 */

const queries: string[] = [];

// postgres.js tagged template: capture the composed SQL text. Nested fragments
// are themselves tagged calls, so joining every call's strings is enough to
// assert on the predicates each statement carries.
function fakeSql(strings: TemplateStringsArray): Promise<unknown[]> {
  queries.push(strings.join("?"));
  // Statements are awaited (empty result set); nested fragments are only
  // interpolated, so the returned promise is never awaited for those.
  return Promise.resolve([]);
}

vi.mock("../../src/db/index.js", () => ({
  sql: (...args: unknown[]) => fakeSql(args[0] as TemplateStringsArray),
}));

beforeEach(() => {
  queries.length = 0;
});

describe("repair-served-lead-emails", () => {
  it("parses --dry-run", async () => {
    const { parseArgs } = await import("../../scripts/repair-served-lead-emails.js");
    expect(parseArgs(["--dry-run"])).toEqual({ dryRun: true });
    expect(parseArgs([])).toEqual({ dryRun: false });
  });

  it("scopes the orphan set to served rows whose lead has no email contact method", async () => {
    const { buildPlan } = await import("../../scripts/repair-served-lead-emails.js");
    await buildPlan();

    const all = queries.join("\n");
    expect(all).toContain("lc.status = 'served'");
    expect(all).toContain("NOT EXISTS");
    expect(all).toContain("m.channel = 'email'");
    // email comes from the stored provider payload, never from a read-path fallback
    expect(all).toContain("l.metadata->>'email'");
  });

  it("re-points to the email owner, records the previous lead_id, and skips (lead, campaign) conflicts", async () => {
    const { repointToEmailOwners } = await import("../../scripts/repair-served-lead-emails.js");
    await repointToEmailOwners();

    const all = queries.join("\n");
    expect(all).toContain("SET lead_id = r.owner_lead_id");
    // traceability: what the row was attributed to before
    expect(all).toContain("repointed_from_lead_id = lc.lead_id");
    // never force-merge into an existing membership row for the same campaign
    expect(all).toContain("x.lead_id = r.owner_lead_id AND x.campaign_id = r.campaign_id");
  });

  it("attaches a free email to the row's own lead, tagged for reversibility", async () => {
    const { attachFreeEmails, REPAIR_SOURCE } = await import(
      "../../scripts/repair-served-lead-emails.js"
    );
    await attachFreeEmails();

    const all = queries.join("\n");
    expect(all).toContain("INSERT INTO lead_contact_methods");
    expect(all).toContain("owner_lead_id IS NULL");
    expect(all).toContain("ON CONFLICT DO NOTHING");
    expect(REPAIR_SOURCE).toBe("served-lead-email-repair");
  });
});
