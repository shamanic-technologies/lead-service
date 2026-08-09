import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * The recovery is SQL-shaped, so what is worth pinning here is the operator
 * contract: the flags, the input parsing, that the candidate set is scoped to
 * served rows resolved EMAIL-FIRST, that contact evidence comes from
 * email-gateway (brand scope) and excludes those recipients, and that the delete
 * archives the row verbatim under the reason tag so the change is reversible.
 */

const queries: string[] = [];
let sqlResult: unknown[] = [];

// postgres.js tagged template: capture the composed SQL text. Nested fragments
// are themselves tagged calls, so joining every call's strings is enough to
// assert on the predicates each statement carries.
function fakeSql(strings: TemplateStringsArray): Promise<unknown[]> {
  queries.push(strings.join("?"));
  return Promise.resolve(sqlResult);
}

vi.mock("../../src/db/index.js", () => ({
  sql: (...args: unknown[]) => fakeSql(args[0] as TemplateStringsArray),
}));

const checkDeliveryStatus = vi.fn();

vi.mock("../../src/lib/email-gateway-client.js", () => ({
  checkDeliveryStatus: (...args: unknown[]) => checkDeliveryStatus(...args),
  isContacted: (result: { contacted?: boolean }) => result.contacted === true,
}));

beforeEach(() => {
  queries.length = 0;
  sqlResult = [];
  checkDeliveryStatus.mockReset();
  checkDeliveryStatus.mockResolvedValue({ results: [] });
});

const load = () => import("../../scripts/requeue-uncontacted-serves.js");

describe("requeue-uncontacted-serves", () => {
  it("parses --dry-run and --input", async () => {
    const { parseArgs } = await load();
    expect(parseArgs(["--dry-run", "--input", "/tmp/x.csv"])).toEqual({
      dryRun: true,
      inputPath: "/tmp/x.csv",
    });
    expect(parseArgs(["--input", "/tmp/x.csv"])).toEqual({
      dryRun: false,
      inputPath: "/tmp/x.csv",
    });
    expect(parseArgs([])).toEqual({ dryRun: false, inputPath: null });
  });

  it("parses the CSV, tolerates a header, lowercases emails and dedupes", async () => {
    const { parseInput } = await load();
    expect(
      parseInput("email,campaign_id\nA@x.com,c1\n\na@x.com,c1\nb@y.com,c2\n"),
    ).toEqual([
      { email: "a@x.com", campaignId: "c1" },
      { email: "b@y.com", campaignId: "c2" },
    ]);
  });

  it("rejects a malformed input line instead of silently shrinking the set", async () => {
    const { parseInput } = await load();
    expect(() => parseInput("a@x.com,c1,extra\n")).toThrow(/malformed input line/);
    expect(() => parseInput("not-an-email,c1\n")).toThrow(/no email address/);
  });

  it("scopes candidates to served rows resolved through the registered email", async () => {
    const { loadServedRows } = await load();
    await loadServedRows([{ email: "a@x.com", campaignId: "c1" }]);

    const all = queries.join("\n");
    // email-first identity via the registered contact method, never leads.metadata
    expect(all).toContain("m.channel = 'email' AND lower(m.value) = c.email");
    expect(all).not.toContain("metadata->>'email'");
    // only a consumed serve is in scope
    expect(all).toContain("lc.status = 'served'");
    expect(all).toContain("lc.campaign_id = c.campaign_id");
  });

  it("asks email-gateway at BRAND scope so a contact under any campaign counts", async () => {
    const { fetchContactedEmails } = await load();
    checkDeliveryStatus.mockResolvedValue({
      results: [{ email: "Contacted@x.com", contacted: true }, { email: "clean@x.com" }],
    });

    const contacted = await fetchContactedEmails([
      {
        leadCampaignId: "lc1",
        leadId: "l1",
        campaignId: "c1",
        orgId: "o1",
        brandIds: ["b1"],
        email: "contacted@x.com",
      },
      {
        leadCampaignId: "lc2",
        leadId: "l2",
        campaignId: "c1",
        orgId: "o1",
        brandIds: ["b1"],
        email: "clean@x.com",
      },
    ]);

    expect(checkDeliveryStatus).toHaveBeenCalledTimes(1);
    const [brandId, campaignId, items] = checkDeliveryStatus.mock.calls[0];
    expect(brandId).toBe("b1");
    // undefined campaignId ⟹ brand scope + per-campaign breakdown
    expect(campaignId).toBeUndefined();
    expect(items).toEqual([{ email: "contacted@x.com" }, { email: "clean@x.com" }]);
    expect([...contacted]).toEqual(["contacted@x.com"]);
  });

  it("checks every brand a row is recorded against", async () => {
    const { fetchContactedEmails } = await load();
    await fetchContactedEmails([
      {
        leadCampaignId: "lc1",
        leadId: "l1",
        campaignId: "c1",
        orgId: "o1",
        brandIds: ["b1", "b2"],
        email: "a@x.com",
      },
    ]);
    expect(checkDeliveryStatus.mock.calls.map((c) => c[0]).sort()).toEqual(["b1", "b2"]);
  });

  it("propagates an email-gateway failure rather than assuming not-contacted", async () => {
    const { fetchContactedEmails } = await load();
    checkDeliveryStatus.mockRejectedValue(new Error("gateway down"));
    await expect(
      fetchContactedEmails([
        {
          leadCampaignId: "lc1",
          leadId: "l1",
          campaignId: "c1",
          orgId: "o1",
          brandIds: ["b1"],
          email: "a@x.com",
        },
      ]),
    ).rejects.toThrow(/gateway down/);
  });

  it("excludes contacted recipients from the requeueable set and reports unresolved candidates", async () => {
    const { buildPlan } = await load();
    sqlResult = [
      {
        lead_campaign_id: "lc1",
        lead_id: "l1",
        campaign_id: "c1",
        org_id: "o1",
        brand_ids: ["b1"],
        email: "contacted@x.com",
      },
      {
        lead_campaign_id: "lc2",
        lead_id: "l2",
        campaign_id: "c1",
        org_id: "o1",
        brand_ids: ["b1"],
        email: "clean@x.com",
      },
    ];
    checkDeliveryStatus.mockResolvedValue({
      results: [{ email: "contacted@x.com", contacted: true }, { email: "clean@x.com" }],
    });

    const plan = await buildPlan([
      { email: "contacted@x.com", campaignId: "c1" },
      { email: "clean@x.com", campaignId: "c1" },
      { email: "gone@x.com", campaignId: "c9" },
    ]);

    expect(plan.candidates).toBe(3);
    expect(plan.requeueable.map((r) => r.email)).toEqual(["clean@x.com"]);
    expect(plan.contacted.map((r) => r.email)).toEqual(["contacted@x.com"]);
    expect(plan.unresolved).toEqual([{ email: "gone@x.com", campaignId: "c9" }]);
  });

  it("archives the row verbatim under the reason tag in the same statement as the delete", async () => {
    const { archiveAndDelete, REQUEUE_REASON } = await load();
    await archiveAndDelete([
      {
        leadCampaignId: "11111111-1111-1111-1111-111111111111",
        leadId: "l1",
        campaignId: "c1",
        orgId: "o1",
        brandIds: ["b1"],
        email: "clean@x.com",
      },
    ]);

    const all = queries.join("\n");
    expect(all).toContain("DELETE FROM leads_campaigns");
    // the delete and the archive are one statement — no half-applied state
    expect(all).toContain("INSERT INTO requeued_serves");
    // verbatim snapshot is the undo source
    expect(all).toContain("to_jsonb(r)");
    // never delete a row that is no longer a consumed serve
    expect(all).toContain("lc.id = t.id AND lc.status = 'served'");
    // re-running can never double-archive
    expect(all).toContain("ON CONFLICT (lead_id, campaign_id, reason) DO NOTHING");
    expect(REQUEUE_REASON).toBe("instantly-timezone-send-failure");
  });

  it("archives nothing when there is nothing to requeue", async () => {
    const { archiveAndDelete } = await load();
    expect(await archiveAndDelete([])).toBe(0);
    expect(queries).toHaveLength(0);
  });

  it("reads the ledger back from the database, scoped to this repair's reason", async () => {
    const { readLedger } = await load();
    await readLedger();
    const all = queries.join("\n");
    expect(all).toContain("FROM requeued_serves");
    expect(all).toContain("reason = ");
    expect(all).toContain("unnest(brand_ids)");
  });
});
