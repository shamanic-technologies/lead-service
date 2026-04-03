import { describe, it, expect } from "vitest";
import { getTableConfig } from "drizzle-orm/pg-core";
import { leadBuffer, servedLeads } from "../../src/db/schema.js";

describe("lead_buffer indexes", () => {
  const config = getTableConfig(leadBuffer);
  const indexes = config.indexes.map((i) => ({
    name: (i as any).config.name as string,
    columns: (i as any).config.columns.map((c: any) => c.name) as string[],
  }));
  const indexNames = indexes.map((i) => i.name);

  it("has index covering pullNext query (org_id, campaign_id, namespace, status)", () => {
    expect(indexNames).toContain("idx_buffer_org_campaign_ns_status");
    const idx = indexes.find((i) => i.name === "idx_buffer_org_campaign_ns_status")!;
    expect(idx.columns).toEqual(["org_id", "campaign_id", "namespace", "status"]);
  });

  it("has index covering isInBuffer query (org_id, campaign_id, external_id)", () => {
    expect(indexNames).toContain("idx_buffer_org_campaign_extid");
    const idx = indexes.find((i) => i.name === "idx_buffer_org_campaign_extid")!;
    expect(idx.columns).toEqual(["org_id", "campaign_id", "external_id"]);
  });

  it("does NOT have the old suboptimal index (org_id, namespace, status)", () => {
    expect(indexNames).not.toContain("idx_buffer_org_ns_status");
  });
});

describe("served_leads indexes", () => {
  const config = getTableConfig(servedLeads);
  const indexNames = config.indexes.map((i) => (i as any).config.name as string);

  it("has unique index on (org_id, campaign_id, email)", () => {
    expect(indexNames).toContain("idx_served_org_campaign_email");
  });

  it("has single-column index on org_id", () => {
    expect(indexNames).toContain("idx_served_org_id");
  });

  it("has single-column index on campaign_id", () => {
    expect(indexNames).toContain("idx_served_campaign");
  });
});
