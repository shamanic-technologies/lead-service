import { beforeEach, describe, expect, it, vi } from "vitest";
import express from "express";
import request from "supertest";

// `GET /orgs/stats?campaignId=` narrows by a single campaign for exactly the same reason
// `/orgs/leads` did, so it totals the campaign IDENTITY too — otherwise the two surfaces disagree
// about the same campaign.

let statusRows: Array<Record<string, unknown>> = [];
let whereConditions: unknown[] = [];

const selectMock = vi.fn(() => ({
  from: () => ({
    where: (cond: unknown) => {
      whereConditions.push(cond);
      return { groupBy: () => Promise.resolve(statusRows) };
    },
  }),
}));

vi.mock("../../src/db/index.js", () => ({
  db: {
    select: () => selectMock(),
    execute: () => Promise.resolve([]),
  },
}));

const fetchEmailGatewayStatsMock = vi.fn();
vi.mock("../../src/lib/email-gateway-client.js", () => ({
  fetchEmailGatewayStats: (...args: unknown[]) => fetchEmailGatewayStatsMock(...args),
}));

const resolveCampaignFamilyMock = vi.fn();
vi.mock("../../src/lib/campaign-identity-client.js", () => ({
  resolveCampaignFamily: (...args: unknown[]) => resolveCampaignFamilyMock(...args),
}));

vi.mock("../../src/lib/dynasty-client.js", () => ({
  resolveFeatureDynastySlugs: vi.fn(),
  resolveWorkflowDynastySlugs: vi.fn(),
  fetchFeatureDynastyMap: vi.fn(),
  fetchWorkflowDynastyMap: vi.fn(),
}));

vi.mock("../../src/config.js", () => ({
  LEAD_SERVICE_API_KEY: "test-api-key",
}));

const ORG = "org-1";
const BRAND = "brand-1";
const LIVE = "campaign-live";
const ANCESTOR = "campaign-ancestor";

function recipientStats(over: Partial<Record<string, number>> = {}) {
  return {
    contacted: over.contacted ?? 0,
    sent: over.sent ?? 0,
    delivered: over.delivered ?? 0,
    opened: over.opened ?? 0,
    bounced: over.bounced ?? 0,
    clicked: over.clicked ?? 0,
    unsubscribed: over.unsubscribed ?? 0,
    repliesPositive: over.repliesPositive ?? 0,
    repliesNegative: 0,
    repliesNeutral: 0,
    repliesAutoReply: 0,
    repliesDetail: {
      interested: over.interested ?? 0,
      meetingBooked: 0, closed: 0, notInterested: 0, wrongPerson: 0,
      unsubscribe: 0, neutral: 0, autoReply: 0, outOfOffice: 0,
    },
  };
}

async function buildApp() {
  const { default: route } = await import("../../src/routes/stats.js");
  const app = express();
  app.use(route);
  return app;
}

function get(app: express.Express, query: Record<string, string>) {
  return request(app).get("/orgs/stats").query(query).set("x-api-key", "test-api-key").set("x-org-id", ORG);
}

describe("GET /orgs/stats campaign identity scope", () => {
  beforeEach(() => {
    statusRows = [];
    whereConditions = [];
    selectMock.mockClear();
    fetchEmailGatewayStatsMock.mockReset();
    resolveCampaignFamilyMock.mockReset();
  });

  it("sums the identity's outreach evidence across every member campaign", async () => {
    resolveCampaignFamilyMock.mockResolvedValue([ANCESTOR, LIVE]);
    statusRows = [{ status: "served", count: 7895 }];
    fetchEmailGatewayStatsMock.mockImplementation((params: { campaignId?: string }) =>
      Promise.resolve(
        params.campaignId === ANCESTOR
          ? { broadcast: { recipientStats: recipientStats({ contacted: 2814, sent: 2814, clicked: 10 }) } }
          : { broadcast: { recipientStats: recipientStats({ contacted: 5081, sent: 5081, clicked: 4 }) } },
      ),
    );

    const app = await buildApp();
    const res = await get(app, { brandId: BRAND, campaignId: LIVE });

    expect(res.status).toBe(200);
    expect(res.body.totalLeads).toBe(7895);
    expect(res.body.byOutreachStatus.contacted).toBe(7895);
    expect(res.body.byOutreachStatus.clicked).toBe(14);
    expect(fetchEmailGatewayStatsMock).toHaveBeenCalledTimes(2);
  });

  it("a single-member identity makes exactly one call, with the campaign id, as today", async () => {
    resolveCampaignFamilyMock.mockResolvedValue([LIVE]);
    statusRows = [{ status: "served", count: 12 }];
    fetchEmailGatewayStatsMock.mockResolvedValue({
      broadcast: { recipientStats: recipientStats({ contacted: 12, sent: 12 }) },
    });

    const app = await buildApp();
    const res = await get(app, { brandId: BRAND, campaignId: LIVE });

    expect(res.status).toBe(200);
    expect(fetchEmailGatewayStatsMock).toHaveBeenCalledTimes(1);
    expect(fetchEmailGatewayStatsMock.mock.calls[0][0]).toEqual(
      expect.objectContaining({ brandId: BRAND, campaignId: LIVE }),
    );
    expect(res.body.byOutreachStatus.contacted).toBe(12);
  });

  it("groupBy=campaignId over an identity stays ONE line, keyed on the campaign the caller named", async () => {
    resolveCampaignFamilyMock.mockResolvedValue([ANCESTOR, LIVE]);
    statusRows = [
      { key: ANCESTOR, status: "served", count: 2814 },
      { key: LIVE, status: "served", count: 5081 },
    ];
    fetchEmailGatewayStatsMock.mockImplementation((params: { campaignId?: string }) =>
      Promise.resolve({
        groups: [
          {
            key: params.campaignId!,
            broadcast: { recipientStats: recipientStats({ contacted: params.campaignId === LIVE ? 5081 : 2814 }) },
          },
        ],
      }),
    );

    const app = await buildApp();
    const res = await get(app, { brandId: BRAND, campaignId: LIVE, groupBy: "campaignId" });

    expect(res.status).toBe(200);
    expect(res.body.groups).toHaveLength(1);
    expect(res.body.groups[0].key).toBe(LIVE);
    expect(res.body.groups[0].totalLeads).toBe(7895);
    expect(res.body.groups[0].byOutreachStatus.contacted).toBe(7895);
  });

  it("a brand-scoped read never resolves an identity and keeps its single brand-mode call", async () => {
    statusRows = [{ status: "served", count: 3 }];
    fetchEmailGatewayStatsMock.mockResolvedValue({ broadcast: { recipientStats: recipientStats({ contacted: 3 }) } });

    const app = await buildApp();
    const res = await get(app, { brandId: BRAND });

    expect(res.status).toBe(200);
    expect(resolveCampaignFamilyMock).not.toHaveBeenCalled();
    expect(fetchEmailGatewayStatsMock).toHaveBeenCalledTimes(1);
    expect(fetchEmailGatewayStatsMock.mock.calls[0][0].campaignId).toBeUndefined();
  });

  it("resolution failing keeps today's single-campaign answer", async () => {
    resolveCampaignFamilyMock.mockResolvedValue([LIVE]);
    statusRows = [{ status: "served", count: 5081 }];
    fetchEmailGatewayStatsMock.mockResolvedValue({
      broadcast: { recipientStats: recipientStats({ contacted: 5081 }) },
    });

    const app = await buildApp();
    const res = await get(app, { brandId: BRAND, campaignId: LIVE });

    expect(res.body.totalLeads).toBe(5081);
    expect(fetchEmailGatewayStatsMock).toHaveBeenCalledTimes(1);
    expect(fetchEmailGatewayStatsMock.mock.calls[0][0].campaignId).toBe(LIVE);
  });
});
