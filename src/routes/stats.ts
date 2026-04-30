import { Router } from "express";
import { eq, and, count, inArray, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, type ServiceContext, apiKeyAuth, requireOrgId, getServiceContext } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import {
  fetchEmailGatewayStats,
  type RecipientStats,
  type EmailGatewayStatsResponse,
  type EmailGatewayGroupedStatsResponse,
} from "../lib/email-gateway-client.js";
import {
  resolveFeatureDynastySlugs,
  resolveWorkflowDynastySlugs,
  fetchFeatureDynastyMap,
  fetchWorkflowDynastyMap,
} from "../lib/dynasty-client.js";

const VALID_GROUP_BY = [
  "campaignId",
  "brandId",
  "workflowSlug",
  "featureSlug",
  "workflowDynastySlug",
  "featureDynastySlug",
] as const;
type GroupByField = (typeof VALID_GROUP_BY)[number];

const COLUMN_MAP = {
  campaignId: { served: servedLeads.campaignId, buffer: leadBuffer.campaignId },
  workflowSlug: { served: servedLeads.workflowSlug, buffer: leadBuffer.workflowSlug },
  featureSlug: { served: servedLeads.featureSlug, buffer: leadBuffer.featureSlug },
} as const;

/** Map groupBy param to email-gateway groupBy value */
const EG_GROUP_BY_MAP: Record<string, string> = {
  campaignId: "campaignId",
  brandId: "brandId",
  workflowSlug: "workflowSlug",
  featureSlug: "featureSlug",
};

const router = Router();

/**
 * Resolve dynasty slug query params into lists of versioned slugs.
 * Dynasty slugs take priority over exact slugs.
 */
async function resolveDynastySlugs(
  req: AuthenticatedRequest,
): Promise<{
  workflowSlugs: string[] | null;
  featureSlugs: string[] | null;
  emptyDynasty: boolean;
}> {
  const str = (v: unknown): string | undefined =>
    typeof v === "string" ? v : undefined;

  const workflowDynastySlug = str(req.query.workflowDynastySlug);
  const featureDynastySlug = str(req.query.featureDynastySlug);
  const workflowSlug = str(req.query.workflowSlug);
  const workflowSlugsParam = str(req.query.workflowSlugs);
  const featureSlug = str(req.query.featureSlug);
  const featureSlugsParam = str(req.query.featureSlugs);

  const context = { orgId: req.orgId, userId: req.userId, runId: req.runId };

  let workflowSlugs: string[] | null = null;
  let featureSlugs: string[] | null = null;

  if (workflowDynastySlug) {
    workflowSlugs = await resolveWorkflowDynastySlugs(workflowDynastySlug, context);
    if (workflowSlugs.length === 0) return { workflowSlugs: [], featureSlugs: null, emptyDynasty: true };
  } else if (workflowSlugsParam) {
    workflowSlugs = workflowSlugsParam.split(",").filter(Boolean);
  } else if (workflowSlug) {
    workflowSlugs = [workflowSlug];
  }

  if (featureDynastySlug) {
    featureSlugs = await resolveFeatureDynastySlugs(featureDynastySlug, context);
    if (featureSlugs.length === 0) return { workflowSlugs, featureSlugs: [], emptyDynasty: true };
  } else if (featureSlugsParam) {
    featureSlugs = featureSlugsParam.split(",").filter(Boolean);
  } else if (featureSlug) {
    featureSlugs = [featureSlug];
  }

  return { workflowSlugs, featureSlugs, emptyDynasty: false };
}

function buildConditions(
  req: AuthenticatedRequest,
  table: "served" | "buffer",
  dynastyResolved: { workflowSlugs: string[] | null; featureSlugs: string[] | null },
) {
  const { brandId, campaignId, orgId, userId, runIds } = req.query;
  const str = (v: unknown): string | undefined =>
    typeof v === "string" ? v : undefined;
  const brandIdStr = str(brandId);
  const campaignIdStr = str(campaignId);
  const orgIdStr = str(orgId);
  const userIdStr = str(userId);
  const runIdList =
    typeof runIds === "string" ? runIds.split(",").filter(Boolean) : [];

  if (table === "served") {
    const conds: SQL[] = [eq(servedLeads.orgId, req.orgId!)];
    if (brandIdStr) conds.push(sql`${brandIdStr} = ANY(${servedLeads.brandIds})`);
    if (campaignIdStr) conds.push(eq(servedLeads.campaignId, campaignIdStr));
    if (orgIdStr) conds.push(eq(servedLeads.orgId, orgIdStr));
    if (userIdStr) conds.push(eq(servedLeads.userId, userIdStr));
    if (runIdList.length > 0) {
      conds.push(
        sql`(${servedLeads.parentRunId} = ANY(ARRAY[${sql.join(runIdList.map(id => sql`${id}`), sql`, `)}]) OR ${servedLeads.runId} = ANY(ARRAY[${sql.join(runIdList.map(id => sql`${id}`), sql`, `)}]))`,
      );
    }
    if (dynastyResolved.workflowSlugs && dynastyResolved.workflowSlugs.length > 0) {
      conds.push(inArray(servedLeads.workflowSlug, dynastyResolved.workflowSlugs));
    }
    if (dynastyResolved.featureSlugs && dynastyResolved.featureSlugs.length > 0) {
      conds.push(inArray(servedLeads.featureSlug, dynastyResolved.featureSlugs));
    }
    return { conds, runIdList, brandIdStr, campaignIdStr, orgIdStr };
  }

  const conds: SQL[] = [eq(leadBuffer.orgId, req.orgId!)];
  if (brandIdStr) conds.push(sql`${brandIdStr} = ANY(${leadBuffer.brandIds})`);
  if (campaignIdStr) conds.push(eq(leadBuffer.campaignId, campaignIdStr));
  if (orgIdStr) conds.push(eq(leadBuffer.orgId, orgIdStr));
  if (userIdStr) conds.push(eq(leadBuffer.userId, userIdStr));
  if (runIdList.length > 0) {
    conds.push(inArray(leadBuffer.pushRunId, runIdList));
  }
  if (dynastyResolved.workflowSlugs && dynastyResolved.workflowSlugs.length > 0) {
    conds.push(inArray(leadBuffer.workflowSlug, dynastyResolved.workflowSlugs));
  }
  if (dynastyResolved.featureSlugs && dynastyResolved.featureSlugs.length > 0) {
    conds.push(inArray(leadBuffer.featureSlug, dynastyResolved.featureSlugs));
  }
  return { conds, runIdList, brandIdStr, campaignIdStr, orgIdStr };
}

const ZERO_RECIPIENT_STATS: RecipientStats = {
  contacted: 0, sent: 0, delivered: 0, opened: 0, bounced: 0, clicked: 0,
  unsubscribed: 0, repliesPositive: 0, repliesNegative: 0, repliesNeutral: 0,
  repliesAutoReply: 0,
  repliesDetail: {
    interested: 0, meetingBooked: 0, closed: 0, notInterested: 0,
    wrongPerson: 0, unsubscribe: 0, neutral: 0, autoReply: 0, outOfOffice: 0,
  },
};

type GroupStats = { totalLeads: number; byOutreachStatus: RecipientStats; repliesDetail: RecipientStats["repliesDetail"]; buffered: number; skipped: number; claimed: number };

function newGroupStats(): GroupStats {
  return {
    totalLeads: 0,
    byOutreachStatus: { ...ZERO_RECIPIENT_STATS, repliesDetail: { ...ZERO_RECIPIENT_STATS.repliesDetail } },
    repliesDetail: { ...ZERO_RECIPIENT_STATS.repliesDetail },
    buffered: 0,
    skipped: 0,
    claimed: 0,
  };
}

function mergeRecipientStats(broadcast?: { recipientStats: RecipientStats }, transactional?: { recipientStats: RecipientStats }): { byOutreachStatus: RecipientStats; repliesDetail: RecipientStats["repliesDetail"] } {
  const bc = broadcast?.recipientStats ?? ZERO_RECIPIENT_STATS;
  const tx = transactional?.recipientStats ?? ZERO_RECIPIENT_STATS;

  const byOutreachStatus: RecipientStats = {
    contacted: bc.contacted + tx.contacted,
    sent: bc.sent + tx.sent,
    delivered: bc.delivered + tx.delivered,
    opened: bc.opened + tx.opened,
    bounced: bc.bounced + tx.bounced,
    clicked: bc.clicked + tx.clicked,
    unsubscribed: bc.unsubscribed + tx.unsubscribed,
    repliesPositive: bc.repliesPositive + tx.repliesPositive,
    repliesNegative: bc.repliesNegative + tx.repliesNegative,
    repliesNeutral: bc.repliesNeutral + tx.repliesNeutral,
    repliesAutoReply: bc.repliesAutoReply + tx.repliesAutoReply,
    repliesDetail: {
      interested: (bc.repliesDetail?.interested ?? 0) + (tx.repliesDetail?.interested ?? 0),
      meetingBooked: (bc.repliesDetail?.meetingBooked ?? 0) + (tx.repliesDetail?.meetingBooked ?? 0),
      closed: (bc.repliesDetail?.closed ?? 0) + (tx.repliesDetail?.closed ?? 0),
      notInterested: (bc.repliesDetail?.notInterested ?? 0) + (tx.repliesDetail?.notInterested ?? 0),
      wrongPerson: (bc.repliesDetail?.wrongPerson ?? 0) + (tx.repliesDetail?.wrongPerson ?? 0),
      unsubscribe: (bc.repliesDetail?.unsubscribe ?? 0) + (tx.repliesDetail?.unsubscribe ?? 0),
      neutral: (bc.repliesDetail?.neutral ?? 0) + (tx.repliesDetail?.neutral ?? 0),
      autoReply: (bc.repliesDetail?.autoReply ?? 0) + (tx.repliesDetail?.autoReply ?? 0),
      outOfOffice: (bc.repliesDetail?.outOfOffice ?? 0) + (tx.repliesDetail?.outOfOffice ?? 0),
    },
  };

  return { byOutreachStatus, repliesDetail: byOutreachStatus.repliesDetail };
}

function applyBufferRows(groups: Map<string, GroupStats>, bufferRows: { key: string; status: string; count: number }[], keyFn: (key: string) => string = (k) => k) {
  for (const row of bufferRows) {
    const g = groups.get(keyFn(row.key)) ?? newGroupStats();
    if (!groups.has(keyFn(row.key))) groups.set(keyFn(row.key), g);
    if (row.status === "buffered") g.buffered += row.count;
    if (row.status === "skipped") g.skipped += row.count;
    if (row.status === "claimed") g.claimed += row.count;
  }
}

const ZERO_STATS = { groups: [] };

router.get("/orgs/stats", apiKeyAuth, requireOrgId, async (req: AuthenticatedRequest, res) => {
  try {
    const groupByParam =
      typeof req.query.groupBy === "string" ? req.query.groupBy : undefined;

    if (groupByParam && !VALID_GROUP_BY.includes(groupByParam as GroupByField)) {
      res.status(400).json({
        error: `Invalid groupBy value. Allowed: ${VALID_GROUP_BY.join(", ")}`,
      });
      return;
    }

    // Resolve dynasty slugs (if provided) before building conditions
    const dynastyResolved = await resolveDynastySlugs(req);
    if (dynastyResolved.emptyDynasty) {
      if (groupByParam) {
        res.json(ZERO_STATS);
      } else {
        res.json({
          totalLeads: 0,
          byOutreachStatus: ZERO_RECIPIENT_STATS,
          repliesDetail: ZERO_RECIPIENT_STATS.repliesDetail,
          buffered: 0,
          skipped: 0,
          claimed: 0,
        });
      }
      return;
    }

    const served = buildConditions(req, "served", dynastyResolved);
    const buffer = buildConditions(req, "buffer", dynastyResolved);
    const egContext = getServiceContext(req);

    // Build email-gateway stats params
    const egParams: Parameters<typeof fetchEmailGatewayStats>[0] = {};
    if (served.brandIdStr) egParams.brandId = served.brandIdStr;
    if (served.campaignIdStr) egParams.campaignId = served.campaignIdStr;
    if (dynastyResolved.workflowSlugs) egParams.workflowSlugs = dynastyResolved.workflowSlugs.join(",");
    if (dynastyResolved.featureSlugs) egParams.featureSlugs = dynastyResolved.featureSlugs.join(",");

    // --- Dynasty groupBy (requires reverse map) ---
    if (groupByParam === "workflowDynastySlug" || groupByParam === "featureDynastySlug") {
      const isWorkflow = groupByParam === "workflowDynastySlug";
      const dbField = isWorkflow ? "workflowSlug" : "featureSlug";
      const servedCol = COLUMN_MAP[dbField].served;
      const bufferCol = COLUMN_MAP[dbField].buffer;
      const context = { orgId: req.orgId, userId: req.userId, runId: req.runId };

      egParams.groupBy = dbField;

      const [dynastyMap, servedRows, egStats, bufferRows] = await Promise.all([
        isWorkflow ? fetchWorkflowDynastyMap(context) : fetchFeatureDynastyMap(context),
        db
          .select({ key: servedCol, count: count() })
          .from(servedLeads)
          .where(and(...served.conds))
          .groupBy(servedCol),
        fetchEmailGatewayStats(egParams, egContext),
        db
          .select({ key: bufferCol, status: leadBuffer.status, count: count() })
          .from(leadBuffer)
          .where(and(...buffer.conds))
          .groupBy(bufferCol, leadBuffer.status),
      ]);

      const groups = new Map<string, GroupStats>();

      const toDynasty = (slug: string | null): string =>
        dynastyMap.get(slug ?? "") ?? slug ?? "unknown";

      const getGroup = (dynastyKey: string) => {
        if (!groups.has(dynastyKey)) groups.set(dynastyKey, newGroupStats());
        return groups.get(dynastyKey)!;
      };

      for (const row of servedRows) {
        getGroup(toDynasty(row.key)).totalLeads += row.count;
      }

      if ("groups" in egStats) {
        for (const g of (egStats as EmailGatewayGroupedStatsResponse).groups) {
          const dynastyKey = toDynasty(g.key);
          const group = getGroup(dynastyKey);
          const merged = mergeRecipientStats(g.broadcast, g.transactional);
          for (const k of Object.keys(merged.byOutreachStatus) as (keyof RecipientStats)[]) {
            if (k === "repliesDetail") continue;
            (group.byOutreachStatus[k] as number) += merged.byOutreachStatus[k] as number;
          }
          for (const k of Object.keys(merged.repliesDetail) as (keyof RecipientStats["repliesDetail"])[]) {
            (group.repliesDetail[k] as number) += merged.repliesDetail[k] as number;
            (group.byOutreachStatus.repliesDetail[k] as number) += merged.repliesDetail[k] as number;
          }
        }
      }

      applyBufferRows(groups, bufferRows as any[], (k) => toDynasty(k));

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({ key, ...stats })),
      });
      return;
    }

    // --- brandId groupBy (unnest brand_ids) ---
    if (groupByParam === "brandId") {
      egParams.groupBy = "brandId";

      const [servedRows, egStats, bufferRows] = await Promise.all([
        db.execute(sql`
          SELECT unnest(brand_ids) AS key, COUNT(*)::int AS count
          FROM served_leads
          WHERE ${and(...served.conds)}
          GROUP BY key
        `) as Promise<{ key: string; count: number }[]>,
        fetchEmailGatewayStats(egParams, egContext),
        db.execute(sql`
          SELECT unnest(brand_ids) AS key, status, COUNT(*)::int AS count
          FROM lead_buffer
          WHERE ${and(...buffer.conds)}
          GROUP BY key, status
        `) as Promise<{ key: string; status: string; count: number }[]>,
      ]);

      const groups = new Map<string, GroupStats>();

      const getGroup = (key: string | null) => {
        const k = key ?? "unknown";
        if (!groups.has(k)) groups.set(k, newGroupStats());
        return groups.get(k)!;
      };

      for (const row of servedRows) {
        getGroup(row.key).totalLeads = row.count;
      }

      if ("groups" in egStats) {
        for (const g of (egStats as EmailGatewayGroupedStatsResponse).groups) {
          const group = getGroup(g.key);
          const merged = mergeRecipientStats(g.broadcast, g.transactional);
          group.byOutreachStatus = merged.byOutreachStatus;
          group.repliesDetail = merged.repliesDetail;
        }
      }

      applyBufferRows(groups, bufferRows as any[]);

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({ key, ...stats })),
      });
      return;
    }

    // --- Standard groupBy (exact slug columns) ---
    if (groupByParam) {
      const field = groupByParam as keyof typeof COLUMN_MAP;
      const servedCol = COLUMN_MAP[field].served;
      const bufferCol = COLUMN_MAP[field].buffer;

      egParams.groupBy = EG_GROUP_BY_MAP[field] ?? field;

      const [servedRows, egStats, bufferRows] = await Promise.all([
        db
          .select({ key: servedCol, count: count() })
          .from(servedLeads)
          .where(and(...served.conds))
          .groupBy(servedCol),
        fetchEmailGatewayStats(egParams, egContext),
        db
          .select({ key: bufferCol, status: leadBuffer.status, count: count() })
          .from(leadBuffer)
          .where(and(...buffer.conds))
          .groupBy(bufferCol, leadBuffer.status),
      ]);

      const groups = new Map<string, GroupStats>();

      const getGroup = (key: string | null) => {
        const k = key ?? "unknown";
        if (!groups.has(k)) groups.set(k, newGroupStats());
        return groups.get(k)!;
      };

      for (const row of servedRows) {
        getGroup(row.key).totalLeads = row.count;
      }

      if ("groups" in egStats) {
        for (const g of (egStats as EmailGatewayGroupedStatsResponse).groups) {
          const group = getGroup(g.key);
          const merged = mergeRecipientStats(g.broadcast, g.transactional);
          group.byOutreachStatus = merged.byOutreachStatus;
          group.repliesDetail = merged.repliesDetail;
        }
      }

      applyBufferRows(groups, bufferRows as any[]);

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({ key, ...stats })),
      });
      return;
    }

    // --- Flat response ---
    const [servedResult, egStats, bufferRows] = await Promise.all([
      db
        .select({ count: count() })
        .from(servedLeads)
        .where(and(...served.conds))
        .then(([r]) => r),
      fetchEmailGatewayStats(egParams, egContext),
      db
        .select({ status: leadBuffer.status, count: count() })
        .from(leadBuffer)
        .where(and(...buffer.conds))
        .groupBy(leadBuffer.status),
    ]);

    const bufferByStatus = Object.fromEntries(
      bufferRows.map((r) => [r.status, r.count]),
    );

    const egFlat = egStats as EmailGatewayStatsResponse;
    const merged = mergeRecipientStats(egFlat.broadcast, egFlat.transactional);

    res.json({
      totalLeads: servedResult?.count ?? 0,
      byOutreachStatus: merged.byOutreachStatus,
      repliesDetail: merged.repliesDetail,
      buffered: bufferByStatus["buffered"] ?? 0,
      skipped: bufferByStatus["skipped"] ?? 0,
      claimed: bufferByStatus["claimed"] ?? 0,
    });
  } catch (error) {
    console.error("[lead-service] Stats error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
