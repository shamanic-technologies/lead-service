import { Router } from "express";
import { eq, and, count, inArray, or, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, type ServiceContext, authenticate, getServiceContext } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";
import {
  checkDeliveryStatus,
  isContacted,
  type DeliveryStatusItem,
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
        or(
          inArray(servedLeads.parentRunId, runIdList),
          inArray(servedLeads.runId, runIdList),
        )!,
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

/**
 * Count distinct contacted leads by querying email-gateway for delivery status.
 * Groups served leads by first brandId + campaignId, calls email-gateway per group,
 * and returns the count of unique leadIds confirmed contacted.
 */
async function countContacted(
  servedConds: SQL[],
  context: ServiceContext,
): Promise<number> {
  const rows = await db
    .select({
      leadId: servedLeads.leadId,
      email: servedLeads.email,
      brandIds: servedLeads.brandIds,
      campaignId: servedLeads.campaignId,
    })
    .from(servedLeads)
    .where(and(...servedConds));

  if (rows.length === 0) return 0;

  // Group by first brandId + campaignId since email-gateway scopes status per brand/campaign
  const groups = new Map<string, { brandId: string; campaignId: string; items: DeliveryStatusItem[] }>();
  for (const row of rows) {
    if (!row.leadId) continue;
    const primaryBrandId = row.brandIds[0] ?? "unknown";
    const key = `${primaryBrandId}::${row.campaignId}`;
    if (!groups.has(key)) {
      groups.set(key, { brandId: primaryBrandId, campaignId: row.campaignId, items: [] });
    }
    groups.get(key)!.items.push({ leadId: row.leadId, email: row.email });
  }

  const contactedLeadIds = new Set<string>();

  await Promise.all(
    Array.from(groups.values()).map(async (group) => {
      const response = await checkDeliveryStatus(
        group.brandId,
        group.campaignId,
        group.items,
        context,
      );
      if (!response) return;
      for (const result of response.results) {
        if (isContacted(result)) {
          // Find the leadId for this email
          const item = group.items.find((i) => i.email === result.email);
          if (item?.leadId) contactedLeadIds.add(item.leadId);
        }
      }
    }),
  );

  return contactedLeadIds.size;
}

/**
 * Count contacted leads per groupBy dimension.
 * For brandId groupBy, unnests brand_ids to get per-brand counts.
 */
async function countContactedGrouped(
  servedConds: SQL[],
  groupByField: "campaignId" | "workflowSlug" | "featureSlug",
  context: ServiceContext,
): Promise<Map<string, number>> {
  const groupCol = COLUMN_MAP[groupByField].served;

  const rows = await db
    .select({
      leadId: servedLeads.leadId,
      email: servedLeads.email,
      brandIds: servedLeads.brandIds,
      campaignId: servedLeads.campaignId,
      groupKey: groupCol,
    })
    .from(servedLeads)
    .where(and(...servedConds));

  if (rows.length === 0) return new Map();

  // Group by first brandId + campaignId for email-gateway calls
  const callGroups = new Map<string, { brandId: string; campaignId: string; items: (DeliveryStatusItem & { groupKey: string })[] }>();
  for (const row of rows) {
    if (!row.leadId) continue;
    const primaryBrandId = row.brandIds[0] ?? "unknown";
    const key = `${primaryBrandId}::${row.campaignId}`;
    if (!callGroups.has(key)) {
      callGroups.set(key, { brandId: primaryBrandId, campaignId: row.campaignId, items: [] });
    }
    callGroups.get(key)!.items.push({
      leadId: row.leadId,
      email: row.email,
      groupKey: row.groupKey ?? "unknown",
    });
  }

  // Track contacted leadIds per groupBy key
  const contactedPerGroup = new Map<string, Set<string>>();

  await Promise.all(
    Array.from(callGroups.values()).map(async (group) => {
      const response = await checkDeliveryStatus(
        group.brandId,
        group.campaignId,
        group.items,
        context,
      );
      if (!response) return;
      for (const result of response.results) {
        if (isContacted(result)) {
          const item = group.items.find((i) => i.email === result.email);
          if (item) {
            if (!contactedPerGroup.has(item.groupKey)) {
              contactedPerGroup.set(item.groupKey, new Set());
            }
            contactedPerGroup.get(item.groupKey)!.add(item.leadId!);
          }
        }
      }
    }),
  );

  const result = new Map<string, number>();
  for (const [key, set] of contactedPerGroup) {
    result.set(key, set.size);
  }
  return result;
}

/**
 * Count contacted leads grouped by individual brand ID (unnesting brand_ids).
 */
async function countContactedGroupedByBrand(
  servedConds: SQL[],
  context: ServiceContext,
): Promise<Map<string, number>> {
  const rows = await db
    .select({
      leadId: servedLeads.leadId,
      email: servedLeads.email,
      brandIds: servedLeads.brandIds,
      campaignId: servedLeads.campaignId,
    })
    .from(servedLeads)
    .where(and(...servedConds));

  if (rows.length === 0) return new Map();

  // Group by first brandId + campaignId for email-gateway calls
  const callGroups = new Map<string, { brandId: string; campaignId: string; items: (DeliveryStatusItem & { brandIds: string[] })[] }>();
  for (const row of rows) {
    if (!row.leadId) continue;
    const primaryBrandId = row.brandIds[0] ?? "unknown";
    const key = `${primaryBrandId}::${row.campaignId}`;
    if (!callGroups.has(key)) {
      callGroups.set(key, { brandId: primaryBrandId, campaignId: row.campaignId, items: [] });
    }
    callGroups.get(key)!.items.push({
      leadId: row.leadId,
      email: row.email,
      brandIds: row.brandIds,
    });
  }

  // Track contacted leadIds per brand
  const contactedPerBrand = new Map<string, Set<string>>();

  await Promise.all(
    Array.from(callGroups.values()).map(async (group) => {
      const response = await checkDeliveryStatus(
        group.brandId,
        group.campaignId,
        group.items,
        context,
      );
      if (!response) return;
      for (const result of response.results) {
        if (isContacted(result)) {
          const item = group.items.find((i) => i.email === result.email);
          if (item) {
            // Attribute to each brand in the array
            for (const bid of item.brandIds) {
              if (!contactedPerBrand.has(bid)) {
                contactedPerBrand.set(bid, new Set());
              }
              contactedPerBrand.get(bid)!.add(item.leadId!);
            }
          }
        }
      }
    }),
  );

  const result = new Map<string, number>();
  for (const [key, set] of contactedPerBrand) {
    result.set(key, set.size);
  }
  return result;
}

const ZERO_STATS = { groups: [] };

router.get("/stats", authenticate, async (req: AuthenticatedRequest, res) => {
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
      // Dynasty resolved to zero slugs — return zero stats immediately
      if (groupByParam) {
        res.json(ZERO_STATS);
      } else {
        res.json({
          served: 0,
          contacted: 0,
          buffered: 0,
          skipped: 0,
          apollo: { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 },
        });
      }
      return;
    }

    const served = buildConditions(req, "served", dynastyResolved);
    const buffer = buildConditions(req, "buffer", dynastyResolved);
    const egContext = getServiceContext(req);

    // --- Dynasty groupBy (requires reverse map) ---
    if (groupByParam === "workflowDynastySlug" || groupByParam === "featureDynastySlug") {
      const isWorkflow = groupByParam === "workflowDynastySlug";
      const dbField = isWorkflow ? "workflowSlug" : "featureSlug";
      const servedCol = COLUMN_MAP[dbField].served;
      const bufferCol = COLUMN_MAP[dbField].buffer;
      const context = { orgId: req.orgId, userId: req.userId, runId: req.runId };

      const [dynastyMap, servedRows, contactedMap, bufferRows] = await Promise.all([
        isWorkflow ? fetchWorkflowDynastyMap(context) : fetchFeatureDynastyMap(context),
        db
          .select({ key: servedCol, count: count() })
          .from(servedLeads)
          .where(and(...served.conds))
          .groupBy(servedCol),
        countContactedGrouped(served.conds, dbField, egContext),
        db
          .select({ key: bufferCol, status: leadBuffer.status, count: count() })
          .from(leadBuffer)
          .where(and(...buffer.conds))
          .groupBy(bufferCol, leadBuffer.status),
      ]);

      // Aggregate by dynasty slug using reverse map
      const groups = new Map<
        string,
        { served: number; contacted: number; buffered: number; skipped: number }
      >();

      const getGroup = (dynastyKey: string) => {
        if (!groups.has(dynastyKey))
          groups.set(dynastyKey, { served: 0, contacted: 0, buffered: 0, skipped: 0 });
        return groups.get(dynastyKey)!;
      };

      const toDynasty = (slug: string | null): string =>
        dynastyMap.get(slug ?? "") ?? slug ?? "unknown";

      for (const row of servedRows) {
        getGroup(toDynasty(row.key)).served += row.count;
      }
      for (const [key, contacted] of contactedMap) {
        getGroup(toDynasty(key)).contacted += contacted;
      }
      for (const row of bufferRows) {
        const g = getGroup(toDynasty(row.key));
        if (row.status === "buffered") g.buffered += row.count;
        if (row.status === "skipped") g.skipped += row.count;
      }

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({
          key,
          ...stats,
        })),
      });
      return;
    }

    // --- brandId groupBy (unnest brand_ids) ---
    if (groupByParam === "brandId") {
      const [servedRows, contactedMap, bufferRows] = await Promise.all([
        // Unnest brand_ids for per-brand served counts
        db.execute(sql`
          SELECT unnest(brand_ids) AS key, COUNT(*)::int AS count
          FROM served_leads
          WHERE ${and(...served.conds)}
          GROUP BY key
        `) as Promise<{ key: string; count: number }[]>,
        countContactedGroupedByBrand(served.conds, egContext),
        // Unnest brand_ids for per-brand buffer counts
        db.execute(sql`
          SELECT unnest(brand_ids) AS key, status, COUNT(*)::int AS count
          FROM lead_buffer
          WHERE ${and(...buffer.conds)}
          GROUP BY key, status
        `) as Promise<{ key: string; status: string; count: number }[]>,
      ]);

      const groups = new Map<
        string,
        { served: number; contacted: number; buffered: number; skipped: number }
      >();

      const getGroup = (key: string | null) => {
        const k = key ?? "unknown";
        if (!groups.has(k))
          groups.set(k, { served: 0, contacted: 0, buffered: 0, skipped: 0 });
        return groups.get(k)!;
      };

      for (const row of servedRows) {
        getGroup(row.key).served = row.count;
      }
      for (const [key, contacted] of contactedMap) {
        getGroup(key).contacted = contacted;
      }
      for (const row of bufferRows) {
        const g = getGroup(row.key);
        if (row.status === "buffered") g.buffered = row.count;
        if (row.status === "skipped") g.skipped = row.count;
      }

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({
          key,
          ...stats,
        })),
      });
      return;
    }

    // --- Standard groupBy (exact slug columns) ---
    if (groupByParam) {
      const field = groupByParam as keyof typeof COLUMN_MAP;
      const servedCol = COLUMN_MAP[field].served;
      const bufferCol = COLUMN_MAP[field].buffer;

      const [servedRows, contactedMap, bufferRows] = await Promise.all([
        db
          .select({ key: servedCol, count: count() })
          .from(servedLeads)
          .where(and(...served.conds))
          .groupBy(servedCol),
        countContactedGrouped(served.conds, field, egContext),
        db
          .select({ key: bufferCol, status: leadBuffer.status, count: count() })
          .from(leadBuffer)
          .where(and(...buffer.conds))
          .groupBy(bufferCol, leadBuffer.status),
      ]);

      // Merge into a map keyed by groupBy value
      const groups = new Map<
        string,
        { served: number; contacted: number; buffered: number; skipped: number }
      >();

      const getGroup = (key: string | null) => {
        const k = key ?? "unknown";
        if (!groups.has(k))
          groups.set(k, { served: 0, contacted: 0, buffered: 0, skipped: 0 });
        return groups.get(k)!;
      };

      for (const row of servedRows) {
        getGroup(row.key).served = row.count;
      }
      for (const [key, contacted] of contactedMap) {
        getGroup(key).contacted = contacted;
      }
      for (const row of bufferRows) {
        const g = getGroup(row.key);
        if (row.status === "buffered") g.buffered = row.count;
        if (row.status === "skipped") g.skipped = row.count;
      }

      res.json({
        groups: Array.from(groups.entries()).map(([key, stats]) => ({
          key,
          ...stats,
        })),
      });
      return;
    }

    // --- Flat response ---
    const apolloFilters: Record<string, unknown> = {};
    if (served.brandIdStr) apolloFilters.brandId = served.brandIdStr;
    if (served.campaignIdStr) apolloFilters.campaignId = served.campaignIdStr;
    if (served.runIdList.length > 0) apolloFilters.runIds = served.runIdList;

    const [servedResult, contacted, bufferRows, apollo] = await Promise.all([
      db
        .select({ count: count() })
        .from(servedLeads)
        .where(and(...served.conds))
        .then(([r]) => r),
      countContacted(served.conds, egContext),
      db
        .select({ status: leadBuffer.status, count: count() })
        .from(leadBuffer)
        .where(and(...buffer.conds))
        .groupBy(leadBuffer.status),
      fetchApolloStats(
        apolloFilters as Parameters<typeof fetchApolloStats>[0],
        served.orgIdStr ?? req.orgId,
        egContext,
      ),
    ]);

    const bufferByStatus = Object.fromEntries(
      bufferRows.map((r) => [r.status, r.count]),
    );

    res.json({
      served: servedResult?.count ?? 0,
      contacted,
      buffered: bufferByStatus["buffered"] ?? 0,
      skipped: bufferByStatus["skipped"] ?? 0,
      apollo,
    });
  } catch (error) {
    console.error("[lead-service] Stats error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
