import { Router } from "express";
import { eq, and, count, inArray, or, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";
import {
  checkDeliveryStatus,
  isContacted,
  type DeliveryStatusItem,
} from "../lib/email-gateway-client.js";

const VALID_GROUP_BY = ["campaignId", "brandId"] as const;
type GroupByField = (typeof VALID_GROUP_BY)[number];

const COLUMN_MAP = {
  campaignId: { served: servedLeads.campaignId, buffer: leadBuffer.campaignId },
  brandId: { served: servedLeads.brandId, buffer: leadBuffer.brandId },
} as const;

const router = Router();

function buildConditions(
  req: AuthenticatedRequest,
  table: "served" | "buffer",
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
    if (brandIdStr) conds.push(eq(servedLeads.brandId, brandIdStr));
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
    return { conds, runIdList, brandIdStr, campaignIdStr, orgIdStr };
  }

  const conds: SQL[] = [eq(leadBuffer.orgId, req.orgId!)];
  if (brandIdStr) conds.push(eq(leadBuffer.brandId, brandIdStr));
  if (campaignIdStr) conds.push(eq(leadBuffer.campaignId, campaignIdStr));
  if (orgIdStr) conds.push(eq(leadBuffer.orgId, orgIdStr));
  if (userIdStr) conds.push(eq(leadBuffer.userId, userIdStr));
  if (runIdList.length > 0) {
    conds.push(inArray(leadBuffer.pushRunId, runIdList));
  }
  return { conds, runIdList, brandIdStr, campaignIdStr, orgIdStr };
}

/**
 * Count distinct contacted leads by querying email-gateway for delivery status.
 * Groups served leads by brandId+campaignId, calls email-gateway per group,
 * and returns the count of unique leadIds confirmed contacted.
 */
async function countContacted(
  servedConds: SQL[],
  context: { orgId?: string; userId?: string; runId?: string },
): Promise<number> {
  const rows = await db
    .select({
      leadId: servedLeads.leadId,
      email: servedLeads.email,
      brandId: servedLeads.brandId,
      campaignId: servedLeads.campaignId,
    })
    .from(servedLeads)
    .where(and(...servedConds));

  if (rows.length === 0) return 0;

  // Group by brandId+campaignId since email-gateway scopes status per brand/campaign
  const groups = new Map<string, { brandId: string; campaignId: string; items: DeliveryStatusItem[] }>();
  for (const row of rows) {
    if (!row.leadId) continue;
    const key = `${row.brandId}::${row.campaignId}`;
    if (!groups.has(key)) {
      groups.set(key, { brandId: row.brandId, campaignId: row.campaignId, items: [] });
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
          if (item) contactedLeadIds.add(item.leadId);
        }
      }
    }),
  );

  return contactedLeadIds.size;
}

/**
 * Count contacted leads per groupBy dimension (brandId or campaignId).
 */
async function countContactedGrouped(
  servedConds: SQL[],
  groupByField: GroupByField,
  context: { orgId?: string; userId?: string; runId?: string },
): Promise<Map<string, number>> {
  const groupCol = COLUMN_MAP[groupByField].served;

  const rows = await db
    .select({
      leadId: servedLeads.leadId,
      email: servedLeads.email,
      brandId: servedLeads.brandId,
      campaignId: servedLeads.campaignId,
      groupKey: groupCol,
    })
    .from(servedLeads)
    .where(and(...servedConds));

  if (rows.length === 0) return new Map();

  // Group by brandId+campaignId for email-gateway calls
  const callGroups = new Map<string, { brandId: string; campaignId: string; items: (DeliveryStatusItem & { groupKey: string })[] }>();
  for (const row of rows) {
    if (!row.leadId) continue;
    const key = `${row.brandId}::${row.campaignId}`;
    if (!callGroups.has(key)) {
      callGroups.set(key, { brandId: row.brandId, campaignId: row.campaignId, items: [] });
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
            contactedPerGroup.get(item.groupKey)!.add(item.leadId);
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

    const served = buildConditions(req, "served");
    const buffer = buildConditions(req, "buffer");
    const egContext = { orgId: req.orgId, userId: req.userId, runId: req.runId };

    // --- Grouped response ---
    if (groupByParam) {
      const field = groupByParam as GroupByField;
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
        { userId: req.userId, runId: req.runId },
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
    console.error("[stats] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
