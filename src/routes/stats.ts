import { Router } from "express";
import { eq, and, count, inArray, or, sql, type SQL } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { servedLeads, leadBuffer } from "../db/schema.js";
import { fetchApolloStats } from "../lib/apollo-client.js";

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

    // --- Grouped response ---
    if (groupByParam) {
      const field = groupByParam as GroupByField;
      const servedCol = COLUMN_MAP[field].served;
      const bufferCol = COLUMN_MAP[field].buffer;

      const [servedRows, bufferRows] = await Promise.all([
        db
          .select({ key: servedCol, count: count() })
          .from(servedLeads)
          .where(and(...served.conds))
          .groupBy(servedCol),
        db
          .select({ key: bufferCol, status: leadBuffer.status, count: count() })
          .from(leadBuffer)
          .where(and(...buffer.conds))
          .groupBy(bufferCol, leadBuffer.status),
      ]);

      // Merge into a map keyed by groupBy value
      const groups = new Map<
        string,
        { served: number; buffered: number; skipped: number }
      >();

      const getGroup = (key: string | null) => {
        const k = key ?? "unknown";
        if (!groups.has(k))
          groups.set(k, { served: 0, buffered: 0, skipped: 0 });
        return groups.get(k)!;
      };

      for (const row of servedRows) {
        getGroup(row.key).served = row.count;
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

    // --- Flat response (existing behavior) ---
    const apolloFilters: Record<string, unknown> = {};
    if (served.brandIdStr) apolloFilters.brandId = served.brandIdStr;
    if (served.campaignIdStr) apolloFilters.campaignId = served.campaignIdStr;
    if (served.runIdList.length > 0) apolloFilters.runIds = served.runIdList;

    const [servedResult, bufferRows, apollo] = await Promise.all([
      db
        .select({ count: count() })
        .from(servedLeads)
        .where(and(...served.conds))
        .then(([r]) => r),
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
