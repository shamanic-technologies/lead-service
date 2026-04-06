import { Request, Response, NextFunction } from "express";
import { LEAD_SERVICE_API_KEY } from "../config.js";

export interface ServiceContext {
  orgId?: string;
  userId?: string;
  runId?: string;
  campaignId?: string;
  brandId?: string;
  workflowSlug?: string;
  featureSlug?: string;
}

export interface AuthenticatedRequest extends Request {
  orgId?: string;
  userId?: string;
  runId?: string;
  campaignId?: string;
  brandId?: string;
  brandIds?: string[];
  workflowSlug?: string;
  featureSlug?: string;
}

export function getServiceContext(req: AuthenticatedRequest): ServiceContext {
  return {
    orgId: req.orgId,
    userId: req.userId,
    runId: req.runId,
    campaignId: req.campaignId,
    brandId: req.brandId,
    workflowSlug: req.workflowSlug,
    featureSlug: req.featureSlug,
  };
}

/**
 * Parse x-brand-id header as CSV, returning an array of brand IDs.
 */
function parseBrandIds(header: string | undefined): string[] {
  if (!header) return [];
  return String(header).split(",").map(s => s.trim()).filter(Boolean);
}

/**
 * Validates x-api-key header. Used on /public/*, /internal/*, and /orgs/* tiers.
 */
export function apiKeyAuth(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
) {
  const apiKey = req.headers["x-api-key"] as string;
  if (!apiKey || apiKey !== LEAD_SERVICE_API_KEY) {
    res.status(401).json({ error: "Invalid API key" });
    return;
  }
  next();
}

/**
 * Parses all 7 identity headers, requires only x-org-id.
 * Must be used after apiKeyAuth. Used on /orgs/* tier.
 */
export function requireOrgId(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
) {
  const orgId = req.headers["x-org-id"] as string;
  if (!orgId) {
    res.status(400).json({ error: "x-org-id header required" });
    return;
  }

  const userId = req.headers["x-user-id"] as string | undefined;
  const runId = req.headers["x-run-id"] as string | undefined;
  const campaignId = req.headers["x-campaign-id"] as string | undefined;
  const brandIdRaw = req.headers["x-brand-id"] as string | undefined;
  const workflowSlug = req.headers["x-workflow-slug"] as string | undefined;
  const featureSlug = req.headers["x-feature-slug"] as string | undefined;

  req.orgId = orgId;
  if (userId) req.userId = userId;
  if (runId) req.runId = runId;
  if (campaignId) req.campaignId = campaignId;
  if (brandIdRaw) {
    req.brandId = brandIdRaw;
    req.brandIds = parseBrandIds(brandIdRaw);
  } else {
    req.brandIds = [];
  }
  if (workflowSlug) req.workflowSlug = workflowSlug;
  if (featureSlug) req.featureSlug = featureSlug;

  next();
}
