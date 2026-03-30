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

export async function authenticate(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
) {
  try {
    const apiKey = req.headers["x-api-key"] as string;
    if (!apiKey || apiKey !== LEAD_SERVICE_API_KEY) {
      return res.status(401).json({ error: "Invalid API key" });
    }

    const orgId = req.headers["x-org-id"] as string;
    const userId = req.headers["x-user-id"] as string;
    const runId = req.headers["x-run-id"] as string;

    if (!orgId || !userId || !runId) {
      return res.status(400).json({ error: "x-org-id, x-user-id, and x-run-id headers required" });
    }

    const campaignId = req.headers["x-campaign-id"] as string | undefined;
    const brandId = req.headers["x-brand-id"] as string | undefined;
    const workflowSlug = req.headers["x-workflow-slug"] as string | undefined;
    const featureSlug = req.headers["x-feature-slug"] as string | undefined;

    req.orgId = orgId;
    req.userId = userId;
    req.runId = runId;
    if (campaignId) req.campaignId = campaignId;
    if (brandId) req.brandId = brandId;
    if (workflowSlug) req.workflowSlug = workflowSlug;
    if (featureSlug) req.featureSlug = featureSlug;

    next();
  } catch (error) {
    console.error("[auth] Error:", error);
    return res.status(401).json({ error: "Authentication failed" });
  }
}
