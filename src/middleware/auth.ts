import { Request, Response, NextFunction } from "express";

export interface AuthenticatedRequest extends Request {
  orgId?: string;
  userId?: string;
  runId?: string;
}

export async function authenticate(
  req: AuthenticatedRequest,
  res: Response,
  next: NextFunction
) {
  try {
    const apiKey = req.headers["x-api-key"] as string;
    if (!apiKey || apiKey !== process.env.LEAD_SERVICE_API_KEY) {
      return res.status(401).json({ error: "Invalid API key" });
    }

    const orgId = req.headers["x-org-id"] as string;
    const userId = req.headers["x-user-id"] as string;
    const runId = req.headers["x-run-id"] as string;

    if (!orgId || !userId || !runId) {
      return res.status(400).json({ error: "x-org-id, x-user-id, and x-run-id headers required" });
    }

    req.orgId = orgId;
    req.userId = userId;
    req.runId = runId;
    next();
  } catch (error) {
    console.error("[auth] Error:", error);
    return res.status(401).json({ error: "Authentication failed" });
  }
}
