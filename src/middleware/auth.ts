import { Request, Response, NextFunction } from "express";
import { eq, and } from "drizzle-orm";
import { db } from "../db/index.js";
import { organizations } from "../db/schema.js";

export interface AuthenticatedRequest extends Request {
  organizationId?: string;
  appId?: string;
  externalOrgId?: string;
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

    const appId = req.headers["x-app-id"] as string;
    const externalOrgId = req.headers["x-org-id"] as string;

    if (!appId || !externalOrgId) {
      return res.status(400).json({ error: "x-app-id and x-org-id headers required" });
    }

    // Find or create org
    let org = await db.query.organizations.findFirst({
      where: and(
        eq(organizations.appId, appId),
        eq(organizations.externalId, externalOrgId)
      ),
    });

    if (!org) {
      const [newOrg] = await db
        .insert(organizations)
        .values({ appId, externalId: externalOrgId })
        .onConflictDoNothing()
        .returning();

      if (newOrg) {
        org = newOrg;
      } else {
        // Race condition: another request created it
        org = await db.query.organizations.findFirst({
          where: and(
            eq(organizations.appId, appId),
            eq(organizations.externalId, externalOrgId)
          ),
        });
      }
    }

    if (!org) {
      return res.status(500).json({ error: "Failed to resolve organization" });
    }

    req.organizationId = org.id;
    req.appId = appId;
    req.externalOrgId = externalOrgId;
    next();
  } catch (error) {
    console.error("[auth] Error:", error);
    return res.status(401).json({ error: "Authentication failed" });
  }
}
