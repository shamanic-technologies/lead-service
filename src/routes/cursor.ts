import { Router } from "express";
import { eq, and } from "drizzle-orm";
import { type AuthenticatedRequest, authenticate } from "../middleware/auth.js";
import { db } from "../db/index.js";
import { cursors } from "../db/schema.js";

const router = Router();

router.get("/cursor/:namespace", authenticate, async (req: AuthenticatedRequest, res) => {
  /*
    #swagger.summary = 'Get cursor state for a namespace'
    #swagger.parameters['x-app-id'] = { in: 'header', required: true, type: 'string', description: 'Identifies the calling application, e.g. mcpfactory' }
    #swagger.parameters['x-org-id'] = { in: 'header', required: true, type: 'string', description: 'External organization ID, e.g. Clerk org ID' }
  */
  try {
    const { namespace } = req.params;

    const cursor = await db.query.cursors.findFirst({
      where: and(
        eq(cursors.organizationId, req.organizationId!),
        eq(cursors.namespace, namespace)
      ),
    });

    res.json({ state: cursor?.state ?? null });
  } catch (error) {
    console.error("[cursor/get] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

router.put("/cursor/:namespace", authenticate, async (req: AuthenticatedRequest, res) => {
  /*
    #swagger.summary = 'Set cursor state for a namespace'
    #swagger.parameters['x-app-id'] = { in: 'header', required: true, type: 'string', description: 'Identifies the calling application, e.g. mcpfactory' }
    #swagger.parameters['x-org-id'] = { in: 'header', required: true, type: 'string', description: 'External organization ID, e.g. Clerk org ID' }
    #swagger.requestBody = {
      required: true,
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["state"],
            properties: {
              state: { type: "object", description: "Arbitrary JSON cursor state" }
            }
          }
        }
      }
    }
  */
  try {
    const { namespace } = req.params;
    const { state } = req.body;

    if (state === undefined) {
      return res.status(400).json({ error: "state required" });
    }

    const existing = await db.query.cursors.findFirst({
      where: and(
        eq(cursors.organizationId, req.organizationId!),
        eq(cursors.namespace, namespace)
      ),
    });

    if (existing) {
      await db
        .update(cursors)
        .set({ state, updatedAt: new Date() })
        .where(eq(cursors.id, existing.id));
    } else {
      await db.insert(cursors).values({
        organizationId: req.organizationId!,
        namespace,
        state,
      });
    }

    res.json({ ok: true });
  } catch (error) {
    console.error("[cursor/put] Error:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
