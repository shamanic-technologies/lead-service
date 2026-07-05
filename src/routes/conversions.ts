import { Router, Request, Response, NextFunction } from "express";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { apiKeyAuth, requireOrgId, AuthenticatedRequest } from "../middleware/auth.js";
import { CONVERSION_INGEST_URL } from "../config.js";
import {
  isConversionEvent,
  generateConversionToken,
  computeDedupeSignature,
  matchConversion,
  type ConversionEventName,
} from "../lib/conversions.js";

const router = Router();

// Express 4 does not forward async handler rejections to the error middleware.
// Wrap so a DB error surfaces as a clean 500 (fail loud) instead of a hung socket.
function wrap<Req extends Request = Request>(
  fn: (req: Req, res: Response, next: NextFunction) => Promise<void>,
) {
  return (req: Request, res: Response, next: NextFunction) => {
    fn(req as Req, res, next).catch(next);
  };
}

// --- Public ingest token auth ---
// The publishable write-key arrives in `x-conversion-token` OR `Authorization: Bearer`.
// It resolves to exactly one (brandId, orgId). Missing/invalid → 401. Never leak WHY.
interface ConversionRequest extends Request {
  conversionBrandId?: string;
  conversionOrgId?: string;
}

function extractToken(req: Request): string | null {
  const headerToken = req.headers["x-conversion-token"];
  if (typeof headerToken === "string" && headerToken.trim()) return headerToken.trim();

  const auth = req.headers["authorization"];
  if (typeof auth === "string") {
    const m = /^Bearer\s+(.+)$/i.exec(auth.trim());
    if (m && m[1].trim()) return m[1].trim();
  }
  return null;
}

const conversionTokenAuth = wrap(async function conversionTokenAuth(
  req: ConversionRequest,
  res: Response,
  next: NextFunction,
): Promise<void> {
  const token = extractToken(req);
  if (!token) {
    res.status(401).json({ error: "Invalid conversion token" });
    return;
  }

  const rows = (await db.execute(sql`
    SELECT brand_id, org_id
    FROM brand_conversion_tokens
    WHERE token = ${token}
    LIMIT 1
  `)) as unknown as Array<{ brand_id: string; org_id: string }>;

  if (rows.length === 0) {
    res.status(401).json({ error: "Invalid conversion token" });
    return;
  }

  req.conversionBrandId = rows[0].brand_id;
  req.conversionOrgId = rows[0].org_id;
  next();
});

// --- Public ingest body ---
const ConversionIngestBodySchema = z.object({
  event: z.string(),
  email: z.string().optional(),
  phone: z.string().optional(),
  firstName: z.string().optional(),
  lastName: z.string().optional(),
  companyUrl: z.string().optional(),
  dedupeKey: z.string().optional(),
  valueCents: z.number().int().optional(),
});

/**
 * POST /public/conversions
 *
 * Called directly by the CLIENT's website (token-auth, NO Clerk). Records a conversion
 * event and attributes it to a lead we emailed for that brand. NEVER leaks the match
 * result to the public caller — always returns { received: true } on success.
 */
router.post("/public/conversions", conversionTokenAuth, wrap(async (req: ConversionRequest, res) => {
  const brandId = req.conversionBrandId!;
  const orgId = req.conversionOrgId!;

  const parsed = ConversionIngestBodySchema.safeParse(req.body);
  if (!parsed.success || !isConversionEvent(parsed.data.event)) {
    res.status(400).json({ error: "Invalid or missing event" });
    return;
  }

  const body = parsed.data;
  const event = body.event as ConversionEventName;
  const now = new Date();

  const dedupeSignature = computeDedupeSignature({
    dedupeKey: body.dedupeKey,
    event,
    email: body.email,
    phone: body.phone,
    now,
  });

  // Dedupe: a duplicate returns 200 without a second attribution row.
  if (dedupeSignature) {
    const existing = (await db.execute(sql`
      SELECT 1
      FROM conversion_events
      WHERE brand_id = ${brandId} AND dedupe_signature = ${dedupeSignature}
      LIMIT 1
    `)) as unknown as Array<unknown>;
    if (existing.length > 0) {
      res.json({ received: true });
      return;
    }
  }

  const match = await matchConversion({
    brandId,
    email: body.email,
    phone: body.phone,
    firstName: body.firstName,
    lastName: body.lastName,
    companyUrl: body.companyUrl,
  });

  // Race-safe insert: if a concurrent request already claimed this signature, the
  // partial unique index makes this a no-op and we still return 200.
  await db.execute(sql`
    INSERT INTO conversion_events (
      brand_id, org_id, event, email, phone, first_name, last_name, company_url,
      dedupe_key, dedupe_signature, value_cents, matched_lead_id, match_method,
      match_confidence, attribution_status, candidate_count, received_at
    ) VALUES (
      ${brandId}, ${orgId}, ${event}, ${body.email ?? null}, ${body.phone ?? null},
      ${body.firstName ?? null}, ${body.lastName ?? null}, ${body.companyUrl ?? null},
      ${body.dedupeKey ?? null}, ${dedupeSignature}, ${body.valueCents ?? null},
      ${match.matchedLeadId}, ${match.matchMethod}, ${match.matchConfidence},
      ${match.attributionStatus}, ${match.candidateCount}, ${now}
    )
    ON CONFLICT (brand_id, dedupe_signature) WHERE dedupe_signature IS NOT NULL DO NOTHING
  `);

  res.json({ received: true });
}));

/**
 * GET /orgs/brands/:brandId/conversion-token
 *
 * Get-or-create the brand's publishable write-key. Returns the token in FULL (it is a
 * publishable key, not a secret) plus the public ingest URL a third party hits.
 */
router.get(
  "/orgs/brands/:brandId/conversion-token",
  apiKeyAuth,
  requireOrgId,
  wrap(async (req: AuthenticatedRequest, res: Response) => {
    const brandId = req.params.brandId;
    const orgId = req.orgId!;
    const token = generateConversionToken();

    const rows = (await db.execute(sql`
      INSERT INTO brand_conversion_tokens (brand_id, org_id, token)
      VALUES (${brandId}, ${orgId}, ${token})
      ON CONFLICT (brand_id) DO UPDATE SET updated_at = now()
      RETURNING token
    `)) as unknown as Array<{ token: string }>;

    res.json({ token: rows[0].token, ingestUrl: CONVERSION_INGEST_URL });
  }),
);

/**
 * POST /orgs/brands/:brandId/conversion-token/rotate
 *
 * Replace the brand's token with a fresh one; the old token immediately 401s on ingest.
 */
router.post(
  "/orgs/brands/:brandId/conversion-token/rotate",
  apiKeyAuth,
  requireOrgId,
  wrap(async (req: AuthenticatedRequest, res: Response) => {
    const brandId = req.params.brandId;
    const orgId = req.orgId!;
    const token = generateConversionToken();

    const rows = (await db.execute(sql`
      INSERT INTO brand_conversion_tokens (brand_id, org_id, token)
      VALUES (${brandId}, ${orgId}, ${token})
      ON CONFLICT (brand_id) DO UPDATE SET token = EXCLUDED.token, rotated_at = now(), updated_at = now()
      RETURNING token
    `)) as unknown as Array<{ token: string }>;

    res.json({ token: rows[0].token, ingestUrl: CONVERSION_INGEST_URL });
  }),
);

export default router;
