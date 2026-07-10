import { Router, Request, Response, NextFunction } from "express";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { apiKeyAuth, requireOrgId, AuthenticatedRequest } from "../middleware/auth.js";
import { CONVERSION_INGEST_URL } from "../config.js";
import {
  isConversionEvent,
  isPingEvent,
  deriveConversionStatus,
  generateConversionToken,
  computeDedupeSignature,
  matchConversion,
  CONVERSION_EVENTS,
  type ConversionEventName,
} from "../lib/conversions.js";
import { toIsoTimestamp } from "../lib/basic-leads.js";

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
  if (!parsed.success) {
    res.status(400).json({ error: "Invalid or missing event" });
    return;
  }

  // Liveness heartbeat. The tag fires this on page-load: authenticate the token exactly
  // like a real event (already done above), stamp last_ping_at, and STOP — no attribution,
  // no conversion_events row, no dedupe, never counted as a conversion. Same opaque
  // { received: true } so the public caller learns nothing.
  if (isPingEvent(parsed.data.event)) {
    await db.execute(sql`
      UPDATE brand_conversion_tokens
      SET last_ping_at = now()
      WHERE brand_id = ${brandId}
    `);
    res.json({ received: true });
    return;
  }

  if (!isConversionEvent(parsed.data.event)) {
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
  //
  // received_at is bound as `now.toISOString()`, NOT a raw `Date`: a raw `sql` template
  // hands params straight to postgres.js `Bind`, which does not serialize a Date (only
  // drizzle's typed insert builder / postgres.js's own tagged template do). Binding a
  // `Date` throws `ERR_INVALID_ARG_TYPE ... Received an instance of Date` at Bind time —
  // a client-side throw invisible to raw-SQL/EXECUTE tests — which 500'd every real
  // conversion in prod (#357). The ISO string casts to timestamptz for the column.
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
      ${match.attributionStatus}, ${match.candidateCount}, ${now.toISOString()}
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
      RETURNING token, last_ping_at
    `)) as unknown as Array<{ token: string; last_ping_at: Date | string | null }>;

    // Liveness overlay, DERIVED from received signals (never self-attested). Real events
    // live in conversion_events; ping never lands there, so eventTypesSeen excludes it
    // for free. array_agg over zero rows → null → [].
    const agg = (await db.execute(sql`
      SELECT max(received_at) AS last_event_at,
             array_agg(DISTINCT event) AS event_types
      FROM conversion_events
      WHERE brand_id = ${brandId}
    `)) as unknown as Array<{
      last_event_at: Date | string | null;
      event_types: string[] | null;
    }>;

    const lastEventAt = toIsoTimestamp(agg[0]?.last_event_at ?? null);
    const lastPingAt = toIsoTimestamp(rows[0].last_ping_at ?? null);
    const eventTypesSeen = agg[0]?.event_types ?? [];
    const status = deriveConversionStatus({
      hasRealEvent: lastEventAt !== null,
      hasPing: lastPingAt !== null,
    });

    res.json({
      token: rows[0].token,
      ingestUrl: CONVERSION_INGEST_URL,
      status,
      lastEventAt,
      lastPingAt,
      eventTypesSeen,
    });
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

/**
 * GET /internal/brands/:brandId/conversion-counts
 *
 * INTERNAL (service-auth: x-api-key — same tier as other /internal/* routes, NO Clerk).
 * Returns the per-event-type COUNT of REAL, attributed conversions for the brand, so
 * features-service can compute real signups / cost-per-signup for the dashboard.
 *
 * Each count = deduped, attributed conversion events of that type:
 *  - Rows in conversion_events are already deduped at WRITE (partial unique index on
 *    (brand_id, dedupe_signature)), so counting stored rows is inherently deduped per the
 *    same rules POST /public/conversions applies.
 *  - `attribution_status = 'attributed'` keeps only conversions credited to a lead we
 *    emailed for this brand (auto-accepted); excludes needs_review (held) + unmatched.
 *    This is the "counts toward the brand's real outcomes" set.
 *  - "ping" is a liveness heartbeat that never lands in conversion_events → excluded for free.
 *
 * All four event-type keys are ALWAYS present (0 when none received). Never 404 — a brand
 * with zero conversions returns all-zero counts.
 */
router.get(
  "/internal/brands/:brandId/conversion-counts",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;

    const rows = (await db.execute(sql`
      SELECT event, count(*)::int AS n
      FROM conversion_events
      WHERE brand_id = ${brandId}
        AND attribution_status = 'attributed'
      GROUP BY event
    `)) as unknown as Array<{ event: string; n: number }>;

    const counts = Object.fromEntries(
      CONVERSION_EVENTS.map((e) => [e, 0]),
    ) as Record<ConversionEventName, number>;

    for (const row of rows) {
      if (isConversionEvent(row.event)) counts[row.event] = row.n;
    }

    res.json({ counts });
  }),
);

/**
 * GET /internal/brands/:brandId/conversion-counts-by-day
 *
 * INTERNAL (service-auth: x-api-key — same tier as conversion-counts, NO Clerk).
 * Returns the SAME set of REAL, attributed conversions as conversion-counts, but broken
 * down by the CALENDAR DAY each conversion was received, so features-service can draw a
 * truthful per-day observed series on the Overview outreach-activity graph (instead of a
 * clicks × rate projection) for today AND past days.
 *
 *  - Same set as /conversion-counts: stored conversion_events rows (deduped at write via
 *    the (brand_id, dedupe_signature) partial unique index), filtered to
 *    attribution_status = 'attributed'. "ping" never lands in conversion_events → excluded.
 *  - `byDay[event]` maps a UTC calendar day (YYYY-MM-DD) → count. Days are bucketed by
 *    `received_at AT TIME ZONE 'UTC'`, matching the UTC-day convention the ingest dedupe
 *    signature already uses (`now.toISOString().slice(0,10)`). A day key appears only when
 *    its count > 0. All four event keys are ALWAYS present (empty object when none).
 *  - `undated[event]` counts attributed conversions whose day genuinely cannot be
 *    determined (received_at IS NULL). received_at is NOT NULL DEFAULT now() today, so this
 *    is 0 in practice — but it is surfaced EXPLICITLY, never dropped and never assigned a
 *    fabricated date, so the contract stays honest if an undated row ever exists.
 *  - Reconciliation guarantee: for every event, sum(byDay[event] values) + undated[event]
 *    === the count /conversion-counts returns for that event (same rows, same filter). No
 *    per-day figure exceeds or contradicts the total.
 *
 * Never 404 — a brand with zero attributed conversions returns all-empty byDay + all-zero
 * undated (200).
 */
router.get(
  "/internal/brands/:brandId/conversion-counts-by-day",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;

    // day is NULL only when received_at IS NULL (an undated conversion). Otherwise the UTC
    // calendar day as a YYYY-MM-DD text (cast date->text is ISO + deterministic regardless
    // of the session timezone). Same attributed-only, deduped-at-write set as /conversion-counts.
    const rows = (await db.execute(sql`
      SELECT
        event,
        CASE
          WHEN received_at IS NULL THEN NULL
          ELSE (received_at AT TIME ZONE 'UTC')::date::text
        END AS day,
        count(*)::int AS n
      FROM conversion_events
      WHERE brand_id = ${brandId}
        AND attribution_status = 'attributed'
      GROUP BY event, day
    `)) as unknown as Array<{ event: string; day: string | null; n: number }>;

    const byDay = Object.fromEntries(
      CONVERSION_EVENTS.map((e) => [e, {} as Record<string, number>]),
    ) as Record<ConversionEventName, Record<string, number>>;
    const undated = Object.fromEntries(
      CONVERSION_EVENTS.map((e) => [e, 0]),
    ) as Record<ConversionEventName, number>;

    for (const row of rows) {
      if (!isConversionEvent(row.event)) continue;
      if (row.day === null) {
        undated[row.event] += row.n;
      } else {
        byDay[row.event][row.day] = (byDay[row.event][row.day] ?? 0) + row.n;
      }
    }

    res.json({ byDay, undated });
  }),
);

/**
 * GET /internal/brands/:brandId/converted-lead-emails?event=<type>
 *
 * INTERNAL (service-auth: x-api-key — same tier as conversion-counts, NO Clerk).
 * Returns the SET of matched-lead canonical emails (the emails-we-served identity) that
 * have at least one REAL, attributed conversion of `event` for the brand — so
 * features-service can intersect it with each audience's email membership and count
 * conversions per audience.
 *
 *  - `event` is REQUIRED and must be one of the four conversion event types
 *    (signup | meeting_booked | form_submission | purchase). Missing/invalid → 400.
 *    "ping" is a liveness heartbeat, never a conversion → not accepted here.
 *  - Only `attribution_status = 'attributed'` rows count (credited to a lead we emailed
 *    for the brand; excludes needs_review + unmatched) — the SAME set conversion-counts uses.
 *  - The returned identity is the MATCHED LEAD's canonical (primary) email — the earliest
 *    email contact method for the lead — NOT the raw email a visitor typed on the client's
 *    site (which may differ, or be absent for a phone/name match). This is the join key
 *    features-service already holds from human-service audience membership. Lowercased +
 *    DISTINCT so the intersection is case-robust and de-duplicated.
 *  - A matched lead with no email contact method yields no join key → excluded (INNER
 *    LATERAL). Rows are already deduped at write via the (brand_id, dedupe_signature)
 *    partial unique index.
 *
 * Never 404 — a brand with zero attributed conversions of `event` returns an empty set (200).
 */
router.get(
  "/internal/brands/:brandId/converted-lead-emails",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;
    const event = req.query.event;

    if (!isConversionEvent(event)) {
      res.status(400).json({ error: "Invalid or missing event" });
      return;
    }

    const rows = (await db.execute(sql`
      SELECT DISTINCT lower(canonical.value) AS email
      FROM conversion_events ce
      JOIN LATERAL (
        SELECT cm.value
        FROM lead_contact_methods cm
        WHERE cm.lead_id = ce.matched_lead_id AND cm.channel = 'email'
        ORDER BY cm.created_at ASC NULLS LAST, cm.value ASC
        LIMIT 1
      ) canonical ON true
      WHERE ce.brand_id = ${brandId}
        AND ce.attribution_status = 'attributed'
        AND ce.event = ${event}
        AND canonical.value IS NOT NULL
    `)) as unknown as Array<{ email: string | null }>;

    const emails = rows
      .map((r) => r.email)
      .filter((e): e is string => typeof e === "string" && e.length > 0);

    res.json({ event, emails });
  }),
);

export default router;
