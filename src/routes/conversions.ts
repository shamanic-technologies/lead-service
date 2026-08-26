import { Router, Request, Response, NextFunction } from "express";
import { z } from "zod";
import { sql } from "drizzle-orm";
import { db } from "../db/index.js";
import { apiKeyAuth, requireOrgId, AuthenticatedRequest } from "../middleware/auth.js";
import { CONVERSION_INGEST_URL } from "../config.js";
import {
  canonicalizeConversionEvent,
  isPingEvent,
  deriveConversionStatus,
  generateConversionToken,
  computeDedupeSignature,
  matchConversion,
} from "../lib/conversions.js";
import {
  LEAD_STEP_OUTCOMES,
  canonicalizeStepOutcome,
  type LeadStepOutcomeName,
  type StatementSource,
} from "../lib/step-statements.js";
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

// Transition alias for the "purchase" → "sale" rename. The internal count contracts emit the
// canonical "sale" key AND a legacy "purchase" key mirroring the same value, so consumers still
// reading `purchase` (features-service conversion-counts clients) stay green while they migrate
// to `sale`. Drop the mirror once every consumer reads `sale`.
function withLegacyPurchaseAlias<T>(
  byEvent: Record<LeadStepOutcomeName, T>,
): Record<string, T> {
  return { ...byEvent, purchase: byEvent.sale };
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

  // Accept the canonical "sale" AND the legacy "purchase" spelling: canonicalize BEFORE
  // dedupe + storage so a legacy client firing "purchase" is stored/attributed/deduped
  // exactly like a "sale". Anything unrecognized (garbage) → fail loud 400.
  const event = canonicalizeConversionEvent(parsed.data.event);
  if (!event) {
    res.status(400).json({ error: "Invalid or missing event" });
    return;
  }

  const body = parsed.data;
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
 * All FIVE canonical step keys (signup | meeting_booked | meeting_attended | form_submission |
 * sale) are ALWAYS present (0 when none). "meeting_attended" is statable by hand only — a
 * page-load tag cannot observe somebody showing up to a meeting — but it counts exactly like the
 * four the tracker reports, which is what finally lets the booked-to-attended rate brand-service
 * prices with be measured against reality. The terminal "customer paid" event was renamed
 * "purchase" → "sale"; for the migration window the response ALSO carries a legacy "purchase"
 * key mirroring "sale" so consumers still reading `purchase` stay green until they migrate.
 *
 * `counts` totals BOTH sources and is what every existing consumer keeps reading unchanged.
 * `bySource.tracker` / `bySource.manual` split the same rows by who said so — a hand-stated
 * outcome is distinguishable from a tracker-reported one after the fact without changing what
 * either counts toward. For every key, tracker + manual === counts.
 *
 * A "never" statement ("this lead will not book / attend / buy") is NOT an outcome: it lives in
 * lead_step_disqualifications, which this read does not touch, so nothing here can count it.
 * Never 404 — a brand with zero conversions returns all-zero counts.
 */
router.get(
  "/internal/brands/:brandId/conversion-counts",
  apiKeyAuth,
  wrap(async (req: Request, res: Response) => {
    const brandId = req.params.brandId;

    const rows = (await db.execute(sql`
      SELECT event, source, count(*)::int AS n
      FROM conversion_events
      WHERE brand_id = ${brandId}
        AND attribution_status = 'attributed'
      GROUP BY event, source
    `)) as unknown as Array<{ event: string; source: string; n: number }>;

    const zeroed = () =>
      Object.fromEntries(LEAD_STEP_OUTCOMES.map((e) => [e, 0])) as Record<
        LeadStepOutcomeName,
        number
      >;
    const counts = zeroed();
    const bySource: Record<StatementSource, Record<LeadStepOutcomeName, number>> = {
      tracker: zeroed(),
      manual: zeroed(),
    };

    for (const row of rows) {
      // Fold canonical + any legacy-spelled historical row into its canonical bucket.
      const canonical = canonicalizeStepOutcome(row.event);
      if (!canonical) continue;
      counts[canonical] += row.n;
      // Anything not explicitly stated by a human came off the website tracker — which is what
      // every row written before the column existed is.
      bySource[row.source === "manual" ? "manual" : "tracker"][canonical] += row.n;
    }

    res.json({
      counts: withLegacyPurchaseAlias(counts),
      bySource: {
        tracker: withLegacyPurchaseAlias(bySource.tracker),
        manual: withLegacyPurchaseAlias(bySource.manual),
      },
    });
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
 *    its count > 0. All five canonical step keys (…, meeting_attended, sale) are ALWAYS present
 *    (empty object when none), plus a legacy "purchase" key mirroring "sale" for the rename window.
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
      LEAD_STEP_OUTCOMES.map((e) => [e, {} as Record<string, number>]),
    ) as Record<LeadStepOutcomeName, Record<string, number>>;
    const undated = Object.fromEntries(
      LEAD_STEP_OUTCOMES.map((e) => [e, 0]),
    ) as Record<LeadStepOutcomeName, number>;

    for (const row of rows) {
      // Fold canonical + any legacy-spelled historical row into its canonical bucket.
      const canonical = canonicalizeStepOutcome(row.event);
      if (!canonical) continue;
      if (row.day === null) {
        undated[canonical] += row.n;
      } else {
        byDay[canonical][row.day] = (byDay[canonical][row.day] ?? 0) + row.n;
      }
    }

    res.json({
      byDay: withLegacyPurchaseAlias(byDay),
      undated: withLegacyPurchaseAlias(undated),
    });
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
 *  - `event` is REQUIRED and must be one of the five step outcomes
 *    (signup | meeting_booked | meeting_attended | form_submission | sale). The legacy spelling "purchase"
 *    is still accepted and normalized to "sale". Missing/invalid → 400. "ping" is a
 *    liveness heartbeat, never a conversion → not accepted here.
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

    // Accept canonical "sale" AND legacy "purchase" (both resolve to the canonical "sale"
    // that stored rows carry); anything else (incl. "ping", garbage, missing) → 400.
    const event = canonicalizeStepOutcome(req.query.event);
    if (!event) {
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
