import { REPLY_QUALIFICATION_SERVICE_URL, REPLY_QUALIFICATION_SERVICE_API_KEY } from "../config.js";

export interface Qualification {
  id: string;
  fromEmail: string;
  classification: string;
  confidence: number;
  createdAt: string;
}

/**
 * Fetch all qualifications for an org from reply-qualification-service.
 * Returns a Map of email -> latest classification (most recent createdAt wins).
 */
export async function fetchQualificationsByOrg(
  orgId: string,
  context?: { runId?: string; brandId?: string; campaignId?: string; workflowSlug?: string; featureSlug?: string },
): Promise<Map<string, Qualification>> {
  const headers: Record<string, string> = {
    "X-API-Key": REPLY_QUALIFICATION_SERVICE_API_KEY,
    "x-org-id": orgId,
  };
  if (context?.runId) headers["x-run-id"] = context.runId;
  if (context?.brandId) headers["x-brand-id"] = context.brandId;
  if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
  if (context?.workflowSlug) headers["x-workflow-slug"] = context.workflowSlug;
  if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

  const url = new URL("/qualifications", REPLY_QUALIFICATION_SERVICE_URL);
  url.searchParams.set("sourceOrgId", orgId);
  url.searchParams.set("limit", "10000");

  const response = await fetch(url.toString(), { headers });

  if (!response.ok) {
    const error = await response.text();
    console.error(
      `[reply-qualification-client] Fetch failed: ${response.status} - ${error}`,
    );
    throw new Error(`reply-qualification-service returned ${response.status}`);
  }

  const qualifications = (await response.json()) as Qualification[];

  // Build map: email -> latest qualification (most recent createdAt wins)
  const map = new Map<string, Qualification>();
  for (const q of qualifications) {
    const existing = map.get(q.fromEmail);
    if (!existing || q.createdAt > existing.createdAt) {
      map.set(q.fromEmail, q);
    }
  }

  return map;
}

const POSITIVE_CLASSIFICATIONS = new Set(["willing_to_meet", "interested"]);
const NEGATIVE_CLASSIFICATIONS = new Set(["not_interested"]);

/**
 * Map a reply-qualification classification to a simplified type.
 * Returns the final outreach-relevant classification:
 * - "positive": willing_to_meet, interested
 * - "negative": not_interested
 * - "other": needs_more_info, out_of_office, unsubscribe, bounce, other
 */
export function classifyReply(classification: string): "positive" | "negative" | "other" {
  if (POSITIVE_CLASSIFICATIONS.has(classification)) return "positive";
  if (NEGATIVE_CLASSIFICATIONS.has(classification)) return "negative";
  return "other";
}
