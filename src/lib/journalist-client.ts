import { JOURNALISTS_SERVICE_URL, JOURNALISTS_SERVICE_API_KEY } from "../config.js";

export interface JournalistEmail {
  email: string;
  isValid: boolean;
  confidence: number;
}

export interface JournalistWithEmails {
  id: string;
  journalistName: string;
  firstName: string | null;
  lastName: string | null;
  entityType: "individual" | "organization";
  relevanceScore: number;
  whyRelevant: string;
  whyNotRelevant: string;
  emails: JournalistEmail[];
}

export async function fetchNextJournalist(
  outletId: string,
  options?: {
    campaignId?: string;
    orgId?: string | null;
    userId?: string;
    runId?: string;
    brandId?: string;
    workflowSlug?: string;
    featureSlug?: string;
    idempotencyKey?: string;
    maxArticles?: number;
  }
): Promise<{ found: boolean; journalist?: JournalistWithEmails }> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": JOURNALISTS_SERVICE_API_KEY,
    };
    if (options?.orgId) headers["x-org-id"] = options.orgId;
    if (options?.userId) headers["x-user-id"] = options.userId;
    if (options?.runId) headers["x-run-id"] = options.runId;
    if (options?.campaignId) headers["x-campaign-id"] = options.campaignId;
    if (options?.brandId) headers["x-brand-id"] = options.brandId;
    if (options?.workflowSlug) headers["x-workflow-slug"] = options.workflowSlug;
    if (options?.featureSlug) headers["x-feature-slug"] = options.featureSlug;

    const body: Record<string, unknown> = { outletId };
    if (options?.idempotencyKey) body.idempotencyKey = options.idempotencyKey;
    if (options?.maxArticles != null) body.maxArticles = options.maxArticles;

    const response = await fetch(`${JOURNALISTS_SERVICE_URL}/buffer/next`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      console.warn(`[journalist-client] buffer/next failed for outlet ${outletId}: ${response.status}`);
      return { found: false };
    }

    const data = (await response.json()) as { found: boolean; journalist?: JournalistWithEmails };
    return data;
  } catch (error) {
    console.error("[journalist-client] Error fetching next journalist:", error);
    return { found: false };
  }
}
