const JOURNALISTS_SERVICE_URL = process.env.JOURNALISTS_SERVICE_URL || "http://localhost:3011";
const JOURNALISTS_SERVICE_API_KEY = process.env.JOURNALISTS_SERVICE_API_KEY || "";

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

export interface DiscoverEmailsResult {
  journalistId: string;
  email: string | null;
  emailStatus: string | null;
  cached: boolean;
  enrichmentId: string;
}

export async function discoverJournalistEmails(params: {
  outletId: string;
  organizationDomain: string;
  brandId: string;
  campaignId: string;
  journalistIds?: string[];
}, context?: {
  orgId?: string | null;
  userId?: string;
  runId?: string;
  workflowName?: string;
  featureSlug?: string;
}): Promise<DiscoverEmailsResult[] | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": JOURNALISTS_SERVICE_API_KEY,
    };
    if (context?.orgId) headers["x-org-id"] = context.orgId;
    if (context?.userId) headers["x-user-id"] = context.userId;
    if (context?.runId) headers["x-run-id"] = context.runId;
    if (params.campaignId) headers["x-campaign-id"] = params.campaignId;
    if (params.brandId) headers["x-brand-id"] = params.brandId;
    if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;
    if (context?.featureSlug) headers["x-feature-slug"] = context.featureSlug;

    const body: Record<string, unknown> = {
      outletId: params.outletId,
      organizationDomain: params.organizationDomain,
      brandId: params.brandId,
      campaignId: params.campaignId,
    };
    if (params.journalistIds) body.journalistIds = params.journalistIds;

    const response = await fetch(`${JOURNALISTS_SERVICE_URL}/journalists/discover-emails`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      console.warn(`[journalist-client] Failed to discover emails for outlet ${params.outletId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { discovered: number; total: number; skipped: number; results: DiscoverEmailsResult[] };
    console.log(`[journalist-client] Discovered ${data.discovered}/${data.total} emails for outlet ${params.outletId} (skipped=${data.skipped})`);
    return data.results;
  } catch (error) {
    console.error("[journalist-client] Error discovering emails:", error);
    return null;
  }
}

export async function fetchJournalistsByOutlet(
  outletId: string,
  options?: {
    campaignId?: string;
    orgId?: string | null;
    userId?: string;
    runId?: string;
    brandId?: string;
    workflowName?: string;
    featureSlug?: string;
  }
): Promise<JournalistWithEmails[] | null> {
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
    if (options?.workflowName) headers["x-workflow-name"] = options.workflowName;
    if (options?.featureSlug) headers["x-feature-slug"] = options.featureSlug;

    const response = await fetch(`${JOURNALISTS_SERVICE_URL}/journalists/resolve`, {
      method: "POST",
      headers,
      body: JSON.stringify({ outletId }),
    });

    if (!response.ok) {
      console.warn(`[journalist-client] Failed to resolve journalists for outlet ${outletId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { journalists: JournalistWithEmails[]; cached: boolean };
    if (data.cached) {
      console.log(`[journalist-client] Resolved journalists for outlet ${outletId} (cached)`);
    }
    return data.journalists;
  } catch (error) {
    console.error("[journalist-client] Error resolving journalists:", error);
    return null;
  }
}
