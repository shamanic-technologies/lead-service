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
  emails: JournalistEmail[];
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

    const url = new URL(
      `${JOURNALISTS_SERVICE_URL}/internal/journalists/by-outlet-with-emails/${outletId}`
    );
    if (options?.campaignId) url.searchParams.set("campaignId", options.campaignId);

    const response = await fetch(url.toString(), { headers });

    if (!response.ok) {
      console.warn(`[journalist-client] Failed to fetch journalists for outlet ${outletId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { journalists: JournalistWithEmails[] };
    return data.journalists;
  } catch (error) {
    console.error("[journalist-client] Error fetching journalists:", error);
    return null;
  }
}
