const APOLLO_SERVICE_URL = process.env.APOLLO_SERVICE_URL || "http://localhost:3003";
const APOLLO_SERVICE_API_KEY = process.env.APOLLO_SERVICE_API_KEY || "";

async function callApolloService<T>(
  path: string,
  options: { method?: string; body?: unknown; headers?: Record<string, string> } = {}
): Promise<T> {
  const { method = "GET", body, headers: extraHeaders } = options;

  const response = await fetch(`${APOLLO_SERVICE_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": APOLLO_SERVICE_API_KEY,
      ...extraHeaders,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Apollo service call failed: ${response.status} - ${error}`);
  }

  return response.json() as Promise<T>;
}

export interface ApolloSearchParams {
  personTitles?: string[];
  organizationLocations?: string[];
  organizationIndustries?: string[];
  organizationSizeRanges?: string[];
  keywords?: string[];
  [key: string]: unknown;
}

export interface ApolloPersonResult {
  id: string;
  email?: string;
  emailStatus?: string;
  firstName?: string;
  lastName?: string;
  title?: string;
  linkedinUrl?: string;
  // Person details
  photoUrl?: string;
  headline?: string;
  city?: string;
  state?: string;
  country?: string;
  seniority?: string;
  departments?: string[];
  subdepartments?: string[];
  functions?: string[];
  twitterUrl?: string;
  githubUrl?: string;
  facebookUrl?: string;
  employmentHistory?: Array<{
    title?: string;
    organizationName?: string;
    startDate?: string;
    endDate?: string;
    description?: string;
    current?: boolean;
  }>;
  // Organization details
  organizationName?: string;
  organizationDomain?: string;
  organizationIndustry?: string;
  organizationSize?: string;
  organizationRevenueUsd?: string;
  organizationWebsiteUrl?: string;
  organizationLogoUrl?: string;
  organizationShortDescription?: string;
  organizationSeoDescription?: string;
  organizationLinkedinUrl?: string;
  organizationTwitterUrl?: string;
  organizationFacebookUrl?: string;
  organizationBlogUrl?: string;
  organizationCrunchbaseUrl?: string;
  organizationAngellistUrl?: string;
  organizationFoundedYear?: number;
  organizationPrimaryPhone?: string;
  organizationPubliclyTradedSymbol?: string;
  organizationPubliclyTradedExchange?: string;
  organizationAnnualRevenuePrinted?: string;
  organizationTotalFunding?: string;
  organizationTotalFundingPrinted?: string;
  organizationLatestFundingRoundDate?: string;
  organizationLatestFundingStage?: string;
  organizationFundingEvents?: Array<{
    id?: string;
    date?: string;
    type?: string;
    investors?: string;
    amount?: number;
    currency?: string;
  }>;
  organizationCity?: string;
  organizationState?: string;
  organizationCountry?: string;
  organizationStreetAddress?: string;
  organizationPostalCode?: string;
  organizationTechnologyNames?: string[];
  organizationCurrentTechnologies?: Array<{
    uid?: string;
    name?: string;
    category?: string;
  }>;
  organizationKeywords?: string[];
  organizationIndustries?: string[];
  organizationSecondaryIndustries?: string[];
  organizationNumSuborganizations?: number;
  organizationRetailLocationCount?: number;
  organizationAlexaRanking?: number;
  // Allow any additional fields Apollo adds in the future
  [key: string]: unknown;
}

export interface ApolloSearchResult {
  people: ApolloPersonResult[];
  pagination: {
    page: number;
    totalPages: number;
    totalEntries: number;
  };
}

interface ApolloSearchRawResponse {
  people?: ApolloPersonResult[];
  pagination?: ApolloSearchResult["pagination"];
  total_entries?: number;
  totalEntries?: number;
  per_page?: number;
  perPage?: number;
  [key: string]: unknown;
}

export async function apolloSearch(
  params: ApolloSearchParams,
  page: number = 1,
  options?: { runId?: string | null; clerkOrgId?: string | null; appId?: string; brandId?: string; campaignId?: string; workflowName?: string }
): Promise<ApolloSearchResult | null> {
  try {
    const headers: Record<string, string> = {};
    if (options?.clerkOrgId) headers["x-clerk-org-id"] = options.clerkOrgId;

    const raw = await callApolloService<ApolloSearchRawResponse>("/search", {
      method: "POST",
      body: {
        ...params,
        page,
        ...(options?.runId ? { runId: options.runId } : {}),
        ...(options?.appId ? { appId: options.appId } : {}),
        ...(options?.brandId ? { brandId: options.brandId } : {}),
        ...(options?.campaignId ? { campaignId: options.campaignId } : {}),
        ...(options?.workflowName ? { workflowName: options.workflowName } : {}),
      },
      headers,
    });

    const people = raw.people ?? [];
    const pagination = raw.pagination ?? {
      page,
      totalEntries: raw.total_entries ?? raw.totalEntries ?? 0,
      totalPages: Math.ceil((raw.total_entries ?? raw.totalEntries ?? 0) / (raw.per_page ?? raw.perPage ?? 25)),
    };

    const result: ApolloSearchResult = { people, pagination };
    return result;
  } catch (error) {
    console.error("[apollo-client] Search failed:", error);
    return null;
  }
}

// --- Search Next (server-managed pagination) ---

export interface ApolloSearchNextResult {
  people: ApolloPersonResult[];
  done: boolean;
  totalEntries: number;
}

export async function apolloSearchNext(options: {
  campaignId: string;
  brandId: string;
  appId: string;
  searchParams?: ApolloSearchParams;
  runId?: string | null;
  clerkOrgId?: string | null;
  workflowName?: string;
}): Promise<ApolloSearchNextResult | null> {
  try {
    const headers: Record<string, string> = {};
    if (options.clerkOrgId) headers["x-clerk-org-id"] = options.clerkOrgId;

    const body: Record<string, unknown> = {
      campaignId: options.campaignId,
      brandId: options.brandId,
      appId: options.appId,
    };
    if (options.searchParams) body.searchParams = options.searchParams;
    if (options.runId) body.runId = options.runId;
    if (options.workflowName) body.workflowName = options.workflowName;

    return await callApolloService<ApolloSearchNextResult>("/search/next", {
      method: "POST",
      body,
      headers,
    });
  } catch (error) {
    console.error("[apollo-client] SearchNext failed:", error);
    return null;
  }
}

// --- Search Params (LLM-powered search filter generation) ---

export interface ApolloSearchParamsResult {
  searchParams: ApolloSearchParams;
  totalResults: number;
  attempts: number;
}

export async function apolloSearchParams(options: {
  context: string;
  keySource: "byok" | "app";
  runId: string;
  appId: string;
  brandId: string;
  campaignId: string;
  clerkOrgId?: string | null;
  workflowName?: string;
}): Promise<ApolloSearchParamsResult> {
  const headers: Record<string, string> = {};
  if (options.clerkOrgId) headers["x-clerk-org-id"] = options.clerkOrgId;

  return callApolloService<ApolloSearchParamsResult>("/search/params", {
    method: "POST",
    body: {
      context: options.context,
      keySource: options.keySource,
      runId: options.runId,
      appId: options.appId,
      brandId: options.brandId,
      campaignId: options.campaignId,
      ...(options.workflowName ? { workflowName: options.workflowName } : {}),
    },
    headers,
  });
}

// --- Stats ---

export interface ApolloStats {
  enrichedLeadsCount: number;
  searchCount: number;
  fetchedPeopleCount: number;
  totalMatchingPeople: number;
}

export async function fetchApolloStats(
  filters: { runIds?: string[]; appId?: string; brandId?: string; campaignId?: string },
  clerkOrgId?: string | null
): Promise<ApolloStats> {
  try {
    const headers: Record<string, string> = {};
    if (clerkOrgId) headers["x-clerk-org-id"] = clerkOrgId;

    const result = await callApolloService<{ stats: ApolloStats }>("/stats", {
      method: "POST",
      body: filters,
      headers,
    });

    return result.stats;
  } catch (error) {
    console.error("[apollo-client] Stats fetch failed:", error);
    return { enrichedLeadsCount: 0, searchCount: 0, fetchedPeopleCount: 0, totalMatchingPeople: 0 };
  }
}

// --- Enrichment ---

export interface ApolloEnrichResult {
  person: ApolloPersonResult;
}

export async function apolloEnrich(
  personId: string,
  options?: { runId?: string | null; clerkOrgId?: string | null; appId?: string; brandId?: string; campaignId?: string; workflowName?: string }
): Promise<ApolloEnrichResult | null> {
  try {
    const headers: Record<string, string> = {};
    if (options?.clerkOrgId) headers["x-clerk-org-id"] = options.clerkOrgId;

    const result = await callApolloService<ApolloEnrichResult>("/enrich", {
      method: "POST",
      body: {
        apolloPersonId: personId,
        ...(options?.runId ? { runId: options.runId } : {}),
        ...(options?.appId ? { appId: options.appId } : {}),
        ...(options?.brandId ? { brandId: options.brandId } : {}),
        ...(options?.campaignId ? { campaignId: options.campaignId } : {}),
        ...(options?.workflowName ? { workflowName: options.workflowName } : {}),
      },
      headers,
    });

    return result;
  } catch (error) {
    console.error(`[apollo-client] Enrich failed for personId=${personId}:`, error);
    return null;
  }
}
