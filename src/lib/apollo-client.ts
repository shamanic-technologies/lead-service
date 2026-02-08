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
  firstName?: string;
  lastName?: string;
  title?: string;
  linkedinUrl?: string;
  organizationName?: string;
  organizationDomain?: string;
  organizationIndustry?: string;
  organizationSize?: string;
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
  options?: { runId?: string | null; clerkOrgId?: string | null; appId?: string; brandId?: string; campaignId?: string }
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

// --- Validation ---

export interface ValidationError {
  field: string;
  message: string;
  value?: unknown;
}

export interface ValidationResult {
  index: number;
  valid: boolean;
  endpoint: string;
  errors: ValidationError[];
}

export interface ValidationResponse {
  results: ValidationResult[];
}

export async function validateSearchParams(
  params: Record<string, unknown>,
  clerkOrgId?: string | null
): Promise<ValidationResult> {
  const headers: Record<string, string> = {};
  if (clerkOrgId) headers["x-clerk-org-id"] = clerkOrgId;

  const response = await callApolloService<ValidationResponse>("/validate", {
    method: "POST",
    body: { endpoint: "search", items: [params] },
    headers,
  });

  return response.results[0];
}

// --- Reference Data ---

export interface ApolloIndustry {
  id: string;
  name: string;
  [key: string]: unknown;
}

export interface ApolloEmployeeRange {
  value: string;
  label: string;
  [key: string]: unknown;
}

let industriesCache: { data: ApolloIndustry[]; fetchedAt: number } | null = null;
let employeeRangesCache: { data: ApolloEmployeeRange[]; fetchedAt: number } | null = null;
const REFERENCE_CACHE_TTL = 24 * 60 * 60 * 1000; // 24h

export async function fetchIndustries(clerkOrgId?: string | null): Promise<ApolloIndustry[]> {
  if (industriesCache && Date.now() - industriesCache.fetchedAt < REFERENCE_CACHE_TTL) {
    return industriesCache.data;
  }
  const headers: Record<string, string> = {};
  if (clerkOrgId) headers["x-clerk-org-id"] = clerkOrgId;

  const raw = await callApolloService<unknown>("/reference/industries", { headers });
  const unwrapped = Array.isArray(raw) ? raw : (raw as Record<string, unknown>)?.industries;
  const data = (Array.isArray(unwrapped) ? unwrapped : []) as ApolloIndustry[];
  industriesCache = { data, fetchedAt: Date.now() };
  return data;
}

export async function fetchEmployeeRanges(clerkOrgId?: string | null): Promise<ApolloEmployeeRange[]> {
  if (employeeRangesCache && Date.now() - employeeRangesCache.fetchedAt < REFERENCE_CACHE_TTL) {
    return employeeRangesCache.data;
  }
  const headers: Record<string, string> = {};
  if (clerkOrgId) headers["x-clerk-org-id"] = clerkOrgId;

  const raw = await callApolloService<unknown>("/reference/employee-ranges", { headers });
  const unwrapped = Array.isArray(raw) ? raw : (raw as Record<string, unknown>)?.ranges;
  const data = (Array.isArray(unwrapped) ? unwrapped : []) as ApolloEmployeeRange[];
  employeeRangesCache = { data, fetchedAt: Date.now() };
  return data;
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
  options?: { runId?: string | null; clerkOrgId?: string | null; appId?: string; brandId?: string; campaignId?: string }
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
      },
      headers,
    });

    return result;
  } catch (error) {
    console.error(`[apollo-client] Enrich failed for personId=${personId}:`, error);
    return null;
  }
}
