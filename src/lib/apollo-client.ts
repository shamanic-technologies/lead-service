const APOLLO_SERVICE_URL = process.env.APOLLO_SERVICE_URL || "http://localhost:3003";
const APOLLO_SERVICE_API_KEY = process.env.APOLLO_SERVICE_API_KEY || "";

async function callApolloService<T>(
  path: string,
  options: { method?: string; body?: unknown } = {}
): Promise<T> {
  const { method = "GET", body } = options;

  const response = await fetch(`${APOLLO_SERVICE_URL}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": APOLLO_SERVICE_API_KEY,
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

export interface ApolloSearchResult {
  people: Array<{
    id: string;
    email: string;
    firstName?: string;
    lastName?: string;
    title?: string;
    linkedinUrl?: string;
    organizationName?: string;
    organizationDomain?: string;
    organizationIndustry?: string;
    organizationSize?: string;
  }>;
  pagination: {
    page: number;
    totalPages: number;
    totalEntries: number;
  };
}

export async function apolloSearch(
  params: ApolloSearchParams,
  page: number = 1
): Promise<ApolloSearchResult | null> {
  try {
    console.log(`[apollo-client] Searching page=${page} url=${APOLLO_SERVICE_URL}/search`);
    const result = await callApolloService<ApolloSearchResult>("/search", {
      method: "POST",
      body: { ...params, page },
    });
    console.log(`[apollo-client] Response: ${result.people.length} people, page ${result.pagination.page}/${result.pagination.totalPages} (total=${result.pagination.totalEntries})`);
    return result;
  } catch (error) {
    console.error("[apollo-client] Search failed:", error);
    return null;
  }
}
