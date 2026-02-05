const APOLLO_SERVICE_URL = process.env.APOLLO_SERVICE_URL || "http://localhost:3003";
const APOLLO_SERVICE_API_KEY = process.env.APOLLO_SERVICE_API_KEY || "";

export interface ApolloEnrichResponse {
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
}

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

export async function apolloEnrich(email: string): Promise<ApolloEnrichResponse | null> {
  try {
    return await callApolloService<ApolloEnrichResponse>("/enrich", {
      method: "POST",
      body: { email },
    });
  } catch (error) {
    console.error("[apollo-client] Enrich failed:", error);
    return null;
  }
}
