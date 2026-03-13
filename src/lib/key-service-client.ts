const KEY_SERVICE_URL = process.env.KEY_SERVICE_URL || "http://localhost:3001";
const KEY_SERVICE_API_KEY = process.env.KEY_SERVICE_API_KEY || "";

export interface ProviderRequirement {
  service: string;
  method: string;
  path: string;
  provider: string;
}

export interface ProviderRequirementsResponse {
  requirements: ProviderRequirement[];
  providers: string[];
}

export async function queryProviderRequirements(
  endpoints: Array<{ service: string; method: string; path: string }>
): Promise<ProviderRequirementsResponse> {
  const response = await fetch(`${KEY_SERVICE_URL}/provider-requirements`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": KEY_SERVICE_API_KEY,
    },
    body: JSON.stringify({ endpoints }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Key service provider-requirements failed: ${response.status} - ${error}`);
  }

  return response.json() as Promise<ProviderRequirementsResponse>;
}

export async function registerProviderRequirement(
  provider: string,
  callerService: string,
  callerMethod: string,
  callerPath: string
): Promise<void> {
  const response = await fetch(`${KEY_SERVICE_URL}/keys/platform/${provider}/decrypt`, {
    method: "GET",
    headers: {
      "X-API-Key": KEY_SERVICE_API_KEY,
      "x-caller-service": callerService,
      "x-caller-method": callerMethod,
      "x-caller-path": callerPath,
    },
  });

  // We only care about the side effect (provider requirement tracking).
  // 404 (key not configured) is fine — the requirement is still recorded.
  if (!response.ok && response.status !== 404) {
    const error = await response.text();
    throw new Error(`Key service registration failed: ${response.status} - ${error}`);
  }
}
