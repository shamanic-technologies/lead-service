import { KEY_SERVICE_URL, KEY_SERVICE_API_KEY } from "../config.js";

function getKeyServiceConfig() {
  return { url: KEY_SERVICE_URL, apiKey: KEY_SERVICE_API_KEY };
}

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
  const { url, apiKey } = getKeyServiceConfig();
  const response = await fetch(`${url}/provider-requirements`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKey,
    },
    body: JSON.stringify({ endpoints }),
    signal: AbortSignal.timeout(300_000),
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
  const { url, apiKey } = getKeyServiceConfig();
  const response = await fetch(`${url}/keys/platform/${provider}/decrypt`, {
    method: "GET",
    headers: {
      "X-API-Key": apiKey,
      "x-caller-service": callerService,
      "x-caller-method": callerMethod,
      "x-caller-path": callerPath,
    },
    signal: AbortSignal.timeout(300_000),
  });

  // We only care about the side effect (provider requirement tracking).
  // 404 (key not configured) is fine — the requirement is still recorded.
  if (!response.ok && response.status !== 404) {
    const error = await response.text();
    throw new Error(`Key service registration failed: ${response.status} - ${error}`);
  }
}
