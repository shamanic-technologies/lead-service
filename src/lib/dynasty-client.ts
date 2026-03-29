import {
  FEATURES_SERVICE_URL,
  FEATURES_SERVICE_API_KEY,
  WORKFLOW_SERVICE_URL,
  WORKFLOW_SERVICE_API_KEY,
} from "../config.js";

interface DynastyEntry {
  dynastySlug: string;
  slugs: string[];
}

function buildHeaders(
  apiKey: string,
  context?: { orgId?: string; userId?: string; runId?: string },
): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": apiKey,
  };
  if (context?.orgId) headers["x-org-id"] = context.orgId;
  if (context?.userId) headers["x-user-id"] = context.userId;
  if (context?.runId) headers["x-run-id"] = context.runId;
  return headers;
}

/**
 * Resolve a feature dynasty slug to its list of versioned slugs.
 * Returns empty array if resolution fails or dynasty doesn't exist.
 */
export async function resolveFeatureDynastySlugs(
  dynastySlug: string,
  context?: { orgId?: string; userId?: string; runId?: string },
): Promise<string[]> {
  try {
    const url = `${FEATURES_SERVICE_URL}/features/dynasty/slugs?dynastySlug=${encodeURIComponent(dynastySlug)}`;
    const response = await fetch(url, {
      headers: buildHeaders(FEATURES_SERVICE_API_KEY, context),
    });
    if (!response.ok) {
      console.warn(`[lead-service] Failed to resolve feature dynasty slug ${dynastySlug}: ${response.status}`);
      return [];
    }
    const data = (await response.json()) as { slugs: string[] };
    return data.slugs ?? [];
  } catch (error) {
    console.error("[lead-service] Error resolving feature dynasty slug:", error);
    return [];
  }
}

/**
 * Resolve a workflow dynasty slug to its list of versioned slugs.
 * Returns empty array if resolution fails or dynasty doesn't exist.
 */
export async function resolveWorkflowDynastySlugs(
  dynastySlug: string,
  context?: { orgId?: string; userId?: string; runId?: string },
): Promise<string[]> {
  try {
    const url = `${WORKFLOW_SERVICE_URL}/workflows/dynasty/slugs?dynastySlug=${encodeURIComponent(dynastySlug)}`;
    const response = await fetch(url, {
      headers: buildHeaders(WORKFLOW_SERVICE_API_KEY, context),
    });
    if (!response.ok) {
      console.warn(`[lead-service] Failed to resolve workflow dynasty slug ${dynastySlug}: ${response.status}`);
      return [];
    }
    const data = (await response.json()) as { slugs: string[] };
    return data.slugs ?? [];
  } catch (error) {
    console.error("[lead-service] Error resolving workflow dynasty slug:", error);
    return [];
  }
}

/**
 * Fetch all feature dynasties and build a reverse map: slug → dynastySlug.
 */
export async function fetchFeatureDynastyMap(
  context?: { orgId?: string; userId?: string; runId?: string },
): Promise<Map<string, string>> {
  try {
    const url = `${FEATURES_SERVICE_URL}/features/dynasties`;
    const response = await fetch(url, {
      headers: buildHeaders(FEATURES_SERVICE_API_KEY, context),
    });
    if (!response.ok) {
      console.warn(`[lead-service] Failed to fetch feature dynasties: ${response.status}`);
      return new Map();
    }
    const data = (await response.json()) as { dynasties: DynastyEntry[] };
    return buildSlugToDynastyMap(data.dynasties ?? []);
  } catch (error) {
    console.error("[lead-service] Error fetching feature dynasties:", error);
    return new Map();
  }
}

/**
 * Fetch all workflow dynasties and build a reverse map: slug → dynastySlug.
 */
export async function fetchWorkflowDynastyMap(
  context?: { orgId?: string; userId?: string; runId?: string },
): Promise<Map<string, string>> {
  try {
    const url = `${WORKFLOW_SERVICE_URL}/workflows/dynasties`;
    const response = await fetch(url, {
      headers: buildHeaders(WORKFLOW_SERVICE_API_KEY, context),
    });
    if (!response.ok) {
      console.warn(`[lead-service] Failed to fetch workflow dynasties: ${response.status}`);
      return new Map();
    }
    const data = (await response.json()) as { dynasties: DynastyEntry[] };
    return buildSlugToDynastyMap(data.dynasties ?? []);
  } catch (error) {
    console.error("[lead-service] Error fetching workflow dynasties:", error);
    return new Map();
  }
}

function buildSlugToDynastyMap(
  dynasties: DynastyEntry[],
): Map<string, string> {
  const map = new Map<string, string>();
  for (const d of dynasties) {
    for (const slug of d.slugs) {
      map.set(slug, d.dynastySlug);
    }
  }
  return map;
}
