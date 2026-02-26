const BRAND_SERVICE_URL = process.env.BRAND_SERVICE_URL || "http://localhost:3005";
const BRAND_SERVICE_API_KEY = process.env.BRAND_SERVICE_API_KEY || "";

export interface BrandDetails {
  id: string;
  name: string | null;
  domain: string | null;
  elevatorPitch: string | null;
  bio: string | null;
  mission: string | null;
  location: string | null;
  categories: string | null;
}

export async function fetchBrand(
  brandId: string,
  orgId?: string | null
): Promise<BrandDetails | null> {
  try {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "X-API-Key": BRAND_SERVICE_API_KEY,
    };
    // TODO: rename header + query param when brand-service is migrated
    if (orgId) headers["x-clerk-org-id"] = orgId;

    const url = new URL(`${BRAND_SERVICE_URL}/brands/${brandId}`);
    if (orgId) url.searchParams.set("clerkOrgId", orgId);

    const response = await fetch(url.toString(), { headers });

    if (!response.ok) {
      console.warn(`[brand-client] Failed to fetch brand ${brandId}: ${response.status}`);
      return null;
    }

    const data = (await response.json()) as { brand: BrandDetails };
    return data.brand;
  } catch (error) {
    console.error("[brand-client] Error fetching brand:", error);
    return null;
  }
}
