export interface AuthorizeCreditsResult {
  sufficient: boolean;
  balance_cents: number;
}

export async function authorizeCredits(params: {
  requiredCents: number;
  description: string;
  orgId: string;
  userId: string;
  runId: string;
  campaignId?: string;
  brandId?: string;
  workflowName?: string;
}): Promise<AuthorizeCreditsResult> {
  const billingUrl = process.env.BILLING_SERVICE_URL || "";
  const billingApiKey = process.env.BILLING_SERVICE_API_KEY || "";

  if (!billingUrl) {
    throw new Error("BILLING_SERVICE_URL not configured");
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-API-Key": billingApiKey,
    "x-org-id": params.orgId,
    "x-user-id": params.userId,
    "x-run-id": params.runId,
  };
  if (params.campaignId) headers["x-campaign-id"] = params.campaignId;
  if (params.brandId) headers["x-brand-id"] = params.brandId;
  if (params.workflowName) headers["x-workflow-name"] = params.workflowName;

  const response = await fetch(`${billingUrl}/v1/credits/authorize`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      required_cents: params.requiredCents,
      description: params.description,
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Billing service call failed: ${response.status} - ${error}`);
  }

  return response.json() as Promise<AuthorizeCreditsResult>;
}
