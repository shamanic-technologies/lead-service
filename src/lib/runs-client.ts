const RUNS_SERVICE_URL = process.env.RUNS_SERVICE_URL || "https://runs.distribute.org";
const RUNS_SERVICE_API_KEY = process.env.RUNS_SERVICE_API_KEY || "";

async function callRunsService(path: string, options: {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
} = {}): Promise<unknown> {
  const { method = "GET", body, headers: extraHeaders } = options;

  const response = await fetch(`${RUNS_SERVICE_URL}/v1${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": RUNS_SERVICE_API_KEY,
      ...extraHeaders,
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Runs service call failed: ${response.status} - ${error}`);
  }

  return response.json();
}

export async function createRun(params: {
  orgId: string;
  serviceName: string;
  taskName: string;
  parentRunId?: string;
  userId?: string;
  brandId?: string;
  campaignId?: string;
  workflowName?: string;
}): Promise<{ id: string }> {
  const headers: Record<string, string> = {
    "x-org-id": params.orgId,
  };
  if (params.userId) headers["x-user-id"] = params.userId;
  if (params.parentRunId) headers["x-run-id"] = params.parentRunId;
  if (params.campaignId) headers["x-campaign-id"] = params.campaignId;
  if (params.brandId) headers["x-brand-id"] = params.brandId;
  if (params.workflowName) headers["x-workflow-name"] = params.workflowName;

  return callRunsService("/runs", {
    method: "POST",
    body: {
      serviceName: params.serviceName,
      taskName: params.taskName,
      brandId: params.brandId,
      campaignId: params.campaignId,
      workflowName: params.workflowName,
    },
    headers,
  }) as Promise<{ id: string }>;
}

export async function updateRun(
  runId: string,
  status: "completed" | "failed",
  context?: { orgId?: string; userId?: string; campaignId?: string; brandId?: string; workflowName?: string }
): Promise<void> {
  const headers: Record<string, string> = {};
  if (context?.orgId) headers["x-org-id"] = context.orgId;
  if (context?.userId) headers["x-user-id"] = context.userId;
  headers["x-run-id"] = runId;
  if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
  if (context?.brandId) headers["x-brand-id"] = context.brandId;
  if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;

  await callRunsService(`/runs/${runId}`, {
    method: "PATCH",
    body: { status },
    headers,
  });
}

export async function addCosts(
  runId: string,
  items: Array<{ costName: string; quantity: number; costSource: "platform" | "org" }>,
  context?: { orgId?: string; userId?: string; campaignId?: string; brandId?: string; workflowName?: string }
): Promise<void> {
  if (items.length === 0) return;

  const headers: Record<string, string> = {};
  if (context?.orgId) headers["x-org-id"] = context.orgId;
  if (context?.userId) headers["x-user-id"] = context.userId;
  headers["x-run-id"] = runId;
  if (context?.campaignId) headers["x-campaign-id"] = context.campaignId;
  if (context?.brandId) headers["x-brand-id"] = context.brandId;
  if (context?.workflowName) headers["x-workflow-name"] = context.workflowName;

  await callRunsService(`/runs/${runId}/costs`, {
    method: "POST",
    body: { items },
    headers,
  });
}
