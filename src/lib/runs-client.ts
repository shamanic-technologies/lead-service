const RUNS_SERVICE_URL = process.env.RUNS_SERVICE_URL || "https://runs.mcpfactory.org";
const RUNS_SERVICE_API_KEY = process.env.RUNS_SERVICE_API_KEY || "";

async function callRunsService(path: string, options: {
  method?: string;
  body?: unknown;
} = {}): Promise<unknown> {
  const { method = "GET", body } = options;

  const response = await fetch(`${RUNS_SERVICE_URL}/v1${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": RUNS_SERVICE_API_KEY,
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
  clerkOrgId: string;
  appId: string;
  serviceName: string;
  taskName: string;
  parentRunId?: string;
  clerkUserId?: string;
  brandId?: string;
  campaignId?: string;
  workflowName?: string;
}): Promise<{ id: string }> {
  return callRunsService("/runs", {
    method: "POST",
    body: params,
  }) as Promise<{ id: string }>;
}

export async function updateRun(
  runId: string,
  status: "completed" | "failed"
): Promise<void> {
  await callRunsService(`/runs/${runId}`, {
    method: "PATCH",
    body: { status },
  });
}

export async function addCosts(
  runId: string,
  items: Array<{ costName: string; quantity: number }>
): Promise<void> {
  if (items.length === 0) return;
  await callRunsService(`/runs/${runId}/costs`, {
    method: "POST",
    body: { items },
  });
}
