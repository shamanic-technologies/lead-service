import Anthropic from "@anthropic-ai/sdk";
import {
  validateSearchParams,
  fetchIndustries,
  fetchEmployeeRanges,
  type ApolloSearchParams,
  type ValidationResult,
} from "./apollo-client.js";
import { addCosts } from "./runs-client.js";

const MAX_RETRIES = 3;
const CACHE_TTL = 6 * 30 * 24 * 60 * 60 * 1000; // ~6 months

// In-memory cache: hash(raw input) → validated ApolloSearchParams
const transformCache = new Map<string, { params: ApolloSearchParams; cachedAt: number }>();

function cacheKey(input: Record<string, unknown>): string {
  return JSON.stringify(input, Object.keys(input).sort());
}

function getCached(key: string): ApolloSearchParams | null {
  const entry = transformCache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.cachedAt > CACHE_TTL) {
    transformCache.delete(key);
    return null;
  }
  return entry.params;
}

function buildSystemPrompt(
  industries: Array<{ id: string; name: string }>,
  employeeRanges: Array<{ value: string; label: string }>
): string {
  return `You transform search parameters into Apollo API search format.

Output ONLY valid JSON matching ApolloSearchParams. No explanation, no markdown.

ApolloSearchParams fields:
- personTitles: string[] — job titles (e.g. ["VP Sales", "Head of Marketing"])
- organizationLocations: string[] — locations (e.g. ["San Francisco, California, United States"])
- organizationIndustries: string[] — industry tag IDs from the list below
- organizationNumEmployeesRanges: string[] — exact enum values from the list below
- keywords: string[] — additional search keywords

Valid employee ranges:
${employeeRanges.map((r) => `- "${r.value}" (${r.label})`).join("\n")}

Valid industry IDs (use the id, not the name):
${industries.map((i) => `- "${i.id}" (${i.name})`).join("\n")}

Rules:
- Only include fields that are relevant to the input
- Use exact enum values for employee ranges
- Use industry IDs (not names) for organizationIndustries
- Output raw JSON only, no wrapping`;
}

function buildRetryPrompt(
  previousAttempt: string,
  errors: ValidationResult["errors"]
): string {
  const errorLines = errors
    .map((e) => `- Field "${e.field}": ${e.message}${e.value ? ` (got: ${JSON.stringify(e.value)})` : ""}`)
    .join("\n");
  return `Your previous output was invalid:

${previousAttempt}

Validation errors:
${errorLines}

Fix these errors and output corrected JSON only.`;
}

let anthropicClient: Anthropic | null = null;

function getClient(): Anthropic {
  if (!anthropicClient) {
    anthropicClient = new Anthropic();
  }
  return anthropicClient;
}

export async function transformSearchParams(
  rawParams: Record<string, unknown>,
  clerkOrgId?: string | null,
  runId?: string | null
): Promise<ApolloSearchParams> {
  // Check cache first
  const key = cacheKey(rawParams);
  const cached = getCached(key);
  if (cached) {
    console.log("[Lead Service][search-transform] Cache hit");
    return cached;
  }

  // Fetch reference data for the LLM prompt
  const [industries, employeeRanges] = await Promise.all([
    fetchIndustries(clerkOrgId),
    fetchEmployeeRanges(clerkOrgId),
  ]);

  const systemPrompt = buildSystemPrompt(industries, employeeRanges);
  const client = getClient();
  let lastAttempt = "";
  let lastErrors: ValidationResult["errors"] = [];
  let totalInputTokens = 0;
  let totalOutputTokens = 0;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const userMessage =
      attempt === 1
        ? `Transform this into ApolloSearchParams:\n${JSON.stringify(rawParams, null, 2)}`
        : buildRetryPrompt(lastAttempt, lastErrors);

    const response = await client.messages.create({
      model: "claude-opus-4-5",
      max_tokens: 1024,
      system: systemPrompt,
      messages: [{ role: "user", content: userMessage }],
    });

    totalInputTokens += response.usage.input_tokens;
    totalOutputTokens += response.usage.output_tokens;

    const text = response.content[0].type === "text" ? response.content[0].text : "";
    lastAttempt = text.trim();

    let parsed: ApolloSearchParams;
    try {
      parsed = JSON.parse(lastAttempt);
    } catch {
      console.error(`[Lead Service][search-transform] Attempt ${attempt}: invalid JSON`);
      lastErrors = [{ field: "root", message: "Response was not valid JSON" }];
      continue;
    }

    // Validate via Apollo
    const validation = await validateSearchParams(parsed, clerkOrgId);

    if (validation.valid) {
      console.log(`[Lead Service][search-transform] Valid on attempt ${attempt}`);
      transformCache.set(key, { params: parsed, cachedAt: Date.now() });
      await logCosts(runId, totalInputTokens, totalOutputTokens);
      return parsed;
    }

    console.warn(`[Lead Service][search-transform] Attempt ${attempt} invalid:`, validation.errors);
    lastErrors = validation.errors;
  }

  await logCosts(runId, totalInputTokens, totalOutputTokens);
  throw new Error(
    `Failed to transform search params after ${MAX_RETRIES} attempts. Last errors: ${JSON.stringify(lastErrors)}`
  );
}

async function logCosts(
  runId: string | null | undefined,
  inputTokens: number,
  outputTokens: number
): Promise<void> {
  if (!runId || (inputTokens === 0 && outputTokens === 0)) return;
  try {
    await addCosts(runId, [
      { costName: "anthropic-opus-4.5-tokens-input", quantity: inputTokens },
      { costName: "anthropic-opus-4.5-tokens-output", quantity: outputTokens },
    ]);
  } catch (err) {
    console.error("[Lead Service][search-transform] Failed to log costs:", err);
  }
}
