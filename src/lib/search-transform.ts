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

export function buildSystemPrompt(
  industries: Array<{ id: string; name: string }>,
  employeeRanges: Array<{ value: string; label: string }>
): string {
  return `You transform search parameters into Apollo API search format.

Output ONLY valid JSON matching the Apollo search schema. No explanation, no markdown.

## Available fields

### Person filters
- personTitles: string[] — job titles (e.g. ["VP Sales", "Head of Marketing"])
- personLocations: string[] — person's own location (e.g. ["San Francisco, California, United States"])
- personSeniorities: string[] — seniority levels. Valid values: "entry", "senior", "manager", "director", "vp", "c_suite", "owner", "founder", "partner"
- contactEmailStatus: string[] — filter by email status. Valid values: "verified", "guessed", "unavailable", "bounced", "pending_manual_fulfillment"

### Organization filters
- organizationLocations: string[] — organization HQ location (e.g. ["California, US", "New York, US"])
- qOrganizationIndustryTagIds: string[] — industry names from the valid list below
- organizationNumEmployeesRanges: string[] — exact enum values from the list below
- qOrganizationKeywordTags: string[] — keyword tags describing the organization (e.g. ["SaaS", "fintech"])
- qOrganizationDomains: string[] — specific company domains (e.g. ["google.com", "stripe.com"])
- organizationIds: string[] — specific Apollo organization IDs
- revenueRange: string[] — company revenue ranges, same format as employee ranges (e.g. ["1000000,10000000"])
- currentlyUsingAnyOfTechnologyUids: string[] — Apollo technology UIDs for tech stack filtering

### General
- qKeywords: string — free-text keyword search across all person and organization fields

## CRITICAL: How filters combine

- BETWEEN different fields: **AND** — every field you include narrows the results further
  personTitles AND qOrganizationIndustryTagIds AND qKeywords = must match ALL
- WITHIN a single field: **OR** — values are alternatives
  personTitles: ["CEO", "CTO", "Founder"] = matches CEO OR CTO OR Founder

## Strategy for effective queries

1. **Start broad** — use 1-2 filters maximum. Each additional filter drastically reduces results.
2. **Use personTitles broadly** — include many title variations and seniority levels (e.g. ["CEO", "Founder", "Managing Director", "Head of Operations", "COO"])
3. **Prefer qKeywords for niche topics** — instead of combining qOrganizationKeywordTags + qOrganizationIndustryTagIds + qKeywords (3 AND'd filters), use a single broad qKeywords with OR syntax: "blockchain OR web3 OR crypto"
4. **Do NOT combine qOrganizationKeywordTags with qOrganizationIndustryTagIds** — these overlap in meaning and AND'ing them often gives 0 results. Pick the one that best matches the intent.
5. **organizationLocations is expensive** — only include when location is explicitly required by the user.

## BAD example (too many AND'd filters → 0 results):
{
  "personTitles": ["Executive Director", "Community Manager"],
  "qOrganizationKeywordTags": ["community", "blockchain", "web3"],
  "qOrganizationIndustryTagIds": ["Non-Profit Organization Management"],
  "qKeywords": "blockchain OR web3 OR ambassador"
}
Problem: 4 filters AND'd together. Nonprofits + blockchain keywords + blockchain industry + those exact titles = empty intersection.

## GOOD example (broad, effective):
{
  "personTitles": ["Executive Director", "Program Director", "Community Manager", "Community Director", "Outreach Director", "Engagement Manager", "Head of Community", "VP Community"],
  "qKeywords": "blockchain OR web3 OR crypto OR decentralized"
}
Why it works: Only 2 AND'd filters. Many title variations (OR'd). Broad keyword search.

## GOOD example (industry-focused):
{
  "personTitles": ["CEO", "Founder", "CTO", "VP Engineering", "Head of Engineering"],
  "qOrganizationIndustryTagIds": ["Computer Software", "Information Technology and Services"]
}
Why it works: 2 filters only. Broad titles. Related industries.

## GOOD example (seniority + location):
{
  "personSeniorities": ["director", "vp", "c_suite"],
  "organizationLocations": ["United States"],
  "qOrganizationIndustryTagIds": ["Financial Services"]
}
Why it works: seniority is broad (3 levels OR'd), location is wide (whole country), one industry filter.

Valid employee ranges:
${employeeRanges.map((r) => `- "${r.value}" (${r.label})`).join("\n")}

Valid industry names (use the name as the tag ID):
${industries.map((i) => `- "${i.name}"`).join("\n")}

Rules:
- Only include fields that are relevant to the input
- Use exact enum values for employee ranges
- Use industry names for qOrganizationIndustryTagIds
- NEVER use more than 3 filters at once — prefer 1-2
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
      model: "claude-sonnet-4-6",
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
      console.error(`[search-transform] Attempt ${attempt}: invalid JSON`);
      lastErrors = [{ field: "root", message: "Response was not valid JSON" }];
      continue;
    }

    // Validate via Apollo
    const validation = await validateSearchParams(parsed, clerkOrgId);

    if (validation.valid) {
      transformCache.set(key, { params: parsed, cachedAt: Date.now() });
      await logCosts(runId, totalInputTokens, totalOutputTokens);
      return parsed;
    }

    console.warn(`[search-transform] Attempt ${attempt} invalid:`, validation.errors);
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
      { costName: "anthropic-sonnet-4.6-tokens-input", quantity: inputTokens },
      { costName: "anthropic-sonnet-4.6-tokens-output", quantity: outputTokens },
    ]);
  } catch (err) {
    console.error("[search-transform] Failed to log costs:", err);
  }
}
