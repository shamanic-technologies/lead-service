import { z } from "zod";
import {
  OpenAPIRegistry,
  extendZodWithOpenApi,
} from "@asteasolutions/zod-to-openapi";

extendZodWithOpenApi(z);

export const registry = new OpenAPIRegistry();

// --- Common ---

const ErrorResponseSchema = z
  .object({ error: z.string() })
  .openapi("ErrorResponse");

const AuthHeaders = [
  {
    in: "header" as const,
    name: "x-api-key",
    required: true,
    schema: { type: "string" as const },
    description: "API key for authenticating requests",
  },
  {
    in: "header" as const,
    name: "x-org-id",
    required: true,
    schema: { type: "string" as const },
    description: "Internal organization UUID from client-service",
  },
  {
    in: "header" as const,
    name: "x-user-id",
    required: true,
    schema: { type: "string" as const },
    description: "Internal user UUID from client-service",
  },
  {
    in: "header" as const,
    name: "x-run-id",
    required: true,
    schema: { type: "string" as const },
    description: "The caller's run ID (used as parentRunId when creating this service's own run)",
  },
  {
    in: "header" as const,
    name: "x-campaign-id",
    required: false,
    schema: { type: "string" as const },
    description: "Campaign identifier (auto-injected by workflow-service)",
  },
  {
    in: "header" as const,
    name: "x-brand-id",
    required: false,
    schema: { type: "string" as const },
    description: "Brand identifier (auto-injected by workflow-service)",
  },
  {
    in: "header" as const,
    name: "x-workflow-slug",
    required: false,
    schema: { type: "string" as const },
    description: "Workflow slug (auto-injected by workflow-service)",
  },
  {
    in: "header" as const,
    name: "x-feature-slug",
    required: false,
    schema: { type: "string" as const },
    description: "Feature slug for tracking (propagated through the call chain)",
  },
];

// buffer/next requires x-campaign-id and x-brand-id
const BufferNextHeaders = AuthHeaders.map((h) =>
  h.name === "x-campaign-id" || h.name === "x-brand-id"
    ? { ...h, required: true }
    : h
);

// --- Health ---

const HealthResponseSchema = z
  .object({
    status: z.string(),
    service: z.string(),
  })
  .openapi("HealthResponse");

// --- Apollo Person Data (flat camelCase — matches Apollo enrichment API) ---

const EmploymentHistorySchema = z.object({
  title: z.string().nullable().optional(),
  organizationName: z.string().nullable().optional(),
  startDate: z.string().nullable().optional(),
  endDate: z.string().nullable().optional(),
  description: z.string().nullable().optional(),
  current: z.boolean().optional(),
});

const FundingEventSchema = z.object({
  id: z.string().nullable().optional(),
  date: z.string().nullable().optional(),
  type: z.string().nullable().optional(),
  investors: z.string().nullable().optional(),
  amount: z.union([z.number(), z.string()]).nullable().optional(),
  currency: z.string().nullable().optional(),
  news_url: z.string().nullable().optional(),
});

const TechnologySchema = z.object({
  uid: z.string().nullable().optional(),
  name: z.string().nullable().optional(),
  category: z.string().nullable().optional(),
});

export const ApolloPersonDataSchema = z
  .object({
    // Person identifiers
    id: z.string().optional(),
    email: z.string().nullable().optional(),
    emailStatus: z.string().nullable().optional(),
    firstName: z.string(),
    lastName: z.string(),
    title: z.string().nullable().optional(),
    linkedinUrl: z.string().nullable().optional(),
    // Person details
    photoUrl: z.string().nullable().optional(),
    headline: z.string().nullable().optional(),
    city: z.string().nullable().optional(),
    state: z.string().nullable().optional(),
    country: z.string().nullable().optional(),
    seniority: z.string().nullable().optional(),
    departments: z.array(z.string()).optional(),
    subdepartments: z.array(z.string()).optional(),
    functions: z.array(z.string()).optional(),
    twitterUrl: z.string().nullable().optional(),
    githubUrl: z.string().nullable().optional(),
    facebookUrl: z.string().nullable().optional(),
    employmentHistory: z.array(EmploymentHistorySchema).optional(),
    // Organization details (flat, NOT nested)
    organizationName: z.string(),
    organizationDomain: z.string().nullable().optional(),
    organizationIndustry: z.string().nullable().optional(),
    organizationSize: z.string().nullable().optional(),
    organizationRevenueUsd: z.string().nullable().optional(),
    organizationWebsiteUrl: z.string().nullable().optional(),
    organizationLogoUrl: z.string().nullable().optional(),
    organizationShortDescription: z.string().nullable().optional(),
    organizationSeoDescription: z.string().nullable().optional(),
    organizationLinkedinUrl: z.string().nullable().optional(),
    organizationTwitterUrl: z.string().nullable().optional(),
    organizationFacebookUrl: z.string().nullable().optional(),
    organizationBlogUrl: z.string().nullable().optional(),
    organizationCrunchbaseUrl: z.string().nullable().optional(),
    organizationAngellistUrl: z.string().nullable().optional(),
    organizationFoundedYear: z.number().nullable().optional(),
    organizationPrimaryPhone: z.string().nullable().optional(),
    organizationPubliclyTradedSymbol: z.string().nullable().optional(),
    organizationPubliclyTradedExchange: z.string().nullable().optional(),
    organizationAnnualRevenuePrinted: z.string().nullable().optional(),
    organizationTotalFunding: z.string().nullable().optional(),
    organizationTotalFundingPrinted: z.string().nullable().optional(),
    organizationLatestFundingRoundDate: z.string().nullable().optional(),
    organizationLatestFundingStage: z.string().nullable().optional(),
    organizationFundingEvents: z.array(FundingEventSchema).optional(),
    organizationCity: z.string().nullable().optional(),
    organizationState: z.string().nullable().optional(),
    organizationCountry: z.string().nullable().optional(),
    organizationStreetAddress: z.string().nullable().optional(),
    organizationPostalCode: z.string().nullable().optional(),
    organizationTechnologyNames: z.array(z.string()).optional(),
    organizationCurrentTechnologies: z.array(TechnologySchema).optional(),
    organizationKeywords: z.array(z.string()).optional(),
    organizationIndustries: z.array(z.string()).optional(),
    organizationSecondaryIndustries: z.array(z.string()).optional(),
    organizationNumSuborganizations: z.number().nullable().optional(),
    organizationRetailLocationCount: z.number().nullable().optional(),
    organizationAlexaRanking: z.number().nullable().optional(),
  })
  .passthrough()
  .openapi("ApolloPersonData", {
    description:
      "Apollo person + organization data in flat camelCase format. " +
      "Organization fields are prefixed with 'organization' (e.g. organizationDomain, organizationName) — " +
      "there is NO nested 'organization' object. Additional fields from Apollo may be present.",
  });

// --- Buffer Next ---

export const BufferNextRequestSchema = z
  .object({
    sourceType: z.enum(["apollo", "journalist"]),
  })
  .openapi("BufferNextRequest");

const ServedLeadSchema = z.object({
  leadId: z.string().uuid(),
  email: z.string(),
  externalId: z.string().nullable(),
  data: ApolloPersonDataSchema.nullable(),
  brandId: z.string(),
  orgId: z.string().nullable(),
  userId: z.string().nullable(),
});

const BufferNextResponseSchema = z
  .object({
    found: z.boolean(),
    lead: ServedLeadSchema.optional(),
  })
  .openapi("BufferNextResponse");

// --- Cursor ---

export const CursorSetRequestSchema = z
  .object({
    state: z.unknown(),
  })
  .openapi("CursorSetRequest");

const CursorGetResponseSchema = z
  .object({
    state: z.unknown(),
  })
  .openapi("CursorGetResponse");

const CursorSetResponseSchema = z
  .object({
    ok: z.boolean(),
  })
  .openapi("CursorSetResponse");

// --- Leads ---

const LeadDetailSchema = z
  .object({
    id: z.string().uuid(),
    leadId: z.string().uuid().nullable(),
    namespace: z.string(),
    email: z.string(),
    externalId: z.string().nullable(),
    metadata: ApolloPersonDataSchema.nullable(),
    parentRunId: z.string().nullable(),
    runId: z.string().nullable(),
    brandId: z.string(),
    campaignId: z.string(),
    orgId: z.string(),
    userId: z.string().nullable(),
    servedAt: z.string(),
    enrichment: ApolloPersonDataSchema.nullable(),
  })
  .openapi("LeadDetail");

const LeadsResponseSchema = z
  .object({
    leads: z.array(LeadDetailSchema),
  })
  .openapi("LeadsResponse");

// --- Stats ---

const ApolloStatsSchema = z.object({
  enrichedLeadsCount: z.number(),
  searchCount: z.number(),
  fetchedPeopleCount: z.number(),
  totalMatchingPeople: z.number(),
});

const StatsResponseSchema = z
  .object({
    served: z.number(),
    contacted: z.number(),
    buffered: z.number(),
    skipped: z.number(),
    apollo: ApolloStatsSchema,
  })
  .openapi("StatsResponse");

const StatsGroupSchema = z.object({
  key: z.string(),
  served: z.number(),
  contacted: z.number(),
  buffered: z.number(),
  skipped: z.number(),
});

const StatsGroupedResponseSchema = z
  .object({
    groups: z.array(StatsGroupSchema),
  })
  .openapi("StatsGroupedResponse");

// --- Lead Status ---

const LeadStatusItemSchema = z
  .object({
    leadId: z.string().uuid(),
    email: z.string(),
    contacted: z.boolean(),
    delivered: z.boolean(),
    bounced: z.boolean(),
    replied: z.boolean(),
    lastDeliveredAt: z.string().nullable(),
  })
  .openapi("LeadStatusItem");

const LeadStatusResponseSchema = z
  .object({
    statuses: z.array(LeadStatusItemSchema),
  })
  .openapi("LeadStatusResponse");

// --- Register Paths ---

registry.registerPath({
  method: "get",
  path: "/health",
  summary: "Health check",
  responses: {
    200: {
      description: "Service is healthy",
      content: { "application/json": { schema: HealthResponseSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/buffer/next",
  summary: "Pull the next lead from the buffer",
  request: {
    params: z.object({}),
    body: {
      content: { "application/json": { schema: BufferNextRequestSchema } },
    },
  },
  parameters: BufferNextHeaders,
  responses: {
    200: {
      description: "Next lead from buffer",
      content: { "application/json": { schema: BufferNextResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/cursor/{namespace}",
  summary: "Get cursor state for a namespace",
  request: {
    params: z.object({ namespace: z.string() }),
  },
  parameters: AuthHeaders,
  responses: {
    200: {
      description: "Cursor state",
      content: { "application/json": { schema: CursorGetResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "put",
  path: "/cursor/{namespace}",
  summary: "Set cursor state for a namespace",
  request: {
    params: z.object({ namespace: z.string() }),
    body: {
      content: { "application/json": { schema: CursorSetRequestSchema } },
    },
  },
  parameters: AuthHeaders,
  responses: {
    200: {
      description: "Cursor updated",
      content: { "application/json": { schema: CursorSetResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/leads",
  summary: "List served leads with enrichment data",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "orgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "userId",
      required: false,
      schema: { type: "string" as const },
    },
  ],
  responses: {
    200: {
      description: "List of served leads",
      content: { "application/json": { schema: LeadsResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/stats",
  summary: "Get lead stats by status",
  description:
    "Returns counts of leads by status: served (delivered with verified email), contacted (unique leads with at least one successful email send), buffered (awaiting enrichment), and skipped (no email found).",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "orgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "userId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "runIds",
      required: false,
      description: "Comma-separated list of run IDs",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowSlug",
      required: false,
      description: "Filter by exact workflow slug (single value)",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowSlugs",
      required: false,
      description:
        "Filter by multiple workflow slugs (comma-separated). Takes priority over workflowSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureSlug",
      required: false,
      description: "Filter by exact feature slug (single value)",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureSlugs",
      required: false,
      description:
        "Filter by multiple feature slugs (comma-separated). Takes priority over featureSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowDynastySlug",
      required: false,
      description:
        "Filter by workflow dynasty slug. Resolved to all versioned slugs via workflow-service, then filtered with WHERE IN (...). Takes priority over workflowSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureDynastySlug",
      required: false,
      description:
        "Filter by feature dynasty slug. Resolved to all versioned slugs via features-service, then filtered with WHERE IN (...). Takes priority over featureSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "groupBy",
      required: false,
      description:
        "Group stats by this dimension. When set, returns { groups: [...] } instead of flat stats.",
      schema: {
        type: "string" as const,
        enum: [
          "campaignId",
          "brandId",
          "workflowSlug",
          "featureSlug",
          "workflowDynastySlug",
          "featureDynastySlug",
        ],
      },
    },
  ],
  responses: {
    200: {
      description:
        "Lead stats by status. Without groupBy: flat stats with Apollo metrics. With groupBy: grouped stats array.",
      content: {
        "application/json": {
          schema: z.union([StatsResponseSchema, StatsGroupedResponseSchema]),
        },
      },
    },
    400: {
      description: "Invalid groupBy value",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/leads/status",
  summary: "Get per-lead delivery status for a campaign",
  description:
    "Returns delivery status (contacted, delivered, bounced, replied) for each served lead in a campaign. " +
    "Calls email-gateway internally to resolve status.",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "campaignId",
      required: true,
      schema: { type: "string" as const },
      description: "Campaign ID to fetch lead statuses for",
    },
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
      description: "Optional brand ID filter",
    },
  ],
  responses: {
    200: {
      description: "Per-lead delivery statuses",
      content: { "application/json": { schema: LeadStatusResponseSchema } },
    },
    400: {
      description: "Missing campaignId",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/openapi.json",
  summary: "Get OpenAPI specification",
  responses: {
    200: { description: "OpenAPI JSON document" },
    404: { description: "Spec not generated" },
  },
});
