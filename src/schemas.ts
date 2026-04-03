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
    description: "Brand identifier(s), comma-separated for multi-brand campaigns (auto-injected by workflow-service). Example: uuid1,uuid2,uuid3",
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
  .openapi("ApolloPersonData", {
    description:
      "Apollo person + organization data in flat camelCase format. " +
      "Organization fields are prefixed with 'organization' (e.g. organizationDomain, organizationName) — " +
      "there is NO nested 'organization' object.",
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
  data: ApolloPersonDataSchema.nullable(),
  brandIds: z.array(z.string()),
  orgId: z.string().nullable(),
  userId: z.string().nullable(),
  apolloPersonId: z
    .string()
    .nullable()
    .optional()
    .openapi({
      description:
        "Apollo person ID from enrichment. Present when the lead was sourced or enriched via Apollo.",
      example: "5f2a3b4c5d6e7f8a9b0c1d2e",
    }),
  journalistId: z
    .string()
    .nullable()
    .optional()
    .openapi({
      description:
        "Journalist ID from journalists-service. Present only when sourceType is 'journalist'.",
      example: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    }),
  outletId: z
    .string()
    .nullable()
    .optional()
    .openapi({
      description:
        "Outlet ID from outlets-service. Present only when sourceType is 'journalist'.",
      example: "f9e8d7c6-b5a4-3210-fedc-ba0987654321",
    }),
});

const BufferNextResponseSchema = z
  .object({
    found: z.boolean(),
    lead: ServedLeadSchema.optional(),
  })
  .openapi("BufferNextResponse", {
    description:
      "Response from pulling the next lead. When found is true, lead contains the served lead with typed IDs.",
    examples: [
      {
        summary: "Apollo lead",
        value: {
          found: true,
          lead: {
            leadId: "c1d2e3f4-a5b6-7890-abcd-ef1234567890",
            email: "jane.doe@acme.com",
            data: {
              firstName: "Jane",
              lastName: "Doe",
              title: "VP of Marketing",
              organizationName: "Acme Corp",
              organizationDomain: "acme.com",
            },
            brandIds: ["brand-uuid"],
            orgId: "org-uuid",
            userId: "user-uuid",
            apolloPersonId: "5f2a3b4c5d6e7f8a9b0c1d2e",
            journalistId: null,
            outletId: null,
          },
        },
      },
      {
        summary: "Journalist lead",
        value: {
          found: true,
          lead: {
            leadId: "d4e5f6a7-b8c9-0123-abcd-ef4567890123",
            email: "john.writer@techcrunch.com",
            data: {
              firstName: "John",
              lastName: "Writer",
              organizationName: "TechCrunch",
              organizationDomain: "techcrunch.com",
              sourceType: "journalist",
            },
            brandIds: ["brand-uuid"],
            orgId: "org-uuid",
            userId: "user-uuid",
            apolloPersonId: "7a8b9c0d1e2f3a4b5c6d7e8f",
            journalistId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            outletId: "f9e8d7c6-b5a4-3210-fedc-ba0987654321",
          },
        },
      },
      {
        summary: "Buffer exhausted",
        value: {
          found: false,
        },
      },
    ],
  });

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
    brandIds: z.array(z.string()),
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
    journalistId: z
      .string()
      .nullable()
      .openapi({ description: "Journalist ID from journalists-service, if the lead is a journalist" }),
    outletId: z
      .string()
      .nullable()
      .openapi({ description: "Outlet ID from outlets-service, if the lead is a journalist" }),
    contacted: z.boolean(),
    delivered: z.boolean(),
    bounced: z.boolean(),
    replied: z
      .boolean()
      .openapi({ description: "Whether the lead replied (any reply, regardless of sentiment)" }),
    replyClassification: z
      .enum(["positive", "negative", "neutral"])
      .nullable()
      .openapi({
        description:
          "Classification of the most recent reply from email-gateway. " +
          "'positive' = interested or willing to meet, " +
          "'negative' = not interested, " +
          "'neutral' = ambiguous or informational. " +
          "null when no reply detected.",
      }),
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
  summary: "Get per-lead delivery and reply status",
  description:
    "Returns delivery status (contacted, delivered, bounced, replied, replyClassification) for served leads. " +
    "With campaignId: campaign-scoped status. Without campaignId: cross-campaign brand-scoped status " +
    "(requires brandId). At least one of campaignId or brandId must be provided. " +
    "Calls email-gateway internally.",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      schema: { type: "string" as const },
      description:
        "Campaign ID filter. When provided, status is campaign-scoped. " +
        "When absent, status is brand-scoped (cross-campaign) and brandId becomes required.",
    },
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
      description:
        "Brand ID filter. Required when campaignId is absent (cross-campaign mode). " +
        "Optional additional filter when campaignId is present.",
    },
    {
      in: "query" as const,
      name: "outletId",
      required: false,
      schema: { type: "string" as const },
      description:
        "Outlet ID filter. Filters leads whose metadata.outletId matches. " +
        "Useful for outlet-level dedup (e.g. checking if any journalist from an outlet was already contacted).",
    },
  ],
  responses: {
    200: {
      description: "Per-lead delivery and reply statuses",
      content: { "application/json": { schema: LeadStatusResponseSchema } },
    },
    400: {
      description: "Missing required query parameters",
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
