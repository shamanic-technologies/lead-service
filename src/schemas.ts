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
    name: "x-app-id",
    required: true,
    schema: { type: "string" as const },
    description: "Identifies the calling application, e.g. mcpfactory",
  },
  {
    in: "header" as const,
    name: "x-org-id",
    required: true,
    schema: { type: "string" as const },
    description: "External organization ID, e.g. Clerk org ID",
  },
];

// --- Health ---

const HealthResponseSchema = z
  .object({
    status: z.string(),
    service: z.string(),
  })
  .openapi("HealthResponse");

// --- Buffer Push ---

const LeadInputSchema = z.object({
  email: z.string(),
  externalId: z.string().nullish(),
  data: z.unknown().optional(),
});

export const BufferPushRequestSchema = z
  .object({
    campaignId: z.string(),
    brandId: z.string(),
    parentRunId: z.string(),
    clerkUserId: z.string().optional(),
    leads: z.array(LeadInputSchema),
  })
  .openapi("BufferPushRequest");

const BufferPushResponseSchema = z
  .object({
    buffered: z.number(),
    skippedAlreadyServed: z.number(),
  })
  .openapi("BufferPushResponse");

// --- Buffer Next ---

export const BufferNextRequestSchema = z
  .object({
    campaignId: z.string(),
    brandId: z.string(),
    parentRunId: z.string(),
    searchParams: z.record(z.string(), z.unknown()).optional(),
    clerkUserId: z.string().optional(),
  })
  .openapi("BufferNextRequest");

const ServedLeadSchema = z.object({
  email: z.string(),
  externalId: z.string().nullable(),
  data: z.unknown(),
  brandId: z.string(),
  clerkOrgId: z.string().nullable(),
  clerkUserId: z.string().nullable(),
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

const LeadsResponseSchema = z
  .object({
    leads: z.array(z.record(z.string(), z.unknown())),
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
    buffered: z.number(),
    skipped: z.number(),
    apollo: ApolloStatsSchema,
  })
  .openapi("StatsResponse");

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
  path: "/buffer/push",
  summary: "Push leads into the buffer",
  request: {
    params: z.object({}),
    body: {
      content: { "application/json": { schema: BufferPushRequestSchema } },
    },
  },
  parameters: AuthHeaders,
  responses: {
    200: {
      description: "Leads buffered successfully",
      content: { "application/json": { schema: BufferPushResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
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
  parameters: AuthHeaders,
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
      name: "clerkOrgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "clerkUserId",
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
    "Returns counts of leads by status: served (delivered with verified email), buffered (awaiting enrichment), and skipped (no email found).",
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
      name: "clerkOrgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "clerkUserId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "appId",
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
  ],
  responses: {
    200: {
      description: "Lead stats by status with Apollo metrics",
      content: { "application/json": { schema: StatsResponseSchema } },
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
