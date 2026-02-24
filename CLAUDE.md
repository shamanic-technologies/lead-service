# Project: lead-service

Single source of truth for lead management — buffering, deduplication, enrichment caching, and lead retrieval.

## Commands

- `npm test` — run all tests (Vitest)
- `npm run test:unit` — run unit tests only
- `npm run test:integration` — run integration tests only
- `npm run build` — compile TypeScript + generate OpenAPI spec
- `npm run dev` — local dev server with hot reload
- `npm run generate:openapi` — regenerate openapi.json from Zod schemas
- `npm run db:generate` — generate Drizzle migrations
- `npm run db:migrate` — run Drizzle migrations
- `npm run db:push` — push schema directly (dev only)

## Architecture

- `src/schemas.ts` — Zod schemas (source of truth for validation + OpenAPI)
- `src/routes/` — Express route handlers (buffer, leads, cursor, health, stats)
- `src/middleware/auth.ts` — API key + multi-tenant header auth
- `src/lib/buffer.ts` — pullNext(), fillBufferFromSearch() buffer logic
- `src/lib/dedup.ts` — checkDelivered() (via email-gateway), markServed() deduplication
- `src/lib/email-gateway-client.ts` — Email-gateway POST /status client for delivery checks
- `src/lib/leads-registry.ts` — Global lead identity registry (leads + leadEmails tables)
- `src/lib/apollo-client.ts` — Apollo enrichment service integration
- `src/lib/campaign-client.ts` — Campaign service client (fetch campaign details for search context)
- `src/lib/brand-client.ts` — Brand service client (fetch brand details for search context)
- `src/lib/runs-client.ts` — Runs service client for distributed tracing
- `src/db/schema.ts` — Drizzle ORM table definitions (PostgreSQL)
- `src/db/index.ts` — Database connection
- `src/config.ts` — Environment config
- `src/instrument.ts` — Sentry instrumentation
- `tests/` — Test files (`*.test.ts`)
- `openapi.json` — Auto-generated from Zod schemas, do NOT edit manually
