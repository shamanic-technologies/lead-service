/**
 * Vitest global setup — sets all required env vars before any module imports.
 * This prevents config.ts from throwing during test runs.
 */

const TEST_ENV_VARS: Record<string, string> = {
  LEAD_SERVICE_API_KEY: "test-api-key",
  LEAD_SERVICE_DATABASE_URL: "postgresql://test:test@localhost:5432/test",
  APOLLO_SERVICE_URL: "http://apollo:3003",
  APOLLO_SERVICE_API_KEY: "test-apollo-key",
  BRAND_SERVICE_URL: "http://brand:3005",
  BRAND_SERVICE_API_KEY: "test-brand-key",
  CAMPAIGN_SERVICE_URL: "http://campaign:3003",
  CAMPAIGN_SERVICE_API_KEY: "test-campaign-key",
  EMAIL_GATEWAY_SERVICE_URL: "http://email-gateway:3009",
  EMAIL_GATEWAY_SERVICE_API_KEY: "test-email-gateway-key",
  JOURNALISTS_SERVICE_URL: "http://journalists:3011",
  JOURNALISTS_SERVICE_API_KEY: "test-journalists-key",
  OUTLETS_SERVICE_URL: "http://outlets:3010",
  OUTLETS_SERVICE_API_KEY: "test-outlets-key",
  RUNS_SERVICE_URL: "http://runs:3007",
  RUNS_SERVICE_API_KEY: "test-runs-key",
  KEY_SERVICE_URL: "http://key-service:3001",
  KEY_SERVICE_API_KEY: "test-key-service-key",
  FEATURES_SERVICE_URL: "http://features:3010",
  FEATURES_SERVICE_API_KEY: "test-features-key",
  WORKFLOW_SERVICE_URL: "http://workflows:3002",
  WORKFLOW_SERVICE_API_KEY: "test-workflows-key",
  NODE_ENV: "test",
};

for (const [key, value] of Object.entries(TEST_ENV_VARS)) {
  if (!process.env[key]) {
    process.env[key] = value;
  }
}
