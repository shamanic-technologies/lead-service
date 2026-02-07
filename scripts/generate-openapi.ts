import swaggerAutogen from "swagger-autogen";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = join(__dirname, "..");

const doc = {
  info: {
    title: "Lead Service",
    description: "Manages lead buffering, deduplication, cursor pagination, and enrichment for campaign outreach.",
    version: "1.0.0",
  },
  host: process.env.SERVICE_URL || "http://localhost:3006",
  basePath: "/",
  schemes: ["https"],
  securityDefinitions: {
    apiKey: {
      type: "apiKey",
      in: "header",
      name: "x-api-key",
      description: "API key for authenticating requests. Must match the LEAD_SERVICE_API_KEY env var on the server.",
    },
  },
  security: [{ apiKey: [] }],
};

const outputFile = join(projectRoot, "openapi.json");
const routes = [join(projectRoot, "src/index.ts")];

swaggerAutogen({ openapi: "3.0.0" })(outputFile, routes, doc);
