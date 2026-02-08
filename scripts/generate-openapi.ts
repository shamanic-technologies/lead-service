import { OpenApiGeneratorV3 } from "@asteasolutions/zod-to-openapi";
import { registry } from "../src/schemas";
import * as fs from "fs";

const generator = new OpenApiGeneratorV3(registry.definitions);

const document = generator.generateDocument({
  openapi: "3.0.0",
  info: {
    title: "Lead Service",
    description:
      "Manages lead buffering, deduplication, cursor pagination, and enrichment for campaign outreach.",
    version: "1.0.0",
  },
  servers: [
    { url: process.env.SERVICE_URL || "http://localhost:3006" },
  ],
  security: [{ apiKey: [] }],
});

// Add security scheme (zod-to-openapi doesn't handle securityDefinitions directly)
(document as Record<string, unknown>).components = {
  ...(document.components ?? {}),
  securitySchemes: {
    apiKey: {
      type: "apiKey",
      in: "header",
      name: "x-api-key",
      description:
        "API key for authenticating requests. Must match the LEAD_SERVICE_API_KEY env var on the server.",
    },
  },
};

fs.writeFileSync("openapi.json", JSON.stringify(document, null, 2));
console.log("Generated openapi.json");
