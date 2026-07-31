import { describe, it, expect, beforeEach } from "vitest";
import express from "express";
import request from "supertest";
import { requireBootReady } from "../../src/middleware/readiness.js";
import { markBootReady, markBootFailed, resetBootState } from "../../src/lib/boot-state.js";

function makeApp() {
  const app = express();
  app.get("/health", (_req, res) => {
    res.json({ status: "starting", service: "lead-service" });
  });
  app.use(requireBootReady);
  app.post("/orgs/buffer/next", (_req, res) => {
    res.json({ found: false });
  });
  return app;
}

describe("requireBootReady", () => {
  beforeEach(() => {
    resetBootState();
  });

  it("503s business routes while migrations are still running", async () => {
    const res = await request(makeApp()).post("/orgs/buffer/next");

    expect(res.status).toBe(503);
    expect(res.headers["retry-after"]).toBe("5");
    expect(res.body.error).toMatch(/migrations in progress/);
  });

  it("leaves /health reachable while migrations are still running", async () => {
    const res = await request(makeApp()).get("/health");

    expect(res.status).toBe(200);
  });

  it("503s business routes when migrations failed, naming the reason", async () => {
    markBootFailed(new Error("syntax error at or near \"CREAT\""));

    const res = await request(makeApp()).post("/orgs/buffer/next");

    expect(res.status).toBe(503);
    expect(res.body.error).toMatch(/migrations failed/);
    expect(res.body.detail).toContain("syntax error");
  });

  it("passes traffic through once migrations completed", async () => {
    markBootReady();

    const res = await request(makeApp()).post("/orgs/buffer/next");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ found: false });
  });
});
