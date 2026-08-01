import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, it, expect } from "vitest";

/**
 * CI must never touch the database the running service uses.
 *
 * The workflow used to apply Drizzle migrations against `secrets.LEAD_SERVICE_DATABASE_URL`
 * on `pull_request`, and that secret has no `_DEV` variant — so opening a PR moved the
 * production schema before review, before merge, and before the code expecting the new shape
 * was deployed (issue #397). These assertions are textual on purpose: the invariant is that
 * the secret's NAME cannot appear in the file at all, which no amount of YAML parsing states
 * more clearly.
 */
const workflow = readFileSync(
  fileURLToPath(new URL("../../.github/workflows/ci.yml", import.meta.url)),
  "utf8",
);

/** Comments explain why the secret is gone; a real reference is `${{ secrets.NAME }}`. */
const secretReferences = workflow.match(/\$\{\{\s*secrets\.[A-Z0-9_]+\s*\}\}/g) ?? [];

describe("CI workflow database isolation", () => {
  it("never references the live database secret", () => {
    expect(secretReferences).not.toContain("${{ secrets.LEAD_SERVICE_DATABASE_URL }}");
  });

  it("uses no repository secrets at all", () => {
    expect(secretReferences).toEqual([]);
  });

  it("provisions its own Postgres for the run", () => {
    expect(workflow).toMatch(/services:\s*\n\s*postgres:\s*\n\s*image: postgres:\d+/);
  });

  it("points the service at that Postgres, on localhost", () => {
    expect(workflow).toMatch(
      /LEAD_SERVICE_DATABASE_URL: postgres:\/\/postgres:postgres@localhost:5432\//,
    );
  });

  it("still applies migrations, so a broken migration fails the run", () => {
    expect(workflow).toContain("npm run db:migrate");
  });

  it("runs the whole suite against the database it just created", () => {
    expect(workflow).toMatch(/run: npm test\b/);
  });
});
