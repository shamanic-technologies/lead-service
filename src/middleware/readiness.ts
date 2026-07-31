import type { Request, Response, NextFunction } from "express";
import { getBootState, getBootFailure } from "../lib/boot-state.js";

/**
 * Gate every route behind boot completion.
 *
 * The port opens before migrations run so a cold Neon compute cannot eat
 * Railway's healthcheck window. The cost of that is a window where the process
 * is listening while the schema may still be mid-migration — this middleware
 * closes it. Nothing but `/health` (and the static `/openapi.json`) is reachable
 * until `markBootReady()` has been called, so the service never touches the DB
 * on a schema its code does not expect.
 */
export function requireBootReady(_req: Request, res: Response, next: NextFunction): void {
  const state = getBootState();

  if (state === "ready") {
    next();
    return;
  }

  if (state === "failed") {
    res.status(503).json({
      error: "Service unavailable: database migrations failed",
      detail: getBootFailure(),
    });
    return;
  }

  res.status(503).set("Retry-After", "5").json({
    error: "Service starting: database migrations in progress",
  });
}
