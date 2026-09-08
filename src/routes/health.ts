import { Router } from "express";
import { healthSql } from "../db/index.js";
import { getBootState, getBootFailure } from "../lib/boot-state.js";

const router = Router();

const DB_PING_TIMEOUT_MS = 2_000;

router.get("/health", async (_req, res) => {
  const bootState = getBootState();

  // Migrations could not be applied: fail the healthcheck so the release is
  // rejected and Railway keeps the previous deploy. Loud, not silent.
  if (bootState === "failed") {
    res.status(503).json({
      status: "unavailable",
      service: "lead-service",
      migrations: "failed",
      detail: getBootFailure(),
    });
    return;
  }

  // Port is open, migrations still running. Report healthy WITHOUT touching the
  // DB: a Neon compute resuming from scale-to-zero would otherwise fail this
  // ping and take the whole deploy down with it. Traffic stays gated by
  // `requireBootReady` until migrations finish, so a 200 here does not mean the
  // service is serving requests yet.
  if (bootState === "starting") {
    res.json({ status: "starting", service: "lead-service", migrations: "pending" });
    return;
  }

  try {
    await Promise.race([
      // Liveness reads its OWN two-connection pool: on 2026-09-07 the main pool was held by ten
      // abandoned lead reads, and /health — which needs nothing more than a socket and a SELECT 1
      // — reported this service unavailable for 11.5 hours because it queued behind them.
      healthSql`SELECT 1`,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("db ping timeout")), DB_PING_TIMEOUT_MS),
      ),
    ]);
    res.json({ status: "ok", service: "lead-service" });
  } catch (err) {
    console.error("[lead-service] /health DB ping failed:", err);
    res.status(503).json({ status: "unavailable", service: "lead-service" });
  }
});

export default router;
