import net from "node:net";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import * as schema from "./schema.js";

const connectionString = process.env.LEAD_SERVICE_DATABASE_URL;

if (!connectionString) {
  throw new Error("LEAD_SERVICE_DATABASE_URL is not set");
}

// Node 20 gives each candidate address 250ms before happy-eyeballs moves on and,
// once every candidate has been tried, rejects with `AggregateError [ETIMEDOUT]`.
// A Neon compute resuming from scale-to-zero takes ~1-7s to accept, so the first
// connection after a suspend loses that race and fails outright. Widen the
// per-candidate budget to 5s.
//
// This is NOT a postgres.js option: `connect_timeout` already defaults to 30s and
// `idle_timeout` defaults to `null` (idle connections are never closed), so
// neither would help here — see brand-service#389.
net.setDefaultAutoSelectFamilyAttemptTimeout(5_000);

export const sql = postgres(connectionString, {
  prepare: false,
});
export const db = drizzle(sql, { schema });
