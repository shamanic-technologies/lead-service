/**
 * Connect-phase retry for the boot-time migration run.
 *
 * Neon computes suspend after inactivity. A deploy that lands on a suspended
 * compute pays the resume (~1-7s) on its FIRST connection, and that first
 * connection frequently does not merely wait — it is refused or reset while the
 * compute is still coming up, or aborted by Node 20's 250ms happy-eyeballs
 * per-candidate budget. `migrate()` then rejects, which used to `process.exit(1)`
 * into a Railway restart loop.
 *
 * A rejected CONNECTION is safe to retry: no statement reached the server, so no
 * migration was half-applied. A rejected MIGRATION (bad SQL, failed constraint,
 * lock timeout on a real table) is NOT retried — it propagates on the first
 * throw so the failure surfaces intact instead of being smeared over a minute of
 * pointless reconnects.
 *
 * Note this is deliberately NOT a postgres.js connection option. `connect_timeout`
 * already defaults to 30s (postgres/src/index.js), and `idle_timeout` defaults to
 * `null` (idle connections are never closed) — setting either is respectively a
 * no-op and a regression. See brand-service#389.
 */

/** Driver / OS level codes that mean "the connection never got established". */
const TRANSIENT_CODES = new Set([
  // Node / libuv socket layer
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "EHOSTUNREACH",
  "ENETUNREACH",
  "EPIPE",
  // postgres.js internal codes
  "CONNECT_TIMEOUT",
  "CONNECTION_CLOSED",
  "CONNECTION_ENDED",
  "CONNECTION_DESTROYED",
  "CONNECTION_REFUSED",
  // PostgreSQL SQLSTATE class 08 (connection exception) + Neon's "still waking"
  "08000",
  "08003",
  "08006",
  "08001",
  "08004",
  "57P03", // cannot_connect_now — Neon returns this while the compute resumes
]);

/**
 * Neon's proxy and postgres.js both surface some of these as a message with no
 * machine-readable code, so match the text too.
 */
const TRANSIENT_MESSAGES =
  /connect_timeout|connection timeout|timeout expired|timeout exceeded when trying to connect|cannot connect now|the database system is starting up|connection closed|connection ended|write connection_closed|socket hang up|terminating connection due to administrator command/i;

/**
 * Transient failures arrive wrapped: `cause` chains, and `AggregateError.errors`
 * for happy-eyeballs (one sub-error per candidate address). Walk both, guarding
 * against cycles.
 */
export function isTransientConnectError(err: unknown): boolean {
  const seen = new Set<unknown>();
  const stack: unknown[] = [err];
  while (stack.length > 0) {
    const cur = stack.pop();
    if (cur === null || typeof cur !== "object" || seen.has(cur)) continue;
    seen.add(cur);

    const code = (cur as { code?: unknown }).code;
    if (typeof code === "string" && TRANSIENT_CODES.has(code)) return true;

    const message = (cur as { message?: unknown }).message;
    if (typeof message === "string" && TRANSIENT_MESSAGES.test(message)) return true;

    const cause = (cur as { cause?: unknown }).cause;
    if (cause !== undefined) stack.push(cause);

    const errors = (cur as { errors?: unknown }).errors;
    if (Array.isArray(errors)) stack.push(...errors);
  }
  return false;
}

/**
 * ~65s of retry budget, front-loaded. A Neon resume lands in the first few
 * hundred ms of waiting; the long tail exists so a compute that is genuinely
 * slow to wake still boots rather than failing the release.
 */
export const CONNECT_BACKOFF_MS = [250, 500, 1_000, 2_000, 4_000, 8_000, 8_000, 8_000, 8_000, 8_000, 8_000, 8_000];

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

export interface RetryOptions {
  backoffMs?: number[];
  /** Injectable for tests so the retry ladder does not take a real minute. */
  wait?: (ms: number) => Promise<void>;
  onRetry?: (attempt: number, delayMs: number, err: unknown) => void;
}

/**
 * Run `fn`, retrying ONLY connect-phase failures. Anything else — and an
 * exhausted budget — rejects with the original error, unwrapped.
 */
export async function withConnectRetry<T>(fn: () => Promise<T>, options: RetryOptions = {}): Promise<T> {
  const backoff = options.backoffMs ?? CONNECT_BACKOFF_MS;
  const wait = options.wait ?? sleep;

  for (let attempt = 0; ; attempt += 1) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= backoff.length || !isTransientConnectError(err)) throw err;
      const delayMs = backoff[attempt];
      options.onRetry?.(attempt + 1, delayMs, err);
      await wait(delayMs);
    }
  }
}
