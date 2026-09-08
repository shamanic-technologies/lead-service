import type { Request, Response } from "express";

/**
 * Thrown out of a streaming read when the caller went away mid-walk.
 *
 * It is an ERROR, not a quiet `break`: the walk is abandoned part-done, so nothing downstream may
 * mistake the truncated stream for a complete one. The route logs it and destroys the socket the
 * caller had already stopped reading.
 */
export class ClientGoneError extends Error {
  readonly code = "client_gone";

  constructor(rowsWritten: number) {
    super(`the caller went away after ${rowsWritten} rows; the read was abandoned`);
    this.name = "ClientGoneError";
  }
}

export function isClientGone(error: unknown): error is ClientGoneError {
  return error instanceof ClientGoneError;
}

export interface ClientWatch {
  /** True once the caller's socket closed before this response finished. */
  readonly gone: boolean;
  /** Throw `ClientGoneError` if the caller is gone — call it between chunks of a long walk. */
  stopIfGone(rowsWritten: number): void;
  /** Drop the listeners once the response is done. */
  dispose(): void;
}

/**
 * Watch for the caller abandoning a long read.
 *
 * A whole-population read of a large brand streams for seconds and holds a database connection
 * for the whole walk. An upstream caller that gives up at its own HTTP timeout closes its socket
 * and stops reading — and until 2026-09-08 nothing here noticed: the walk kept its connection,
 * the suspended portal kept its Postgres backend, and ten such reads in one burst took the whole
 * pool down for 11h30m with no self-recovery.
 *
 * So a read that streams asks between chunks whether anyone is still listening, and stops if not.
 * Stopping breaks out of the chunk generator, which closes the cursor and returns the connection —
 * that release is the whole point, and it is why this cannot be a flag somebody checks at the end.
 */
export function watchClient(req: Request, res: Response): ClientWatch {
  let gone = false;

  const onClose = (): void => {
    // `close` also fires on a response that completed normally — that is not an abandonment, and
    // treating it as one would fail every successful read at its last byte.
    if (res.writableEnded || res.writableFinished) return;
    gone = true;
  };

  req.on("close", onClose);
  res.on("close", onClose);

  return {
    get gone() {
      return gone;
    },
    stopIfGone(rowsWritten: number) {
      if (gone) throw new ClientGoneError(rowsWritten);
    },
    dispose() {
      req.off("close", onClose);
      res.off("close", onClose);
    },
  };
}
