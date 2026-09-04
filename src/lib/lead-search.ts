/**
 * Free-text search over the people a `GET /orgs/leads` read is about.
 *
 * The dashboard used to search by holding the whole population in the browser and filtering the
 * array. That only works while the whole population is in the browser, which is the thing being
 * fixed — so the search has to be evaluated where the population lives, over the WHOLE matching
 * set rather than over whichever page happens to be loaded.
 *
 * Four fields, and they are the four a person is looked up by: their name, their job title, their
 * email address, and their company's name. Nothing here searches the enrichment payload at large:
 * a search that matches on a technology tag or a funding round is a different feature, and it
 * would make the predicate unexplainable to whoever typed two words into a box.
 *
 * A query is TOKENIZED on whitespace and every token must match at least one of the four fields
 * (AND across tokens, OR across fields), so "jane acme" finds Jane at Acme rather than everyone
 * called Jane plus everyone at Acme. Matching is case-insensitive and substring — `ILIKE %token%`.
 */

/** How many words a search may carry. Beyond this it is a 400, never a silently truncated query. */
export const MAX_SEARCH_TOKENS = 8;
/** How long a search may be. Same posture: refused, never trimmed to something else. */
export const MAX_SEARCH_LENGTH = 200;

/**
 * Resolve the `q` query param into the tokens a search matches on, or null when the caller named
 * no search at all.
 *
 * Absent → null (no search; the read is exactly what it is today). Present but blank, too long, or
 * carrying more words than this will match on → a 400 (throws). A silently ignored search is a
 * consumer showing a filtered-looking list that was never filtered, with nothing anywhere going red.
 */
export function parseLeadSearch(raw: unknown): string[] | null {
  if (raw === undefined) return null;
  if (typeof raw !== "string") throw new Error("q must be a single search string");
  const trimmed = raw.trim();
  if (trimmed === "") {
    throw new Error("q must not be blank — omit it to search for nothing");
  }
  if (trimmed.length > MAX_SEARCH_LENGTH) {
    throw new Error(`q must be at most ${MAX_SEARCH_LENGTH} characters, got ${trimmed.length}`);
  }
  const tokens = trimmed.split(/\s+/).filter((t) => t.length > 0);
  if (tokens.length > MAX_SEARCH_TOKENS) {
    throw new Error(`q must name at most ${MAX_SEARCH_TOKENS} words, got ${tokens.length}`);
  }
  return tokens;
}

/**
 * The `ILIKE` pattern one token matches on.
 *
 * `%`, `_` and `\` are LIKE metacharacters, so a search for "50%" must not become a search for
 * "50<anything>". They are escaped rather than stripped: the person typed them.
 */
export function leadSearchPattern(token: string): string {
  return `%${token.replace(/([\\%_])/g, "\\$1")}%`;
}
