import { describe, it, expect } from "vitest";
import {
  MAX_SEARCH_LENGTH,
  MAX_SEARCH_TOKENS,
  leadSearchPattern,
  parseLeadSearch,
} from "../../src/lib/lead-search.js";

describe("the free-text search a caller names", () => {
  it("is absent by default — a read that names none searches for nothing", () => {
    expect(parseLeadSearch(undefined)).toBeNull();
  });

  it("splits into the words every one of which must match", () => {
    expect(parseLeadSearch("  jane   acme ")).toEqual(["jane", "acme"]);
  });

  it("refuses a search it cannot honour rather than ignoring it", () => {
    expect(() => parseLeadSearch("   ")).toThrow(/must not be blank/);
    expect(() => parseLeadSearch("x".repeat(MAX_SEARCH_LENGTH + 1))).toThrow(/at most/);
    expect(() => parseLeadSearch(Array(MAX_SEARCH_TOKENS + 1).fill("a").join(" "))).toThrow(
      /at most/,
    );
    expect(() => parseLeadSearch(["a"])).toThrow(/single search string/);
  });

  it("escapes the LIKE metacharacters somebody typed, rather than obeying them", () => {
    expect(leadSearchPattern("50%")).toBe("%50\\%%");
    expect(leadSearchPattern("a_b")).toBe("%a\\_b%");
    expect(leadSearchPattern("c\\d")).toBe("%c\\\\d%");
    expect(leadSearchPattern("jane")).toBe("%jane%");
  });
});
