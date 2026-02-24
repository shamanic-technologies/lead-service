import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../../src/db/index.js", () => ({
  db: {
    query: {
      leads: { findFirst: vi.fn() },
      leadEmails: { findFirst: vi.fn() },
    },
    insert: vi.fn(),
    update: vi.fn(),
  },
}));

import { db } from "../../src/db/index.js";
import {
  resolveOrCreateLead,
  findLeadByApolloPersonId,
  findLeadByEmail,
} from "../../src/lib/leads-registry.js";

describe("leads-registry", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("resolveOrCreateLead", () => {
    it("finds existing lead by apolloPersonId and links email", async () => {
      vi.mocked(db.query.leads.findFirst).mockResolvedValue({
        id: "lead-1",
        apolloPersonId: "apollo-1",
        metadata: null,
        createdAt: new Date(),
      });

      const onConflictMock = vi.fn().mockResolvedValue(undefined);
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await resolveOrCreateLead({
        apolloPersonId: "apollo-1",
        email: "alice@acme.com",
      });

      expect(result).toEqual({ leadId: "lead-1", isNew: false });
      // Should insert email link
      expect(db.insert).toHaveBeenCalled();
    });

    it("finds existing lead by email when apolloPersonId not found", async () => {
      // No lead by apolloPersonId
      vi.mocked(db.query.leads.findFirst).mockResolvedValue(undefined);
      // Found by email
      vi.mocked(db.query.leadEmails.findFirst).mockResolvedValue({
        id: "le-1",
        leadId: "lead-2",
        email: "bob@acme.com",
        createdAt: new Date(),
      });

      // Update lead with apolloPersonId
      const whereMock = vi.fn().mockResolvedValue(undefined);
      const setMock = vi.fn().mockReturnValue({ where: whereMock });
      vi.mocked(db.update).mockReturnValue({ set: setMock } as never);

      const result = await resolveOrCreateLead({
        apolloPersonId: "apollo-2",
        email: "bob@acme.com",
      });

      expect(result).toEqual({ leadId: "lead-2", isNew: false });
      expect(db.update).toHaveBeenCalled();
    });

    it("creates new lead when neither apolloPersonId nor email found", async () => {
      vi.mocked(db.query.leads.findFirst).mockResolvedValue(undefined);
      vi.mocked(db.query.leadEmails.findFirst).mockResolvedValue(undefined);

      // Insert lead
      const returningMock = vi.fn().mockResolvedValue([{ id: "lead-new", apolloPersonId: "apollo-3", metadata: null, createdAt: new Date() }]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await resolveOrCreateLead({
        apolloPersonId: "apollo-3",
        email: "charlie@acme.com",
        metadata: { firstName: "Charlie" },
      });

      expect(result).toEqual({ leadId: "lead-new", isNew: true });
      // Should insert both lead and email
      expect(db.insert).toHaveBeenCalledTimes(2);
    });

    it("handles race condition on apolloPersonId conflict", async () => {
      vi.mocked(db.query.leads.findFirst)
        .mockResolvedValueOnce(undefined) // First check: not found
        .mockResolvedValueOnce({ id: "lead-raced", apolloPersonId: "apollo-4", metadata: null, createdAt: new Date() }); // Race recovery
      vi.mocked(db.query.leadEmails.findFirst).mockResolvedValue(undefined);

      // Insert returns empty (conflict)
      const returningMock = vi.fn().mockResolvedValue([]);
      const onConflictMock = vi.fn().mockReturnValue({ returning: returningMock });
      const valuesMock = vi.fn().mockReturnValue({ onConflictDoNothing: onConflictMock });
      vi.mocked(db.insert).mockReturnValue({ values: valuesMock } as never);

      const result = await resolveOrCreateLead({
        apolloPersonId: "apollo-4",
        email: "race@acme.com",
      });

      expect(result).toEqual({ leadId: "lead-raced", isNew: false });
    });
  });

  describe("findLeadByApolloPersonId", () => {
    it("returns leadId when found", async () => {
      vi.mocked(db.query.leads.findFirst).mockResolvedValue({
        id: "lead-1",
        apolloPersonId: "apollo-1",
        metadata: null,
        createdAt: new Date(),
      });

      const result = await findLeadByApolloPersonId("apollo-1");
      expect(result).toBe("lead-1");
    });

    it("returns null when not found", async () => {
      vi.mocked(db.query.leads.findFirst).mockResolvedValue(undefined);

      const result = await findLeadByApolloPersonId("nonexistent");
      expect(result).toBeNull();
    });
  });

  describe("findLeadByEmail", () => {
    it("returns leadId when found", async () => {
      vi.mocked(db.query.leadEmails.findFirst).mockResolvedValue({
        id: "le-1",
        leadId: "lead-1",
        email: "alice@acme.com",
        createdAt: new Date(),
      });

      const result = await findLeadByEmail("alice@acme.com");
      expect(result).toBe("lead-1");
    });

    it("returns null when not found", async () => {
      vi.mocked(db.query.leadEmails.findFirst).mockResolvedValue(undefined);

      const result = await findLeadByEmail("nonexistent@acme.com");
      expect(result).toBeNull();
    });
  });
});
