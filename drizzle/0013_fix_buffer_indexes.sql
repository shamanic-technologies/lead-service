-- Replace suboptimal composite index with one matching pullNext WHERE clause
-- Old: (org_id, namespace, status) — skips campaign_id, forcing heap lookups
-- New: (org_id, campaign_id, namespace, status) — exact match for pullNext query
DROP INDEX IF EXISTS "idx_buffer_org_ns_status";
--> statement-breakpoint
CREATE INDEX "idx_buffer_org_campaign_ns_status" ON "lead_buffer" USING btree ("org_id","campaign_id","namespace","status");
--> statement-breakpoint
-- Cover isInBuffer(orgId, campaignId, externalId) which had zero index support
CREATE INDEX "idx_buffer_org_campaign_extid" ON "lead_buffer" USING btree ("org_id","campaign_id","external_id");
