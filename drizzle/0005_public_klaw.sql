ALTER TABLE "enrichments" DROP CONSTRAINT "enrichments_email_unique";--> statement-breakpoint
ALTER TABLE "enrichments" ALTER COLUMN "email" DROP NOT NULL;