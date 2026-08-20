import { db } from "../db/index.js";
import { leadsCampaigns } from "../db/schema.js";
import { serveNext, type Person, type ServiceContext } from "./people-client.js";
import {
  upsertLeadFromPerson,
  recordEmploymentHistory,
  registerServedEmail,
} from "./leads-registry.js";
import { buildFullLead } from "./lead-shape.js";
import { getCurrentGoal } from "./brand-client.js";
import {
  AUDIENCE_EXHAUSTED_REASON,
  NO_AUDIENCE_REASON,
  SERVE_TIMED_OUT_REASON,
  type ServeEmptyReason,
} from "./serve-reasons.js";

interface PullNextParams {
  orgId: string;
  campaignId: string;
  /** All brand ids this serve is recorded against (leads_campaigns.brand_ids). */
  brandIds: string[];
  /** Primary brand the audience is resolved for (per-brand audiences). */
  brandId: string;
  featureSlug: string;
  parentRunId?: string | null;
  runId?: string | null;
  userId?: string | null;
  workflowSlug?: string;
  activeGoalId?: string | null;
  brandProfileId?: string | null;
  audienceId?: string | null;
}

interface PullNextResult {
  found: boolean;
  /**
   * Present on every empty answer, absent when a lead was served. Only
   * `audience_exhausted` says a population ran out — see serve-reasons.ts.
   */
  reason?: ServeEmptyReason;
  lead?: {
    leadId: string;
    email: string;
    data: unknown;
    brandIds: string[];
    orgId: string | null;
    userId: string | null;
    apolloPersonId: string | null;
    goal: string | null;
    activeGoalId: string | null;
    brandProfileId: string | null;
    audienceId: string | null;
  };
}

/**
 * Return the next real person to contact for the audience the campaign selected:
 *   1. Use the audience id the campaign passed in (x-audience-id header).
 *      campaign-service owns audience selection per run and propagates it down
 *      the workflow DAG; lead-service does NOT re-rank or re-select.
 *   2. Ask human-service serve-next for that audience's next unserved person.
 *   3. Record the person into lead-service silver (leads + leads_campaigns) and
 *      return it in the same FullLead shape the workflow already consumes.
 *
 * lead-service generates NO filters and takes NO provider — human-service owns
 * the audience's canonical filters, provider routing, and dedup/suppression.
 * No audience id (campaign selected none) or an exhausted audience surfaces
 * cleanly as found:false; real errors (serve-next non-2xx, network) fail loud.
 *
 * Every empty answer names WHY it is empty. Only `audience_exhausted` means a
 * population ran out — being told to serve no audience, or running out of time
 * before the look finished, is not evidence about anybody's population, and the
 * caller stops a campaign for good on the strength of that distinction.
 */
export async function pullNext(
  params: PullNextParams,
  signal?: AbortSignal,
): Promise<PullNextResult> {
  if (signal?.aborted) return { found: false, reason: SERVE_TIMED_OUT_REASON };

  // 1. The audience is selected by campaign-service per run and passed in via the
  // x-audience-id header. lead-service does NOT re-rank or re-select. No audience
  // id (campaign selected none) ⟹ clean found:false, no serve, no brand call.
  const audienceId = params.audienceId ?? null;
  if (!audienceId) {
    console.log(
      `[lead-service] pullNext found=false campaign=${params.campaignId} reason=${NO_AUDIENCE_REASON} brand=${params.brandId} feature=${params.featureSlug}`,
    );
    // Nothing was looked at: this service does not choose audiences, so an absent
    // x-audience-id means the caller named nobody to serve. NOT exhaustion.
    return { found: false, reason: NO_AUDIENCE_REASON };
  }

  const baseCtx: ServiceContext = {
    orgId: params.orgId,
    userId: params.userId ?? null,
    runId: params.runId ?? null,
    brandId: params.brandId,
    campaignId: params.campaignId,
    workflowSlug: params.workflowSlug,
    featureSlug: params.featureSlug,
    activeGoalId: params.activeGoalId ?? undefined,
    brandProfileId: params.brandProfileId ?? undefined,
    audienceId,
  };

  // 2. The goal belongs to the brand (brands.currentGoal), not the caller — read
  // it from brand-service for attribution/storage. No goal set ⟹ brand-service
  // 404 ⟹ this fails loud.
  const goal = await getCurrentGoal(params.brandId, params.orgId, baseCtx);
  const ctx: ServiceContext = { ...baseCtx, goal };

  if (signal?.aborted) return { found: false, reason: SERVE_TIMED_OUT_REASON };

  // 3. Next unserved person of that audience (human-service owns filters/provider/dedup).
  const served = await serveNext(audienceId, ctx);

  if (served.status === "exhausted" || !served.person) {
    console.log(
      `[lead-service] pullNext found=false campaign=${params.campaignId} reason=${AUDIENCE_EXHAUSTED_REASON} audienceId=${audienceId}`,
    );
    // The audience was walked and has nobody left — the one empty answer here that
    // is evidence about a population.
    return { found: false, reason: AUDIENCE_EXHAUSTED_REASON };
  }

  const person: Person = served.person;
  if (!person.email) {
    // serve-next promised a contactable person but gave no email — a producer
    // contract violation, not an empty result. Fail loud.
    throw new Error(
      `[lead-service] serve-next returned status=served without an email: audienceId=${audienceId}, campaign=${params.campaignId}`,
    );
  }

  // 4. Record into silver (leads + contact + organization + lifecycle row).
  //
  // A person is ONE identity: when this email already belongs to a lead, THAT
  // lead is the person and the serve is attributed to it. The global
  // one-email-one-lead index means no other lead can ever carry the email, so
  // serving a different lead would leave its delivery status permanently
  // unresolvable — invisible in the dashboard funnel, in outreach counts, and
  // in conversion attribution. `registerServedEmail` returns the owning lead,
  // and fails loud if the email cannot be registered at all.
  const resolvedLeadId = await upsertLeadFromPerson(person, { enriched: true });
  const leadId = await registerServedEmail({
    leadId: resolvedLeadId,
    email: person.email,
    status: person.emailStatus ?? null,
    source: person.provider,
  });
  await recordEmploymentHistory({ leadId, person });

  await db
    .insert(leadsCampaigns)
    .values({
      leadId,
      campaignId: params.campaignId,
      orgId: params.orgId,
      brandIds: params.brandIds,
      status: "served",
      statusReason: "served",
      statusDetails: `Served via audience ${audienceId}, email=${person.email}, leadId=${leadId}, campaign=${params.campaignId}`,
      servedAt: new Date(),
      parentRunId: params.parentRunId ?? null,
      runId: params.runId ?? null,
      pushRunId: params.runId ?? null,
      userId: params.userId ?? null,
      workflowSlug: params.workflowSlug ?? null,
      featureSlug: params.featureSlug ?? null,
      goal,
      activeGoalId: params.activeGoalId ?? null,
      brandProfileId: params.brandProfileId ?? null,
      audienceId: audienceId,
    })
    .onConflictDoNothing();

  const fullLead = await buildFullLead(leadId);

  console.log(
    `[lead-service] pullNext found=true campaign=${params.campaignId} audienceId=${audienceId} email=${person.email} leadId=${leadId}`,
  );

  return {
    found: true,
    lead: {
      leadId,
      email: person.email,
      data: fullLead,
      brandIds: params.brandIds,
      orgId: params.orgId,
      userId: params.userId ?? null,
      apolloPersonId: person.providerPersonId,
      goal,
      activeGoalId: params.activeGoalId ?? null,
      brandProfileId: params.brandProfileId ?? null,
      audienceId: audienceId,
    },
  };
}

export { leadsCampaigns };
