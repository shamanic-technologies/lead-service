import { z } from "zod";
import {
  OpenAPIRegistry,
  extendZodWithOpenApi,
} from "@asteasolutions/zod-to-openapi";
// The published `reason` enum IS the empty-serve vocabulary — read it from the one module
// that declares it, so the contract cannot drift from what the serve path actually returns.
import { SERVE_EMPTY_REASONS } from "./lib/serve-reasons.js";

extendZodWithOpenApi(z);

export const registry = new OpenAPIRegistry();

// --- Common ---

const ErrorResponseSchema = z
  .object({ error: z.string() })
  .openapi("ErrorResponse");

const AuthHeaders = [
  {
    in: "header" as const,
    name: "x-api-key",
    required: true,
    schema: { type: "string" as const },
    description: "API key for authenticating requests",
  },
  {
    in: "header" as const,
    name: "x-org-id",
    required: true,
    schema: { type: "string" as const },
    description: "Internal organization UUID from client-service",
  },
  {
    in: "header" as const,
    name: "x-user-id",
    required: true,
    schema: { type: "string" as const },
    description: "Internal user UUID from client-service",
  },
  {
    in: "header" as const,
    name: "x-run-id",
    required: true,
    schema: { type: "string" as const },
    description: "The caller's run ID (used as parentRunId when creating this service's own run)",
  },
  {
    in: "header" as const,
    name: "x-campaign-id",
    required: false,
    schema: { type: "string" as const },
    description: "Campaign identifier (auto-injected by workflow-service)",
  },
  {
    in: "header" as const,
    name: "x-brand-id",
    required: false,
    schema: { type: "string" as const },
    description: "Brand identifier(s), comma-separated for multi-brand campaigns (auto-injected by workflow-service). Example: uuid1,uuid2,uuid3",
  },
  {
    in: "header" as const,
    name: "x-workflow-slug",
    required: false,
    schema: { type: "string" as const },
    description: "Workflow slug (auto-injected by workflow-service)",
  },
  {
    in: "header" as const,
    name: "x-feature-slug",
    required: false,
    schema: { type: "string" as const },
    description: "Feature slug for tracking (propagated through the call chain)",
  },
  {
    in: "header" as const,
    name: "x-goal",
    required: false,
    schema: { type: "string" as const },
    description: "Active goal enum/name for the campaign activity, when explicitly tagged by the caller.",
  },
  {
    in: "header" as const,
    name: "x-active-goal-id",
    required: false,
    schema: { type: "string" as const },
    description: "Active goal identifier for the campaign activity, when explicitly tagged by the caller.",
  },
  {
    in: "header" as const,
    name: "x-brand-profile-id",
    required: false,
    schema: { type: "string" as const },
    description: "Brand profile identifier for persona-scoped attribution, when explicitly tagged by the caller.",
  },
  {
    in: "header" as const,
    name: "x-audience-id",
    required: false,
    schema: { type: "string" as const },
    description: "Audience identifier (human-service audience.id) for attribution, when explicitly tagged by the caller.",
  },
];

// buffer/next requires x-campaign-id and x-brand-id
const BufferNextHeaders = AuthHeaders.map((h) =>
  h.name === "x-campaign-id" || h.name === "x-brand-id"
    ? { ...h, required: true }
    : h
);

// --- Health ---

const HealthResponseSchema = z
  .object({
    status: z.string(),
    service: z.string(),
  })
  .openapi("HealthResponse");

// --- Canonical lead views ---
//
// Every lead-bearing endpoint returns the same canonical FullLead shape.
// Built from structured DB columns only — no Apollo raw blob, no metadata
// passthrough. Clients can rely on field names + types being stable across
// upstream provider changes.

const ContactMethodViewSchema = z
  .object({
    channel: z
      .string()
      .openapi({
        description:
          "Contact channel kind. Currently used: 'email', 'phone'. Stable identifier — case-sensitive.",
        example: "email",
      }),
    value: z
      .string()
      .openapi({
        description:
          "Contact value (the actual email address, phone number, etc.). Unique per (leadId, channel).",
        example: "sara@cascobay.com",
      }),
    status: z
      .string()
      .nullable()
      .openapi({
        description:
          "Provider-reported status of the contact value (e.g. 'verified', 'unverified', 'extrapolated' for emails). null when not classified.",
        example: "verified",
      }),
    source: z
      .string()
      .openapi({
        description:
          "Where this contact method originated (e.g. 'apollo', 'manual', 'csv-upload').",
        example: "apollo",
      }),
  })
  .openapi("ContactMethodView", {
    description:
      "One contact endpoint attached to a lead — email, phone, or any other channel. Multiple rows per lead are possible.",
    example: {
      channel: "email",
      value: "sara@cascobay.com",
      status: "verified",
      source: "apollo",
    },
  });

const FundingEventSchema = z
  .object({
    id: z
      .string()
      .nullable()
      .openapi({
        description: "Apollo-assigned identifier for the funding event.",
        example: "fund_5f2a3b4c5d6e7f8a9b0c1d2e",
      }),
    date: z
      .string()
      .nullable()
      .openapi({
        description: "ISO date (YYYY-MM-DD) of the funding event.",
        example: "2024-06-01",
      }),
    type: z
      .string()
      .nullable()
      .openapi({
        description: "Funding round type (e.g. 'Seed', 'Series A', 'Series B').",
        example: "Series A",
      }),
    investors: z
      .string()
      .nullable()
      .openapi({
        description: "Comma-separated list of investors as reported by Apollo.",
        example: "Acme VC, Foo Capital",
      }),
    amount: z
      .number()
      .nullable()
      .openapi({
        description: "Amount raised in this round, in the round's currency.",
        example: 5000000,
      }),
    currency: z
      .string()
      .nullable()
      .openapi({
        description: "ISO 4217 currency code for the amount.",
        example: "USD",
      }),
    newsUrl: z
      .string()
      .nullable()
      .openapi({
        description:
          "URL to a news article announcing this funding event. Mapped from Apollo's snake_case `news_url` to camelCase for consistency with the rest of the API surface.",
        example: "https://techcrunch.com/2024/06/01/casco-bay-series-a",
      }),
  })
  .openapi("FundingEvent", {
    description:
      "One funding round attached to an organization. All fields are nullable because Apollo's coverage is best-effort.",
    example: {
      id: "fund_5f2a3b4c5d6e7f8a9b0c1d2e",
      date: "2024-06-01",
      type: "Series A",
      investors: "Acme VC, Foo Capital",
      amount: 5000000,
      currency: "USD",
      newsUrl: "https://techcrunch.com/2024/06/01/casco-bay-series-a",
    },
  });

const OrganizationViewSchema = z
  .object({
    id: z
      .string()
      .uuid()
      .openapi({
        description: "Internal organization UUID (lead-service registry).",
        example: "10000000-0000-0000-0000-000000000001",
      }),
    apolloOrganizationId: z
      .string()
      .nullable()
      .openapi({
        description: "Apollo organization ID — present when sourced from Apollo enrichment.",
        example: "5f2a3b4c5d6e7f8a9b0c1d2e",
      }),
    name: z
      .string()
      .nullable()
      .openapi({
        description: "Company name as registered. Use this for recipientCompany on outbound email.",
        example: "Casco Bay",
      }),
    primaryDomain: z
      .string()
      .nullable()
      .openapi({
        description: "Primary domain of the company (no protocol). Useful for domain-level deliverability or matching.",
        example: "cascobay.com",
      }),
    websiteUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Canonical company website URL (with protocol).",
        example: "https://cascobay.com",
      }),
    industry: z
      .string()
      .nullable()
      .openapi({
        description: "Primary industry classification.",
        example: "marketing",
      }),
    estimatedNumEmployees: z
      .number()
      .int()
      .nullable()
      .openapi({
        description: "Estimated employee count.",
        example: 12,
      }),
    annualRevenue: z
      .string()
      .nullable()
      .openapi({
        description:
          "Annual revenue (USD), serialized as a numeric string to avoid float precision loss for very large companies.",
        example: "1000000",
      }),
    logoUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Logo image URL.",
        example: "https://logo.clearbit.com/cascobay.com",
      }),
    shortDescription: z
      .string()
      .nullable()
      .openapi({
        description: "Short marketing-style description of the company.",
        example: "Boutique digital marketing agency in Portland, ME.",
      }),
    linkedinUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Company LinkedIn URL.",
        example: "https://linkedin.com/company/cascobay",
      }),
    twitterUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Company Twitter/X URL.",
        example: "https://twitter.com/cascobay",
      }),
    facebookUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Company Facebook URL.",
        example: "https://facebook.com/cascobay",
      }),
    blogUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Company blog URL.",
        example: "https://cascobay.com/blog",
      }),
    crunchbaseUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Crunchbase profile URL.",
        example: "https://crunchbase.com/organization/cascobay",
      }),
    foundedYear: z
      .number()
      .int()
      .nullable()
      .openapi({
        description: "Year the company was founded.",
        example: 2018,
      }),
    city: z
      .string()
      .nullable()
      .openapi({
        description: "Company HQ city.",
        example: "Portland",
      }),
    state: z
      .string()
      .nullable()
      .openapi({
        description: "Company HQ state / province (ISO subdivision when available).",
        example: "ME",
      }),
    country: z
      .string()
      .nullable()
      .openapi({
        description: "Company HQ country.",
        example: "USA",
      }),
    streetAddress: z
      .string()
      .nullable()
      .openapi({
        description: "Company HQ street address.",
        example: "123 Main St",
      }),
    postalCode: z
      .string()
      .nullable()
      .openapi({
        description: "Company HQ postal / ZIP code.",
        example: "04101",
      }),
    technologyNames: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Technologies the company is known to use (e.g. 'GA4', 'Salesforce').",
        example: ["GA4", "HubSpot"],
      }),
    industries: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "All industry classifications attached to this company.",
        example: ["marketing", "advertising"],
      }),
    secondaryIndustries: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Secondary industry classifications.",
        example: ["digital-marketing"],
      }),
    latestFundingStage: z
      .string()
      .nullable()
      .openapi({
        description:
          "Most recent funding stage label as reported by Apollo (e.g. 'seed', 'series_a', 'series_b'). null when never funded or unknown.",
        example: "series_a",
      }),
    latestFundingRoundDate: z
      .string()
      .nullable()
      .openapi({
        description: "ISO date (YYYY-MM-DD) of the most recent funding round. null when unknown.",
        example: "2024-06-01",
      }),
    totalFunding: z
      .string()
      .nullable()
      .openapi({
        description:
          "Total funding raised, in USD, serialized as a numeric string to avoid float precision loss for very large amounts.",
        example: "5000000",
      }),
    totalFundingPrinted: z
      .string()
      .nullable()
      .openapi({
        description: "Human-friendly total-funding string from Apollo (e.g. '$5M', '$1.2B').",
        example: "$5M",
      }),
    fundingEvents: z
      .array(FundingEventSchema)
      .openapi({
        description:
          "Per-round funding history. Empty array when no funding events are known. Apollo's snake_case `news_url` is mapped to camelCase `newsUrl` for API consistency.",
        example: [
          {
            id: "fund_5f2a3b4c5d6e7f8a9b0c1d2e",
            date: "2024-06-01",
            type: "Series A",
            investors: "Acme VC, Foo Capital",
            amount: 5000000,
            currency: "USD",
            newsUrl: "https://techcrunch.com/2024/06/01/casco-bay-series-a",
          },
        ],
      }),
    retailLocationCount: z
      .number()
      .int()
      .nullable()
      .openapi({
        description: "Number of physical retail locations the organization operates.",
        example: 3,
      }),
    publiclyTradedSymbol: z
      .string()
      .nullable()
      .openapi({
        description:
          "Stock ticker symbol when the company is publicly traded. null for private companies.",
        example: "AAPL",
      }),
    publiclyTradedExchange: z
      .string()
      .nullable()
      .openapi({
        description:
          "Stock exchange where the company is listed (e.g. 'NASDAQ', 'NYSE'). null for private companies.",
        example: "NASDAQ",
      }),
    primaryPhone: z
      .string()
      .nullable()
      .openapi({
        description: "Primary phone number for the company (E.164 when available).",
        example: "+15555550100",
      }),
    seoDescription: z
      .string()
      .nullable()
      .openapi({
        description:
          "Long-form SEO meta description scraped from the company's website. Distinct from `shortDescription` (which is editorial / Apollo-curated).",
        example: "Casco Bay is a boutique digital marketing agency based in Portland, Maine.",
      }),
    angellistUrl: z
      .string()
      .nullable()
      .openapi({
        description: "AngelList / Wellfound profile URL.",
        example: "https://angel.co/cascobay",
      }),
    numSuborganizations: z
      .number()
      .int()
      .nullable()
      .openapi({
        description: "Count of subsidiaries / sub-organizations associated with this company.",
        example: 0,
      }),
    alexaRanking: z
      .number()
      .int()
      .nullable()
      .openapi({
        description: "Alexa global website rank (smaller = more popular). null when unranked.",
        example: 250000,
      }),
    keywords: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Free-form keywords that describe the company (Apollo-curated).",
        example: ["marketing", "branding", "digital"],
      }),
  })
  .openapi("OrganizationView", {
    description:
      "Snapshot of the lead's CURRENT employer organization, joined from leads_organizations where current=true. " +
      "All fields are nullable because organization enrichment is best-effort. " +
      "null at the parent level means the lead has no current employment record.",
    example: {
      id: "10000000-0000-0000-0000-000000000001",
      apolloOrganizationId: "5f2a3b4c5d6e7f8a9b0c1d2e",
      name: "Casco Bay",
      primaryDomain: "cascobay.com",
      websiteUrl: "https://cascobay.com",
      industry: "marketing",
      estimatedNumEmployees: 12,
      annualRevenue: "1000000",
      logoUrl: "https://logo.clearbit.com/cascobay.com",
      shortDescription: "Boutique digital marketing agency in Portland, ME.",
      linkedinUrl: "https://linkedin.com/company/cascobay",
      twitterUrl: null,
      facebookUrl: null,
      blogUrl: null,
      crunchbaseUrl: null,
      foundedYear: 2018,
      city: "Portland",
      state: "ME",
      country: "USA",
      streetAddress: null,
      postalCode: "04101",
      technologyNames: ["GA4", "HubSpot"],
      industries: ["marketing", "advertising"],
      secondaryIndustries: null,
      latestFundingStage: "series_a",
      latestFundingRoundDate: "2024-06-01",
      totalFunding: "5000000",
      totalFundingPrinted: "$5M",
      fundingEvents: [
        {
          id: "fund_5f2a3b4c5d6e7f8a9b0c1d2e",
          date: "2024-06-01",
          type: "Series A",
          investors: "Acme VC, Foo Capital",
          amount: 5000000,
          currency: "USD",
          newsUrl: "https://techcrunch.com/2024/06/01/casco-bay-series-a",
        },
      ],
      retailLocationCount: null,
      publiclyTradedSymbol: null,
      publiclyTradedExchange: null,
      primaryPhone: "+15555550100",
      seoDescription: "Casco Bay is a boutique digital marketing agency based in Portland, Maine.",
      angellistUrl: null,
      numSuborganizations: 0,
      alexaRanking: 250000,
      keywords: ["marketing", "branding", "digital"],
    },
  });

const EmploymentEntryViewSchema = z
  .object({
    organizationId: z
      .string()
      .uuid()
      .openapi({
        description: "Internal organization UUID for this employment row.",
        example: "10000000-0000-0000-0000-000000000001",
      }),
    organizationName: z
      .string()
      .nullable()
      .openapi({
        description: "Organization name at time of join. May differ from current name if company was renamed.",
        example: "Casco Bay",
      }),
    title: z
      .string()
      .nullable()
      .openapi({
        description: "Role title held during this employment.",
        example: "Founder",
      }),
    startDate: z
      .string()
      .nullable()
      .openapi({
        description: "ISO date (YYYY-MM-DD) when this employment started. null when unknown.",
        example: "2018-01-01",
      }),
    endDate: z
      .string()
      .nullable()
      .openapi({
        description: "ISO date (YYYY-MM-DD) when this employment ended. null when current or unknown.",
        example: null,
      }),
    current: z
      .boolean()
      .openapi({
        description: "True when this is the lead's current employment.",
        example: true,
      }),
    description: z
      .string()
      .nullable()
      .openapi({
        description: "Free-form description of the role.",
        example: "Leads strategy and operations.",
      }),
  })
  .openapi("EmploymentEntryView", {
    description:
      "One employment row from the lead's career history. All rows from leads_organizations are returned (past + current).",
    example: {
      organizationId: "10000000-0000-0000-0000-000000000001",
      organizationName: "Casco Bay",
      title: "Founder",
      startDate: "2018-01-01",
      endDate: null,
      current: true,
      description: "Leads strategy and operations.",
    },
  });

export const FullLeadSchema = z
  .object({
    leadId: z
      .string()
      .uuid()
      .openapi({
        description: "Internal lead UUID (lead-service registry). Stable across enrichment refreshes.",
        example: "00000000-0000-0000-0000-000000000001",
      }),
    apolloPersonId: z
      .string()
      .nullable()
      .openapi({
        description: "Apollo person ID — present when the lead was sourced or enriched via Apollo.",
        example: "5f2a3b4c5d6e7f8a9b0c1d2e",
      }),
    firstName: z
      .string()
      .openapi({
        description:
          "Lead's first name. Required — lead-service refuses to register a lead without one. " +
          "Use this for recipientFirstName on outbound email.",
        example: "Sara",
      }),
    lastName: z
      .string()
      .openapi({
        description:
          "Lead's last name. Required. Use this for recipientLastName on outbound email.",
        example: "Freshley",
      }),
    name: z
      .string()
      .nullable()
      .openapi({
        description: "Full display name as provided by source (often 'firstName lastName' but not always).",
        example: "Sara Freshley",
      }),
    headline: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's professional headline / current role line (e.g. LinkedIn-style headline).",
        example: "Founder at Casco Bay",
      }),
    linkedinUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's LinkedIn profile URL.",
        example: "https://linkedin.com/in/sara-freshley",
      }),
    photoUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's profile photo URL.",
        example: "https://media.licdn.com/photo.jpg",
      }),
    city: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's city.",
        example: "Portland",
      }),
    state: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's state / province.",
        example: "ME",
      }),
    country: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's country.",
        example: "USA",
      }),
    timezone: z
      .string()
      .nullable()
      .openapi({
        description:
          "Recipient's IANA timezone (e.g. 'America/New_York'), resolved upstream from the lead's location. " +
          "Forward this to the send path so cold email is scheduled in the recipient's local business hours " +
          "(email-gateway-service → instantly-service). null when upstream provides none — the send path falls back to a safe default.",
        example: "America/New_York",
      }),
    businessLanguages: z
      .array(z.string())
      .nullable()
      .openapi({
        description:
          "Language(s) this lead plausibly conducts business in, as ISO 639-1 codes (e.g. 'de', 'fr', 'it'). " +
          "ORDERED, most plausible first — the ordering is a guarantee, so a consumer may select by position " +
          "(index 0 = the single most plausible business language). Produced and owned by human-service; " +
          "lead-service carries it through unchanged and never derives it. An EMPTY array means UNKNOWN — the " +
          "producer had no usable signal and deliberately does not fabricate one, which is distinct from ['en'] " +
          "(= known to be English). null means the lead predates this field being carried, and is equally not a guess.",
        example: ["de", "en"],
      }),
    seniority: z
      .string()
      .nullable()
      .openapi({
        description: "Seniority bucket from enrichment (e.g. 'founder', 'director', 'vp').",
        example: "founder",
      }),
    departments: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Department classifications.",
        example: ["c_suite"],
      }),
    subdepartments: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Subdepartment classifications.",
        example: ["founders"],
      }),
    functions: z
      .array(z.string())
      .nullable()
      .openapi({
        description: "Job function classifications.",
        example: ["entrepreneurship"],
      }),
    twitterUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's Twitter / X profile URL.",
        example: "https://twitter.com/sara",
      }),
    githubUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's GitHub profile URL.",
        example: "https://github.com/sara",
      }),
    facebookUrl: z
      .string()
      .nullable()
      .openapi({
        description: "Lead's Facebook profile URL.",
        example: "https://facebook.com/sara",
      }),
    enrichedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "ISO 8601 timestamp of last successful enrichment. null when the lead was registered without enrichment.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    currentTitle: z
      .string()
      .nullable()
      .openapi({
        description:
          "Lead's current role title — derived from the leads_organizations row where current=true. " +
          "Mirrors `employmentHistory[].title` for the current entry; surfaced top-level for convenient template binding " +
          "(e.g. `recipientTitle` on outbound email). null when the lead has no current employment row or the row has no title.",
        example: "Founder",
      }),
    organization: OrganizationViewSchema.nullable(),
    contacts: z
      .array(ContactMethodViewSchema)
      .openapi({
        description: "All contact methods attached to this lead — email, phone, etc. May be empty.",
        example: [
          { channel: "email", value: "sara@cascobay.com", status: "verified", source: "apollo" },
        ],
      }),
    employmentHistory: z
      .array(EmploymentEntryViewSchema)
      .openapi({
        description:
          "Full employment history (current + past). Returned in insertion order; check the `current` flag to find the present role.",
        example: [
          {
            organizationId: "10000000-0000-0000-0000-000000000001",
            organizationName: "Casco Bay",
            title: "Founder",
            startDate: "2018-01-01",
            endDate: null,
            current: true,
            description: null,
          },
        ],
      }),
  })
  .openapi("FullLead", {
    description:
      "Canonical lead representation returned by every lead-bearing endpoint. Built entirely from structured columns — there is no `metadata` or `raw` Apollo passthrough. Field names are stable regardless of upstream enrichment provider.",
    example: {
      leadId: "00000000-0000-0000-0000-000000000001",
      apolloPersonId: "5f2a3b4c5d6e7f8a9b0c1d2e",
      firstName: "Sara",
      lastName: "Freshley",
      name: "Sara Freshley",
      headline: "Founder at Casco Bay",
      linkedinUrl: "https://linkedin.com/in/sara-freshley",
      photoUrl: null,
      city: "Portland",
      state: "ME",
      country: "USA",
      timezone: "America/New_York",
      seniority: "founder",
      departments: ["c_suite"],
      subdepartments: ["founders"],
      functions: ["entrepreneurship"],
      twitterUrl: null,
      githubUrl: null,
      facebookUrl: null,
      enrichedAt: "2026-01-01T00:00:00.000Z",
      currentTitle: "Founder",
      organization: {
        id: "10000000-0000-0000-0000-000000000001",
        apolloOrganizationId: "5f2a3b4c5d6e7f8a9b0c1d2e",
        name: "Casco Bay",
        primaryDomain: "cascobay.com",
        websiteUrl: "https://cascobay.com",
        industry: "marketing",
        estimatedNumEmployees: 12,
        annualRevenue: "1000000",
        logoUrl: "https://logo.clearbit.com/cascobay.com",
        shortDescription: "Boutique digital marketing agency in Portland, ME.",
        linkedinUrl: "https://linkedin.com/company/cascobay",
        twitterUrl: null,
        facebookUrl: null,
        blogUrl: null,
        crunchbaseUrl: null,
        foundedYear: 2018,
        city: "Portland",
        state: "ME",
        country: "USA",
        streetAddress: null,
        postalCode: "04101",
        technologyNames: ["GA4", "HubSpot"],
        industries: ["marketing", "advertising"],
        secondaryIndustries: null,
        latestFundingStage: "series_a",
        latestFundingRoundDate: "2024-06-01",
        totalFunding: "5000000",
        totalFundingPrinted: "$5M",
        fundingEvents: [
          {
            id: "fund_5f2a3b4c5d6e7f8a9b0c1d2e",
            date: "2024-06-01",
            type: "Series A",
            investors: "Acme VC, Foo Capital",
            amount: 5000000,
            currency: "USD",
            newsUrl: "https://techcrunch.com/2024/06/01/casco-bay-series-a",
          },
        ],
        retailLocationCount: null,
        publiclyTradedSymbol: null,
        publiclyTradedExchange: null,
        primaryPhone: "+15555550100",
        seoDescription: "Casco Bay is a boutique digital marketing agency based in Portland, Maine.",
        angellistUrl: null,
        numSuborganizations: 0,
        alexaRanking: 250000,
        keywords: ["marketing", "branding", "digital"],
      },
      contacts: [
        { channel: "email", value: "sara@cascobay.com", status: "verified", source: "apollo" },
      ],
      employmentHistory: [
        {
          organizationId: "10000000-0000-0000-0000-000000000001",
          organizationName: "Casco Bay",
          title: "Founder",
          startDate: "2018-01-01",
          endDate: null,
          current: true,
          description: null,
        },
      ],
    },
  });

// --- Buffer Next ---

export const BufferNextRequestSchema = z
  .object({})
  .openapi("BufferNextRequest", {
    description:
      "Empty body. The brand, feature, goal, and run identity are read from headers; lead-service resolves the audience (features-service) and serves the next person (human-service). No filters and no provider are accepted — human-service owns both.",
  });

const ServedLeadSchema = z
  .object({
    leadId: z
      .string()
      .uuid()
      .openapi({
        description:
          "Internal lead UUID. Same as data.leadId — kept at the top level for backwards compatibility with workflow scripts that read it directly.",
        example: "00000000-0000-0000-0000-000000000001",
      }),
    email: z
      .string()
      .openapi({
        description:
          "The email address selected for outreach. Always populated when found=true. Same address appears in data.contacts for the 'email' channel.",
        example: "sara@cascobay.com",
      }),
    data: FullLeadSchema,
    brandIds: z
      .array(z.string())
      .openapi({
        description: "Brand UUIDs this lead was buffered for (echoed back from x-brand-id header).",
        example: ["20000000-0000-0000-0000-000000000001"],
      }),
    orgId: z
      .string()
      .nullable()
      .openapi({
        description: "Internal organization UUID owning the campaign.",
        example: "30000000-0000-0000-0000-000000000001",
      }),
    userId: z
      .string()
      .nullable()
      .openapi({
        description: "Internal user UUID who triggered the campaign run.",
        example: "40000000-0000-0000-0000-000000000001",
      }),
    apolloPersonId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Apollo person ID — same value as data.apolloPersonId.",
        example: "5f2a3b4c5d6e7f8a9b0c1d2e",
      }),
    goal: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit active goal tag for this served lead. null means unattributed.",
        example: "signup",
      }),
    activeGoalId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit active goal ID tag for this served lead. null means unattributed.",
        example: "goal_123",
      }),
    brandProfileId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit brand profile ID tag for this served lead. null means unattributed.",
        example: "brand_profile_123",
      }),
    audienceId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Audience ID (human-service audience.id) this served lead is attributed to. null means unattributed.",
        example: "audience_123",
      }),
  })
  .openapi("ServedLead", {
    description:
      "A single lead served from the campaign buffer. The full lead payload lives under `data` (FullLead shape).",
  });

export const BufferNextResponseSchema = z
  .object({
    found: z
      .boolean()
      .openapi({
        description: "True when a lead was claimed and returned. False when no lead can be served right now.",
        example: true,
      }),
    lead: ServedLeadSchema.optional(),
    reason: z
      .enum(SERVE_EMPTY_REASONS)
      .optional()
      .openapi({
        description:
          "Why the answer is empty. Always present when found=false; absent when a lead was served. " +
          "ONLY `audience_exhausted` is evidence that a population ran out — human-service walked the " +
          "audience this service was told to serve and reported nobody left. Every other value states " +
          "that no exhaustion was observed: `no_audience` means the caller sent no x-audience-id, so " +
          "this service (which never picks audiences) looked at nobody; `serve_timed_out` means the " +
          "serve budget expired before the look finished; `audience_not_serveable` means the named " +
          "audience has no committed provider yet, so its population is unknown rather than empty; " +
          "`credit_insufficient` means the org has no platform credit, so no paid enrichment/search/LLM " +
          "action was performed. Decide whether outreach may stop by testing for `audience_exhausted` " +
          "specifically — never by excluding known-benign values, so a reason added later defaults to " +
          "not-exhaustion.",
        example: "audience_exhausted",
      }),
  })
  .openapi("BufferNextResponse", {
    description:
      "Response from POST /orgs/buffer/next. When found is true, lead contains the served lead with full canonical FullLead payload under `lead.data`.",
    examples: [
      {
        summary: "Apollo lead",
        value: {
          found: true,
          lead: {
            leadId: "00000000-0000-0000-0000-000000000001",
            email: "sara@cascobay.com",
            data: {
              leadId: "00000000-0000-0000-0000-000000000001",
              apolloPersonId: "5f2a3b4c5d6e7f8a9b0c1d2e",
              firstName: "Sara",
              lastName: "Freshley",
              name: "Sara Freshley",
              headline: "Founder at Casco Bay",
              linkedinUrl: "https://linkedin.com/in/sara-freshley",
              photoUrl: null,
              city: "Portland",
              state: "ME",
              country: "USA",
              timezone: "America/New_York",
              seniority: "founder",
              departments: ["c_suite"],
              subdepartments: null,
              functions: null,
              twitterUrl: null,
              githubUrl: null,
              facebookUrl: null,
              enrichedAt: "2026-01-01T00:00:00.000Z",
              currentTitle: "Founder",
              organization: {
                id: "10000000-0000-0000-0000-000000000001",
                apolloOrganizationId: "5f2a3b4c5d6e7f8a9b0c1d2e",
                name: "Casco Bay",
                primaryDomain: "cascobay.com",
                websiteUrl: "https://cascobay.com",
                industry: "marketing",
                estimatedNumEmployees: 12,
                annualRevenue: "1000000",
                logoUrl: null,
                shortDescription: null,
                linkedinUrl: null,
                twitterUrl: null,
                facebookUrl: null,
                blogUrl: null,
                crunchbaseUrl: null,
                foundedYear: 2018,
                city: "Portland",
                state: "ME",
                country: "USA",
                streetAddress: null,
                postalCode: null,
                technologyNames: ["GA4"],
                industries: ["marketing"],
                secondaryIndustries: null,
                latestFundingStage: "series_a",
                latestFundingRoundDate: "2024-06-01",
                totalFunding: "5000000",
                totalFundingPrinted: "$5M",
                fundingEvents: [],
                retailLocationCount: null,
                publiclyTradedSymbol: null,
                publiclyTradedExchange: null,
                primaryPhone: null,
                seoDescription: null,
                angellistUrl: null,
                numSuborganizations: null,
                alexaRanking: null,
                keywords: null,
              },
              contacts: [
                { channel: "email", value: "sara@cascobay.com", status: "verified", source: "apollo" },
              ],
              employmentHistory: [
                {
                  organizationId: "10000000-0000-0000-0000-000000000001",
                  organizationName: "Casco Bay",
                  title: "Founder",
                  startDate: "2018-01-01",
                  endDate: null,
                  current: true,
                  description: null,
                },
              ],
            },
            brandIds: ["20000000-0000-0000-0000-000000000001"],
            orgId: "30000000-0000-0000-0000-000000000001",
            userId: "40000000-0000-0000-0000-000000000001",
            apolloPersonId: "5f2a3b4c5d6e7f8a9b0c1d2e",
          },
        },
      },
      {
        summary: "Buffer exhausted",
        value: {
          found: false,
        },
      },
      {
        summary: "Insufficient credits",
        value: {
          found: false,
          reason: "credit_insufficient",
        },
      },
    ],
  });

// --- Leads ---

const LeadDetailSchema = z
  .object({
    id: z
      .string()
      .uuid()
      .openapi({
        description: "leads_campaigns row UUID (per-campaign per-lead lifecycle row, NOT the lead itself).",
        example: "50000000-0000-0000-0000-000000000001",
      }),
    leadId: z
      .string()
      .uuid()
      .nullable()
      .openapi({
        description: "Internal lead UUID. Null only when the row references a lead that was deleted.",
        example: "00000000-0000-0000-0000-000000000001",
      }),
    namespace: z
      .string()
      .openapi({
        description: "Namespace this lead was sourced from. Currently always 'apollo'.",
        example: "apollo",
      }),
    email: z
      .string()
      .openapi({
        description: "The email address tied to this leads_campaigns row.",
        example: "sara@cascobay.com",
      }),
    status: z
      .enum(["buffered", "skipped", "claimed", "served"])
      .openapi({
        description:
          "Lead lifecycle status in this campaign. 'buffered'/'skipped'/'claimed'/'served' all live in leads_campaigns; 'served' = pulled and served to a workflow.",
        example: "served",
      }),
    statusReason: z
      .string()
      .nullable()
      .openapi({
        description:
          "Why this lead is in its current status (e.g. 'already_contacted', 'bounced'). Set for skipped/buffered leads.",
        example: "already_contacted",
      }),
    statusDetails: z
      .string()
      .nullable()
      .openapi({
        description: "Human-readable details about the status reason.",
        example: "Lead was contacted in campaign abc-123 on 2026-01-01.",
      }),
    parentRunId: z
      .string()
      .nullable()
      .openapi({
        description: "Run ID of the workflow that pulled / processed this lead.",
        example: "run-uuid",
      }),
    runId: z
      .string()
      .nullable()
      .openapi({
        description: "Run ID for the campaign-tick that produced this lead.",
        example: "run-uuid",
      }),
    brandIds: z
      .array(z.string())
      .openapi({
        description: "Brand UUIDs this lead was buffered for.",
        example: ["20000000-0000-0000-0000-000000000001"],
      }),
    campaignId: z
      .string()
      .openapi({
        description: "Campaign ID owning this leads_campaigns row.",
        example: "60000000-0000-0000-0000-000000000001",
      }),
    orgId: z
      .string()
      .openapi({
        description: "Internal organization UUID.",
        example: "30000000-0000-0000-0000-000000000001",
      }),
    userId: z
      .string()
      .nullable()
      .openapi({
        description: "Internal user UUID who triggered the campaign run.",
        example: "40000000-0000-0000-0000-000000000001",
      }),
    workflowSlug: z
      .string()
      .nullable()
      .openapi({
        description: "Workflow slug that processed this lead (e.g. 'sales-cold-email-outreach-helium').",
        example: "sales-cold-email-outreach-helium",
      }),
    featureSlug: z
      .string()
      .nullable()
      .openapi({
        description: "Feature slug for tracking.",
        example: "outreach",
      }),
    goal: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit active goal tag stored on the leads_campaigns row. null means unattributed.",
        example: "signup",
      }),
    activeGoalId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit active goal ID stored on the leads_campaigns row. null means unattributed.",
        example: "goal_123",
      }),
    brandProfileId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Explicit brand profile ID stored on the leads_campaigns row. null means unattributed.",
        example: "brand_profile_123",
      }),
    offer: z
      .object({
        id: z
          .string()
          .openapi({
            description: "Offer UUID (brand-service offer.offerId).",
            example: "0ffe0000-0000-4000-8000-000000000001",
          }),
        name: z
          .string()
          .nullable()
          .openapi({
            description:
              "Offer display name, from brand-service. null when the campaign names an offer " +
              "brand-service does not list back (a deleted offer, or a brand that could not be " +
              "reached) — the id is still true, so it is stated rather than dropping the offer.",
            example: "Fractional CFO retainer",
          }),
      })
      .nullable()
      .openapi({
        description:
          "The OFFER this lead belongs to — brand-service's proposition level, between the brand " +
          "and the campaign. Resolved server-side as the offer named by the campaign the lead was " +
          "served under (`campaignId` above, the attribution frozen on the leads_campaigns row), " +
          "with its name read from brand-service. null when that campaign names no offer, and also " +
          "when the resolution was unavailable (logged loudly server-side) — never inferred from " +
          "the lead's brand, its funnel or a sibling campaign. Present on every lead in both views.",
      }),
    audienceId: z
      .string()
      .nullable()
      .optional()
      .openapi({
        description: "Audience ID (human-service audience.id) stored on the leads_campaigns row. null means unattributed.",
        example: "audience_123",
      }),
    audience: z
      .object({
        id: z.string().openapi({ description: "Audience UUID (human-service audience.id).", example: "audience_123" }),
        name: z.string().openapi({ description: "Audience display name.", example: "US SaaS founders" }),
        avatarUrl: z
          .string()
          .nullable()
          .openapi({ description: "Audience avatar URL. null when the audience has no avatar yet.", example: "https://cdn.example.com/aud.png" }),
      })
      .nullable()
      .openapi({
        description:
          "The lead's ACTIVE audience for this brand, resolved server-side by human-service " +
          "(by tagged audience_id and/or by email → active-audience membership, brand-correct). " +
          "null when the lead belongs to no active audience for the brand. Present on every lead in both views.",
      }),
    servedAt: z
      .string()
      .nullable()
      .openapi({
        description: "ISO timestamp when this lead was served. null for buffered/skipped/claimed rows.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    apolloPersonId: z
      .string()
      .nullable()
      .openapi({
        description: "Apollo person ID — convenience copy of lead.apolloPersonId.",
        example: "5f2a3b4c5d6e7f8a9b0c1d2e",
      }),
    emailStatus: z
      .string()
      .nullable()
      .openapi({
        description: "Email verification status from Apollo (verified, unverified, extrapolated, etc.).",
        example: "verified",
      }),
    lead: FullLeadSchema.nullable(),
    contacted: z
      .boolean()
      .openapi({
        description: "Lead has been contacted at least once in this scope (campaign or brand depending on query).",
        example: true,
      }),
    sent: z
      .boolean()
      .openapi({
        description: "An email send has been attempted.",
        example: true,
      }),
    sentCount: z
      .number()
      .openapi({
        description:
          "Count of emails actually sent to this lead in the outreach sequence " +
          "(initial + follow-ups), summed across providers. Passed through from " +
          "email-gateway delivery status, scoped identically to `sent` " +
          "(brand-scoped when brandId is passed, campaign-scoped when campaignId is passed). " +
          "0 when no send has occurred (or the source count is absent).",
        example: 2,
      }),
    delivered: z
      .boolean()
      .openapi({
        description: "Provider confirmed delivery.",
        example: true,
      }),
    opened: z
      .boolean()
      .openapi({
        description: "Lead has opened at least one email.",
        example: false,
      }),
    clicked: z
      .boolean()
      .openapi({
        description: "Lead has clicked at least one tracked link.",
        example: false,
      }),
    bounced: z
      .boolean()
      .openapi({
        description: "Email bounced.",
        example: false,
      }),
    unsubscribed: z
      .boolean()
      .openapi({
        description: "Lead unsubscribed in this scope.",
        example: false,
      }),
    replied: z
      .boolean()
      .openapi({
        description: "Whether the lead replied (any reply, regardless of sentiment).",
        example: false,
      }),
    replyClassification: z
      .enum(["positive", "negative", "neutral"])
      .nullable()
      .openapi({
        description:
          "Classification of the most recent reply from email-gateway. " +
          "'positive' = interested or willing to meet, " +
          "'negative' = not interested, " +
          "'neutral' = ambiguous or informational. " +
          "null when no reply detected.",
        example: null,
      }),
    lastDeliveredAt: z
      .string()
      .nullable()
      .openapi({
        description: "ISO timestamp of the last delivered message in this scope.",
        example: "2026-01-02T00:00:00.000Z",
      }),
    firstClickedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a click in this scope; " +
          "null if the lead never clicked in scope. Scoped identically to `clicked` " +
          "(brand-scoped when brandId is passed, campaign-scoped when campaignId is passed).",
        example: "2026-01-02T00:00:00.000Z",
      }),
    firstContactedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a contacted event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status. " +
          "For building the per-lead event timeline.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstSentAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a sent event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstDeliveredAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a delivered event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstOpenedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of an opened event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstRepliedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a replied event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstBouncedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of a bounced event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    firstUnsubscribedAt: z
      .string()
      .nullable()
      .openapi({
        description:
          "First-occurrence (MIN) ISO 8601 timestamp of an unsubscribed event in this scope; " +
          "null if it never happened in scope. Passed through from email-gateway status.",
        example: "2026-01-01T00:00:00.000Z",
      }),
    global: z
      .object({
        bounced: z.boolean().openapi({ description: "Lead has bounced anywhere across the platform.", example: false }),
        unsubscribed: z.boolean().openapi({ description: "Lead has unsubscribed anywhere across the platform.", example: false }),
      })
      .openapi({
        description: "Global-scope status (across all brands/campaigns). bounced and unsubscribed are global flags.",
      }),
  })
  .openapi("LeadDetail", {
    description:
      "One leads_campaigns row enriched with the full canonical lead payload (FullLead) and delivery status from email-gateway.",
  });

const LeadsResponseSchema = z
  .object({
    leads: z.array(LeadDetailSchema).openapi({
      description:
        "The leads_campaigns rows matching the query, with full canonical lead payload + delivery overlay. " +
        "Without `limit` this is every matching row; with `limit` it is at most that many, in " +
        "(created_at, id) ascending order.",
    }),
    nextCursor: z.string().nullable().openapi({
      description:
        "Where to resume the walk: pass it back as `?cursor=` to get the rows strictly after the " +
        "last one in this response. null means this response reached the end of the population — " +
        "always null for an unbounded read (no `limit`).",
    }),
  })
  .openapi("LeadsResponse", {
    description: "Response shape for GET /orgs/leads.",
  });

const LeadDetailResponseSchema = z
  .object({
    leadDetail: LeadDetailSchema.openapi({
      description:
        "The one lead the caller named, byte-equal to the element GET /orgs/leads emits for the " +
        "same row — a detail panel renders from it alone.",
    }),
  })
  .openapi("LeadDetailResponse", {
    description:
      "Response shape for GET /orgs/leads/{id}. One record, not a list: a consumer asking for one " +
      "lead should not be constructing a list query.",
  });

// --- Stats ---

const RepliesDetailSchema = z.object({
  interested: z.number(),
  meetingBooked: z.number(),
  closed: z.number(),
  notInterested: z.number(),
  wrongPerson: z.number(),
  unsubscribe: z.number(),
  neutral: z.number(),
  autoReply: z.number(),
  outOfOffice: z.number(),
});

const ByOutreachStatusSchema = z.object({
  contacted: z.number(),
  sent: z.number(),
  delivered: z.number(),
  opened: z.number(),
  bounced: z.number(),
  clicked: z.number(),
  unsubscribed: z.number(),
  repliesPositive: z.number(),
  repliesNegative: z.number(),
  repliesNeutral: z.number(),
  repliesAutoReply: z.number(),
  repliesDetail: RepliesDetailSchema,
});

const StatsResponseSchema = z
  .object({
    totalLeads: z.number(),
    byOutreachStatus: ByOutreachStatusSchema,
    repliesDetail: RepliesDetailSchema,
    buffered: z.number(),
    skipped: z.number(),
    claimed: z.number(),
  })
  .openapi("StatsResponse");

const StatsGroupSchema = z.object({
  key: z.string(),
  totalLeads: z.number(),
  byOutreachStatus: ByOutreachStatusSchema,
  repliesDetail: RepliesDetailSchema,
  buffered: z.number(),
  skipped: z.number(),
  claimed: z.number(),
});

const StatsGroupedResponseSchema = z
  .object({
    groups: z.array(StatsGroupSchema),
  })
  .openapi("StatsGroupedResponse");


// --- Register Paths ---

registry.registerPath({
  method: "get",
  path: "/health",
  summary: "Health check",
  responses: {
    200: {
      description: "Service is healthy",
      content: { "application/json": { schema: HealthResponseSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/buffer/next",
  summary: "Pull the next lead from the buffer",
  description:
    "Claims and returns the next available lead from the campaign buffer. " +
    "Response contains the full canonical lead payload (FullLead) under `lead.data` — " +
    "use `data.firstName`, `data.lastName`, `data.organization.name` for outbound recipient fields.",
  request: {
    params: z.object({}),
    body: {
      content: { "application/json": { schema: BufferNextRequestSchema } },
    },
  },
  parameters: BufferNextHeaders,
  responses: {
    200: {
      description: "Next lead from buffer (or found=false when exhausted)",
      content: { "application/json": { schema: BufferNextResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/leads",
  summary: "List leads with full enrichment and delivery status",
  description:
    "Returns leads_campaigns rows. Each row includes the full canonical lead payload (FullLead — see schema) under `lead`, " +
    "plus delivery status (contacted, sent, sentCount, delivered, opened, clicked, bounced, unsubscribed, replied, replyClassification, lastDeliveredAt, firstClickedAt, global). " +
    "Delivery status is fetched from email-gateway when brandId or campaignId is provided. " +
    "With campaignId: campaign-scoped status. With brandId only: brand-scoped (cross-campaign). " +
    "Without either: status fields default to false/null. " +
    "By default the response carries the ACTIONABLE population only — `buffered`, `claimed` and " +
    "`served` — and NOT `skipped`; use the `status` parameter to ask for a different set. " +
    "The response is UNBOUNDED unless the caller names a `limit`: without one it carries every " +
    "matching row (what the staff console and features-service want). With `limit` it carries at " +
    "most that many rows plus a `nextCursor` to walk the rest with. Rows come back in " +
    "(created_at, id) ascending order, which is a total order, so a `limit` + `cursor` walk visits " +
    "every row exactly once — no gaps, no repeats.",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "offerId",
      required: false,
      description:
        "Restrict returned leads to ONE offer — brand-service's proposition level, between the " +
        "brand and the campaign. A lead's offer is the offer named by the campaign it was served " +
        "under: the membership row's `campaign_id` is a frozen attribution, and campaign-service " +
        "records which offer each campaign sells, so this resolves to every campaign in the org " +
        "naming that offer and filters on the same `campaign_id` a campaignId read filters on. " +
        "Delivery status is offer-scoped (the union over those campaigns), the same way a " +
        "campaign identity's is. " +
        "MUTUALLY EXCLUSIVE with `campaignId` — a campaign already sells exactly one offer, so " +
        "naming both is a 400 rather than one silently winning. " +
        "An offer no campaign sells yet returns an empty list, never the brand's leads. " +
        "If campaign-service cannot say which campaigns sell it, the read is refused with a 502 — " +
        "never widened to the brand. " +
        "When absent, behavior + response shape are unchanged.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "orgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "userId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowSlug",
      required: false,
      description:
        "Restrict returned leads to those whose leads_campaigns row has workflow_slug = <value>. " +
        "When absent, behavior + response shape are unchanged.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "view",
      required: false,
      description:
        "Per-lead payload size. `basic` returns a slim `lead` object: " +
        "firstName, lastName, name, headline, linkedinUrl, photoUrl, apolloPersonId, " +
        "seniority, departments, functions, currentTitle, city, state, country " +
        "+ organization {id, name, logoUrl, primaryDomain, websiteUrl, industry, industries, " +
        "estimatedNumEmployees, annualRevenue, foundedYear, shortDescription, city, state, country}. " +
        "Field names/types are identical to the full FullLead/OrganizationView. " +
        "Still drops the heavy stuff (employmentHistory, subdepartments, technologyNames, " +
        "secondaryIndustries, funding events) so basic stays ~10x smaller than full. " +
        "Absent or any other value => the full FullLead payload (default, " +
        "backward-compatible). Use `basic` for list views.",
      schema: { type: "string" as const, enum: ["basic", "full"] },
    },
    {
      in: "query" as const,
      name: "status",
      required: false,
      description:
        "Which lifecycle statuses to return, as a comma-separated list of " +
        "`buffered`, `skipped`, `claimed`, `served` — or `all` for every one of them. " +
        "ABSENT => `buffered,claimed,served`: the population a caller can act on. " +
        "`skipped` rows are excluded by default because they were never served, so they carry no " +
        "delivery evidence (every engagement field on them is false/null by construction) and no " +
        "engagement-bucketed view can reach them — while being ~82% of the rows for a large brand. " +
        "Pass `all` (or name `skipped` explicitly) to get them back. " +
        "An unknown value is a 400, never a silent fallback.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "limit",
      required: false,
      description:
        "Maximum number of leads to return. ABSENT => unbounded: every matching row, which is what " +
        "the staff console and features-service read. Present => at most that many rows, and a " +
        "`nextCursor` when more may follow. There is no server-imposed ceiling; the caller decides " +
        "how much it can hold. A value that is not a positive integer is a 400.",
      schema: { type: "integer" as const, minimum: 1 },
    },
    {
      in: "query" as const,
      name: "cursor",
      required: false,
      description:
        "Resume position, taken verbatim from a previous response's `nextCursor`. Returns the rows " +
        "strictly after it in (created_at, id) order, so a walk sees every row exactly once even " +
        "while new leads are being written. Mutually exclusive with `offset`; naming both is a 400, " +
        "as is a cursor this endpoint did not issue.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "offset",
      required: false,
      description:
        "Positional start over the same (created_at, id) order — the positional form of the walk. " +
        "Rows are appended in that order, so an offset walk is stable against new leads, but a row " +
        "leaving the filtered set mid-walk shifts it; `cursor` cannot drift that way and is the " +
        "one to prefer. Mutually exclusive with `cursor`. A negative or non-integer value is a 400.",
      schema: { type: "integer" as const, minimum: 0 },
    },
  ],
  responses: {
    200: {
      description: "List of leads with full canonical payload + delivery overlay",
      content: { "application/json": { schema: LeadsResponseSchema } },
    },
    400: {
      description:
        "Invalid `status`, `limit`, `cursor` or `offset` value, or `offerId` and `campaignId` both named",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
    502: {
      description:
        "`offerId` was named and campaign-service could not say which campaigns sell it — the read is refused rather than widened to the brand",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/leads/{id}",
  summary: "Read ONE lead's full record",
  description:
    "Returns the full record of a single lead — the same object GET /orgs/leads emits for that row " +
    "(full canonical FullLead payload under `lead`, active audience, lifecycle fields and the " +
    "delivery overlay), wrapped as `{ leadDetail }` rather than a one-element list. " +
    "This is what a table + detail-panel surface reads: take the slim list for the table, then ask " +
    "for depth one row at a time, instead of holding the full projection for a whole brand " +
    "(~57k rows / >100 MB on the largest one) so that one panel can work. " +
    "`id` is the `id` field of a list row (the leads_campaigns membership row), so a caller needs " +
    "nothing it did not already receive from the list. Scoped like the list is: the read is " +
    "org-scoped and a lead outside the caller's org is a 404, indistinguishable from one that does " +
    "not exist. `brandId` / `campaignId` mean exactly what they mean on the list — which scope the " +
    "delivery overlay answers for — so passing back whatever the table listed with makes the panel " +
    "agree with the row.",
  parameters: [
    ...AuthHeaders,
    {
      in: "path" as const,
      name: "id",
      required: true,
      description: "The `id` of a lead as returned by GET /orgs/leads. A non-uuid value is a 400.",
      schema: { type: "string" as const, format: "uuid" },
    },
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      description:
        "Scope for the delivery overlay, same as on the list. A lead that does not belong to this " +
        "brand is a 404. Absent (and no campaignId) => the overlay fields default to false/null.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      description:
        "Campaign scope for the delivery overlay, same as on the list — resolved to the whole " +
        "campaign IDENTITY, so evidence recorded under a stopped ancestor still counts.",
      schema: { type: "string" as const },
    },
  ],
  responses: {
    200: {
      description: "The lead's full record",
      content: { "application/json": { schema: LeadDetailResponseSchema } },
    },
    400: {
      description: "`id` is not a uuid",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "No such lead in this caller's org (or brand, when brandId is given)",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/stats",
  summary: "Get lead stats by status",
  description:
    "Returns lead stats with outreach status from email-gateway. totalLeads = served leads count, byOutreachStatus = full recipientStats (contacted, sent, delivered, opened, clicked, bounced, unsubscribed, replies*), repliesDetail = granular reply breakdown, buffered/skipped = buffer counts. " +
    "When filtering or grouping by goal/profile/persona attribution fields, lead-service joins explicit leads_campaigns tags to recipient-level email-gateway evidence. Untagged rows stay unattributed and do not produce persona/profile groups.",
  parameters: [
    ...AuthHeaders,
    {
      in: "query" as const,
      name: "brandId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "campaignId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "orgId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "userId",
      required: false,
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "runIds",
      required: false,
      description: "Comma-separated list of run IDs",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowSlug",
      required: false,
      description: "Filter by exact workflow slug (single value)",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowSlugs",
      required: false,
      description:
        "Filter by multiple workflow slugs (comma-separated). Takes priority over workflowSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureSlug",
      required: false,
      description: "Filter by exact feature slug (single value)",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureSlugs",
      required: false,
      description:
        "Filter by multiple feature slugs (comma-separated). Takes priority over featureSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "workflowDynastySlug",
      required: false,
      description:
        "Filter by workflow dynasty slug. Resolved to all versioned slugs via workflow-service, then filtered with WHERE IN (...). Takes priority over workflowSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "featureDynastySlug",
      required: false,
      description:
        "Filter by feature dynasty slug. Resolved to all versioned slugs via features-service, then filtered with WHERE IN (...). Takes priority over featureSlug.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "goal",
      required: false,
      description: "Filter stats to rows explicitly tagged with this active goal.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "activeGoalId",
      required: false,
      description: "Filter stats to rows explicitly tagged with this active goal ID.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "brandProfileId",
      required: false,
      description: "Filter stats to rows explicitly tagged with this brand profile ID.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "audienceId",
      required: false,
      description: "Filter stats to rows explicitly tagged with this audience ID.",
      schema: { type: "string" as const },
    },
    {
      in: "query" as const,
      name: "groupBy",
      required: false,
      description:
        "Group stats by this dimension. When set, returns { groups: [...] } instead of flat stats. Attribution groupings are explicit-only: null attribution rows are omitted, not assigned to an unknown bucket.",
      schema: {
        type: "string" as const,
        enum: [
          "campaignId",
          "brandId",
          "workflowSlug",
          "featureSlug",
          "workflowDynastySlug",
          "featureDynastySlug",
          "goal",
          "activeGoalId",
          "brandProfileId",
          "audienceId",
        ],
      },
    },
  ],
  responses: {
    200: {
      description:
        "Lead stats with outreach status. Without groupBy: flat response with totalLeads, byOutreachStatus, repliesDetail, buffered, skipped. With groupBy: grouped stats array.",
      content: {
        "application/json": {
          schema: z.union([StatsResponseSchema, StatsGroupedResponseSchema]),
        },
      },
    },
    400: {
      description: "Invalid groupBy value",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});


// --- Transfer Brand ---

const InternalApiKeyHeader = [
  {
    in: "header" as const,
    name: "x-api-key",
    required: true,
    schema: { type: "string" as const },
    description: "API key for authenticating requests",
  },
  {
    in: "header" as const,
    name: "x-run-id",
    required: true,
    schema: { type: "string" as const },
    description: "Idempotency key — replaying with the same x-run-id returns the cached response",
  },
];

export const TransferBrandRequestSchema = z
  .object({
    sourceBrandId: z.string().uuid(),
    sourceOrgId: z.string().uuid(),
    targetOrgId: z.string().uuid(),
    targetBrandId: z.string().uuid().optional(),
  })
  .openapi("TransferBrandRequest");

const TransferBrandTableResultSchema = z.object({
  tableName: z.string(),
  count: z.number(),
});

const TransferBrandResponseSchema = z
  .object({
    updatedTables: z.array(TransferBrandTableResultSchema),
  })
  .openapi("TransferBrandResponse");

registry.registerPath({
  method: "post",
  path: "/internal/transfer-brand",
  summary: "Transfer a solo-brand from one org to another",
  description:
    "Updates org_id on all rows that reference exactly this one brand (solo-brand). " +
    "Co-branding rows (multiple brand IDs) are skipped. Idempotent — running twice is a no-op.",
  request: {
    body: {
      content: { "application/json": { schema: TransferBrandRequestSchema } },
    },
  },
  parameters: InternalApiKeyHeader,
  responses: {
    200: {
      description: "Transfer results per table",
      content: { "application/json": { schema: TransferBrandResponseSchema } },
    },
    400: {
      description: "Invalid request body",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// --- Feature memberships (internal) ---

const FeatureMembershipApiKeyHeader = [
  {
    in: "header" as const,
    name: "x-api-key",
    required: true,
    schema: { type: "string" as const },
    description: "API key for authenticating requests",
  },
];

const FeatureMembershipSchema = z
  .object({
    orgId: z
      .string()
      .openapi({
        description: "Internal organization UUID owning the leads.",
        example: "30000000-0000-0000-0000-000000000001",
      }),
    brandId: z
      .string()
      .openapi({
        description: "Brand UUID (unnested from leads_campaigns.brand_ids).",
        example: "20000000-0000-0000-0000-000000000001",
      }),
    workflowSlug: z
      .string()
      .openapi({
        description: "Workflow slug that produced leads for this (org, brand) under the requested feature.",
        example: "sales-cold-email-outreach-lithium",
      }),
  })
  .openapi("FeatureMembership", {
    description:
      "One distinct (org, brand, workflow) combination that has leads for a requested feature.",
  });

const FeatureMembershipsResponseSchema = z
  .object({
    memberships: z.array(FeatureMembershipSchema).openapi({
      description:
        "Distinct (orgId, brandId, workflowSlug) tuples from leads_campaigns whose feature_slug matches the requested feature(s). Empty array when no matches.",
    }),
  })
  .openapi("FeatureMembershipsResponse", {
    description: "Response shape for GET /internal/feature-memberships.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/feature-memberships",
  summary: "List distinct (org, brand, workflow) combinations that have leads for a feature",
  description:
    "Returns the DISTINCT (orgId, brandId, workflowSlug) tuples from leads_campaigns whose feature_slug matches the requested feature(s). " +
    "featureSlugs is comma-separated and matched exactly (feature slugs are not versioned). brandId is unnested from brand_ids[]. " +
    "Rows with a null workflow_slug are excluded. Empty array when no matches. Auth: x-api-key only.",
  parameters: [
    ...FeatureMembershipApiKeyHeader,
    {
      in: "query" as const,
      name: "featureSlugs",
      required: true,
      description: "Comma-separated list of feature slugs to resolve memberships for.",
      schema: { type: "string" as const },
    },
  ],
  responses: {
    200: {
      description: "Distinct (org, brand, workflow) memberships for the requested feature(s)",
      content: { "application/json": { schema: FeatureMembershipsResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/openapi.json",
  summary: "Get OpenAPI specification",
  responses: {
    200: { description: "OpenAPI JSON document" },
    404: { description: "Spec not generated" },
  },
});

// --- Conversion tracking (beta) ---

const ConversionIngestRequestSchema = z
  .object({
    event: z
      .enum(["signup", "meeting_booked", "form_submission", "sale", "purchase", "ping"])
      .openapi({
        description:
          "The conversion that happened on the client's website. \"sale\" is the terminal " +
          "\"a customer paid\" signal (carries a revenue value in valueCents). The legacy spelling " +
          "\"purchase\" is still accepted and normalized to \"sale\" so already-configured " +
          "integrations keep firing. The special value \"ping\" is a liveness heartbeat the on-page " +
          "tag fires on page-load — it is NOT a conversion (no attribution, not counted, excluded " +
          "from eventTypesSeen), it only proves the tag is alive.",
        example: "sale",
      }),
    email: z.string().optional().openapi({ example: "jane@acme.com" }),
    phone: z.string().optional().openapi({ example: "+1 (415) 555-0142" }),
    firstName: z.string().optional().openapi({ example: "Jane" }),
    lastName: z.string().optional().openapi({ example: "Doe" }),
    companyUrl: z.string().optional().openapi({ example: "https://acme.com" }),
    dedupeKey: z.string().optional().openapi({
      description:
        "Client-supplied idempotency key. When present, uniqueness is per (brand, dedupeKey). " +
        "When absent, dedupe is per (brand, event, email-or-phone, calendar-day).",
    }),
    valueCents: z.number().int().optional().openapi({
      description:
        "Optional conversion value in cents — the revenue attached to the event (primarily the " +
        "\"sale\" terminal signal).",
      example: 4900,
    }),
  })
  .openapi("ConversionIngestRequest", {
    description: "A conversion event reported by a client's website pixel.",
  });

const ConversionIngestResponseSchema = z
  .object({ received: z.boolean().openapi({ example: true }) })
  .openapi("ConversionIngestResponse", {
    description:
      "Always { received: true } on success. The match/attribution result is NEVER leaked to the public caller.",
  });

const ConversionTokenResponseSchema = z
  .object({
    token: z.string().openapi({
      description:
        "Publishable write-key for this brand. Returned in FULL (it is embedded in a client-side pixel, so not a secret). Can only WRITE conversion events for its one brand.",
      example: "pk_conv_9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c",
    }),
    ingestUrl: z.string().openapi({
      description: "Full public URL a third party hits for POST /public/conversions.",
      example: "https://api.distribute.you/public/conversions",
    }),
    status: z.enum(["not_set_up", "live_waiting", "live"]).openapi({
      description:
        "Tracker liveness, DERIVED from received signals (never self-attested). " +
        "not_set_up — nothing received yet; live_waiting — a ping proves the tag is alive but " +
        "no real conversion yet; live — at least one real conversion received.",
      example: "live_waiting",
    }),
    lastEventAt: z.string().nullable().openapi({
      description:
        "ISO-8601 timestamp of the last REAL conversion event (signup/meeting_booked/form_submission/sale), or null.",
      example: "2026-07-06T12:00:00.000Z",
    }),
    lastPingAt: z.string().nullable().openapi({
      description: "ISO-8601 timestamp of the last liveness ping, or null.",
      example: "2026-07-06T11:59:00.000Z",
    }),
    eventTypesSeen: z.array(z.string()).openapi({
      description:
        "Distinct REAL conversion event types actually received. Always EXCLUDES \"ping\".",
      example: ["signup"],
    }),
  })
  .openapi("ConversionTokenResponse", {
    description:
      "The brand's publishable conversion write-key, the public ingest URL, and a derived " +
      "liveness overlay (status + last event/ping timestamps + event types seen).",
  });

const ConversionTokenHeader = [
  {
    in: "header" as const,
    name: "x-conversion-token",
    required: false,
    schema: { type: "string" as const },
    description:
      "Brand publishable write-token. Alternatively pass it as `Authorization: Bearer <token>`.",
  },
];

const BrandIdPathParam = z.object({
  brandId: z.string().openapi({
    param: { name: "brandId", in: "path" },
    example: "20000000-0000-0000-0000-000000000001",
  }),
});

registry.registerPath({
  method: "post",
  path: "/public/conversions",
  summary: "Ingest a conversion event from a client's website (token-auth, public)",
  description:
    "Called directly by the CLIENT's website code (token-auth, NO Clerk). Authenticates the brand " +
    "publishable token, records the conversion, and attributes it to a lead we emailed for that brand " +
    "via a confidence-tiered match waterfall (email/phone → deterministic; domain+lastName → strong; " +
    "name-only → probabilistic, auto-attributed to the top candidate). Only strong-ambiguous " +
    "(domain+lastName with >1 candidate) is held for review. NEVER leaks the match result — " +
    "always { received: true } " +
    "on success. Dedupe: per (brand, dedupeKey) when supplied, else per (brand, event, email-or-phone, day). " +
    "The special event \"ping\" is a liveness heartbeat: it stamps the brand's last-ping time and returns " +
    "{ received: true } WITHOUT running attribution, storing a conversion, or counting toward stats.",
  request: {
    body: {
      content: { "application/json": { schema: ConversionIngestRequestSchema } },
    },
  },
  parameters: ConversionTokenHeader,
  responses: {
    200: {
      description: "Event received (match result intentionally not disclosed)",
      content: { "application/json": { schema: ConversionIngestResponseSchema } },
    },
    400: {
      description: "Missing or invalid event",
      content: { "application/json": { schema: ErrorResponseSchema } },
    },
    401: { description: "Missing or invalid conversion token" },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/brands/{brandId}/conversion-token",
  summary: "Get-or-create the brand's publishable conversion write-token",
  description:
    "Returns the brand's publishable conversion token (creating it on first call) plus the public ingest URL. " +
    "The token is returned in full — it is a publishable write-key, not a secret.",
  request: { params: BrandIdPathParam },
  parameters: AuthHeaders,
  responses: {
    200: {
      description: "The brand's conversion token and ingest URL",
      content: { "application/json": { schema: ConversionTokenResponseSchema } },
    },
    400: { description: "Missing x-org-id" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/brands/{brandId}/conversion-token/rotate",
  summary: "Rotate the brand's publishable conversion write-token",
  description:
    "Replaces the brand's token with a fresh one and invalidates the old token (it immediately 401s on ingest). " +
    "Rotation is the abuse remedy for a leaked publishable key.",
  request: { params: BrandIdPathParam },
  parameters: AuthHeaders,
  responses: {
    200: {
      description: "The new conversion token and ingest URL",
      content: { "application/json": { schema: ConversionTokenResponseSchema } },
    },
    400: { description: "Missing x-org-id" },
    401: { description: "Unauthorized" },
  },
});

const StepCountsSchema = z.object({
  signup: z.number().int().openapi({ example: 12 }),
  meeting_booked: z.number().int().openapi({ example: 3 }),
  meeting_attended: z.number().int().openapi({ example: 2 }),
  form_submission: z.number().int().openapi({ example: 7 }),
  sale: z.number().int().openapi({ example: 2 }),
  website_visit: z.number().int().openapi({
    description:
      "Website visits known ONLY by hand: a visit stated for a lead whose click the delivery " +
      "layer already measured is NOT counted here, so this number can be ADDED to the measured " +
      "click count without counting anybody twice. Nothing about what the delivery layer " +
      "measures changes.",
    example: 1,
  }),
  purchase: z.number().int().openapi({ example: 2 }),
});

const ConversionCountsResponseSchema = z
  .object({
    counts: StepCountsSchema.openapi({
      description:
        "Count of REAL, deduped, attributed outcomes per step, BOTH sources together — what every " +
        "existing consumer reads, unchanged. All five canonical keys (signup, meeting_booked, " +
        "meeting_attended, form_submission, sale) are ALWAYS present (0 when none). " +
        "\"meeting_attended\" is statable by hand only (a page-load tag cannot observe somebody " +
        "showing up) and counts exactly like the four the tracker reports. The terminal event was " +
        "renamed \"purchase\" → \"sale\"; a legacy \"purchase\" key mirroring \"sale\" is also " +
        "returned for the migration window (drop once consumers read \"sale\"). Excludes the " +
        "\"ping\" liveness heartbeat, needs_review, and unmatched events. A \"never\" statement is " +
        "NOT an outcome and is counted by nothing here.",
    }),
    bySource: z
      .object({ tracker: StepCountsSchema, manual: StepCountsSchema })
      .openapi({
        description:
          "The SAME rows, split by who said so: tracker — reported by the client's website; " +
          "manual — stated by a human about a lead named by id. For every key, " +
          "tracker + manual === counts. This is how a hand-stated outcome stays distinguishable " +
          "from a tracker-reported one after the fact without changing what either counts toward.",
      }),
  })
  .openapi("ConversionCountsResponse", {
    description:
      "Per-brand real conversion counts by event type, for features-service to compute real " +
      "signups / cost-per-signup.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/brands/{brandId}/conversion-counts",
  summary: "Real conversion counts per event type for a brand (internal, service-auth)",
  description:
    "INTERNAL (service-auth: x-api-key — same tier as other /internal/* routes, NO Clerk). Returns the " +
    "per-event-type COUNT of REAL, attributed conversions for the brand. Each count is deduped (rows are " +
    "deduped at write via the (brand_id, dedupe_signature) partial unique index) and filtered to " +
    "attribution_status = 'attributed' (credited to a lead we emailed for the brand; excludes needs_review " +
    "and unmatched). The \"ping\" liveness heartbeat never lands in conversion_events, so it is excluded. " +
    "All six step keys are ALWAYS present (0 when none received), including \"meeting_attended\" and " +
    "\"website_visit\", which are statable by hand only. \"website_visit\" counts the visits known ONLY " +
    "by hand: a visit stated for a lead whose click the delivery layer already measured is left out, so " +
    "this number can be added to the measured click count without counting anybody twice (email-gateway " +
    "unreachable → 502, never a guessed count). `bySource` splits the same " +
    "rows into tracker-reported and hand-stated (tracker + manual === counts, per key). A \"never\" " +
    "statement is not an outcome and is counted by nothing here. A brand with zero conversions returns " +
    "all-zero counts (200, never 404).",
  request: { params: BrandIdPathParam },
  parameters: FeatureMembershipApiKeyHeader,
  responses: {
    200: {
      description: "Per-event-type real conversion counts for the brand",
      content: { "application/json": { schema: ConversionCountsResponseSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

const ConversionCountsByDaySchema = z
  .object({
    byDay: z
      .object({
        signup: z.record(z.string(), z.number().int()),
        meeting_booked: z.record(z.string(), z.number().int()),
        meeting_attended: z.record(z.string(), z.number().int()),
        form_submission: z.record(z.string(), z.number().int()),
        sale: z.record(z.string(), z.number().int()),
        website_visit: z.record(z.string(), z.number().int()),
        purchase: z.record(z.string(), z.number().int()),
      })
      .openapi({
        description:
          "Per event type, a map of UTC calendar day (YYYY-MM-DD) -> count of REAL, deduped, " +
          "attributed conversions received that day. Days are bucketed by received_at AT TIME ZONE " +
          "'UTC' (matching the ingest dedupe UTC-day convention). A day key appears only when its " +
          "count > 0. All four canonical event keys (…, sale) are ALWAYS present (empty object when " +
          "none), plus a legacy \"purchase\" key mirroring \"sale\" for the rename migration window. " +
          "Excludes the \"ping\" liveness heartbeat, needs_review, and unmatched events — the SAME set " +
          "as /conversion-counts, just placed on the day each conversion occurred.",
        example: {
          signup: { "2026-07-08": 2, "2026-07-09": 1 },
          meeting_booked: {},
          meeting_attended: {},
          form_submission: { "2026-07-09": 3 },
          sale: {},
          purchase: {},
        },
      }),
    undated: z
      .object({
        signup: z.number().int(),
        meeting_booked: z.number().int(),
        meeting_attended: z.number().int(),
        form_submission: z.number().int(),
        sale: z.number().int(),
        purchase: z.number().int(),
      })
      .openapi({
        description:
          "Per event type, the count of attributed conversions whose day genuinely cannot be " +
          "determined (received_at IS NULL) — counted explicitly, NEVER dropped and NEVER assigned a " +
          "fabricated date. received_at is NOT NULL DEFAULT now() today, so this is 0 in practice, but " +
          "the field is always present so the contract stays honest. The legacy \"purchase\" key " +
          "mirrors \"sale\" for the rename migration window. Reconciliation: for every event, " +
          "sum(byDay[event] values) + undated[event] === the /conversion-counts total for that event.",
        example: {
          signup: 0,
          meeting_booked: 0,
          meeting_attended: 0,
          form_submission: 0,
          sale: 0,
          purchase: 0,
        },
      }),
  })
  .openapi("ConversionCountsByDayResponse", {
    description:
      "Per-brand real conversion counts broken down by the calendar day each conversion was received " +
      "(plus an explicit undated bucket), so features-service can draw a truthful per-day observed " +
      "series instead of a projection. Reconciles exactly to /conversion-counts totals.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/brands/{brandId}/conversion-counts-by-day",
  summary: "Real conversion counts per event type broken down by calendar day for a brand (internal, service-auth)",
  description:
    "INTERNAL (service-auth: x-api-key — same tier as conversion-counts, NO Clerk). Returns the SAME set of " +
    "REAL, attributed, deduped-at-write conversions as /conversion-counts, but broken down by the UTC " +
    "calendar day each conversion was received — so features-service can render a truthful per-day observed " +
    "series (today AND past days) instead of a clicks × rate projection. byDay[event] maps YYYY-MM-DD -> " +
    "count (day key present only when > 0); undated[event] counts conversions with no determinable day " +
    "(received_at IS NULL — 0 in practice, but always present and never fabricated). All four event keys are " +
    "ALWAYS present. For every event, sum(byDay values) + undated === the /conversion-counts total. A brand " +
    "with zero attributed conversions returns all-empty byDay + all-zero undated (200, never 404).",
  request: { params: BrandIdPathParam },
  parameters: FeatureMembershipApiKeyHeader,
  responses: {
    200: {
      description: "Per-event-type real conversion counts broken down by calendar day for the brand",
      content: { "application/json": { schema: ConversionCountsByDaySchema } },
    },
    401: { description: "Unauthorized" },
  },
});

const ConvertedLeadEmailsResponseSchema = z
  .object({
    event: z
      .enum(["signup", "meeting_booked", "form_submission", "sale"])
      .openapi({
        description:
          "The CANONICAL conversion event type the emails were filtered to. A legacy \"purchase\" " +
          "query is normalized to and echoed as \"sale\".",
        example: "form_submission",
      }),
    emails: z
      .array(z.string())
      .openapi({
        description:
          "Deduped, lowercased canonical emails of the leads-we-emailed that have >=1 attributed " +
          "conversion of `event` for this brand. This is the emails-we-served join key (the matched " +
          "lead's PRIMARY email), NOT the raw email a visitor typed on the client's site. Intersect it " +
          "with each audience's email membership (also email-keyed) to get a per-audience conversion " +
          "count. Empty array when the brand has no attributed conversions of `event`.",
        example: ["jane@acme.com", "bob@globex.com"],
      }),
  })
  .openapi("ConvertedLeadEmailsResponse", {
    description:
      "Per-brand set of matched-lead canonical emails with an attributed conversion of a given event " +
      "type, so features-service can attribute conversions to audiences by email-membership intersection.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/brands/{brandId}/converted-lead-emails",
  summary: "Matched-lead canonical emails with an attributed conversion of a given type (internal, service-auth)",
  description:
    "INTERNAL (service-auth: x-api-key — same tier as conversion-counts, NO Clerk). Returns the SET of " +
    "matched-lead canonical emails (the emails-we-served identity) that have at least one REAL, attributed " +
    "conversion of the `event` type for the brand. features-service intersects this set with each audience's " +
    "email membership (which it already resolves by email) to count conversions per audience. Only " +
    "attribution_status = 'attributed' rows count (the SAME set conversion-counts uses; excludes needs_review " +
    "+ unmatched). The returned identity is the matched lead's PRIMARY email (earliest email contact method), " +
    "NOT the raw email a visitor typed. Emails are lowercased + DISTINCT. `event` is required and must be one " +
    "of signup | meeting_booked | form_submission | sale (the legacy \"purchase\" spelling is also accepted " +
    "and normalized to \"sale\"; missing/invalid → 400). A brand with zero attributed conversions of `event` " +
    "returns an empty array (200, never 404).",
  request: { params: BrandIdPathParam },
  parameters: [
    ...FeatureMembershipApiKeyHeader,
    {
      in: "query" as const,
      name: "event",
      required: true,
      schema: {
        type: "string" as const,
        enum: [
          "signup",
          "meeting_booked",
          "meeting_attended",
          "form_submission",
          "sale",
          "website_visit",
          "purchase",
        ],
      },
      description:
        "Conversion event type to filter to. Required. Canonical: signup | meeting_booked | " +
        "form_submission | sale. The legacy \"purchase\" spelling is accepted (normalized to \"sale\").",
    },
  ],
  responses: {
    200: {
      description: "The set of matched-lead canonical emails with an attributed conversion of `event`",
      content: { "application/json": { schema: ConvertedLeadEmailsResponseSchema } },
    },
    400: { description: "Invalid or missing event" },
    401: { description: "Unauthorized" },
  },
});

const ConvertedLeadOutcomeSchema = z
  .object({
    leadId: z.string().nullable().openapi({
      description: "The matched lead this outcome is credited to.",
    }),
    email: z.string().nullable().openapi({
      description:
        "The matched lead's canonical (primary) email, lowercased — the SAME join key " +
        "/converted-lead-emails returns. Null when the lead has no email contact method; the row " +
        "is still returned, so this read never disagrees with the counts about how many outcomes exist.",
      example: "jane@acme.com",
    }),
    campaignId: z.string().nullable().openapi({
      description:
        "The campaign the outcome is attributable to, so the per-campaign / per-workflow / per-offer " +
        "grains can move and not only the brand total. Always present on a hand-stated outcome (the " +
        "statement is made on a lead row, which belongs to a campaign). Null on a tracker-reported one: " +
        "a page-load tag knows the brand and nothing else, and a guess would be worse than a null.",
    }),
    occurredAt: z.string().nullable().openapi({
      description:
        "When the outcome actually happened, ISO-8601 — a hand-stated fact carries the date the person " +
        "gave, not the date they typed it. Null only when genuinely undated (the same rows " +
        "/conversion-counts-by-day reports as `undated`); never fabricated.",
      example: "2026-08-19T14:30:00.000Z",
    }),
    valueCents: z.number().int().nullable().openapi({
      description:
        "What the outcome was worth, in cents, when somebody stated it. Null means nobody said — NOT " +
        "zero — so a consumer falls back to its own average for those rows and only those. A \"sale\" " +
        "stated from now on always carries one: the write refuses a sale with no value.",
      example: 490000,
    }),
    source: z.enum(["tracker", "manual"]).openapi({
      description: "manual — a human stated it; tracker — the website tag reported it.",
    }),
  })
  .openapi("ConvertedLeadOutcome");

const ConvertedLeadsResponseSchema = z
  .object({
    event: z
      .enum([
        "signup",
        "meeting_booked",
        "meeting_attended",
        "form_submission",
        "sale",
        "website_visit",
      ])
      .openapi({
      description:
        "The CANONICAL step the outcomes were filtered to. A legacy \"purchase\" query is normalized " +
        "to and echoed as \"sale\".",
      example: "sale",
    }),
    outcomes: z.array(ConvertedLeadOutcomeSchema).openapi({
      description:
        "One row per attributed outcome of `event` for the brand, newest first. Exactly the set " +
        "/conversion-counts counts: `outcomes.length` equals that total, and bucketing `occurredAt` by " +
        "UTC calendar day reproduces /conversion-counts-by-day row for row, `null` landing in `undated`. " +
        "Empty when the brand has no attributed outcome of `event`.",
    }),
  })
  .openapi("ConvertedLeadsResponse", {
    description:
      "Per-brand, per-step outcomes carrying WHEN each happened, WHICH campaign it is attributable to " +
      "and HOW MUCH it was worth — so a consumer values a lead by what somebody observed instead of " +
      "projecting declared rates through it.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/brands/{brandId}/converted-leads",
  summary: "Attributed outcomes of a step for a brand, with date, campaign and value (internal, service-auth)",
  description:
    "INTERNAL (service-auth: x-api-key — same tier as conversion-counts, NO Clerk). Every attributed " +
    "outcome of `event` for the brand, ONE ROW PER OUTCOME, carrying when it happened, which campaign it " +
    "is attributable to and what it was worth — the three things /converted-lead-emails cannot say, and " +
    "without which an observed outcome cannot be charted over time, cannot move any grain below the brand, " +
    "and gets priced at the brand's average lifetime revenue. Exactly the same set /conversion-counts and " +
    "/conversion-counts-by-day count (deduped at write, attribution_status = 'attributed'), so the reads " +
    "reconcile row for row — a lead with no email is returned with a null email rather than dropped. " +
    "`event` is required, one of signup | meeting_booked | meeting_attended | form_submission | sale " +
    "(legacy \"purchase\" accepted, normalized to \"sale\"); missing/invalid → 400. A brand with no " +
    "attributed outcome of `event` returns an empty array (200, never 404).",
  request: { params: BrandIdPathParam },
  parameters: [
    ...FeatureMembershipApiKeyHeader,
    {
      in: "query" as const,
      name: "event",
      required: true,
      schema: {
        type: "string" as const,
        enum: [
          "signup",
          "meeting_booked",
          "meeting_attended",
          "form_submission",
          "sale",
          "website_visit",
          "purchase",
        ],
      },
      description:
        "Step to filter to. Required. Canonical: signup | meeting_booked | meeting_attended | " +
        "form_submission | sale. The legacy \"purchase\" spelling is accepted (normalized to \"sale\").",
    },
  ],
  responses: {
    200: {
      description: "The attributed outcomes of `event` for the brand, newest first",
      content: { "application/json": { schema: ConvertedLeadsResponseSchema } },
    },
    400: { description: "Invalid or missing event" },
    401: { description: "Unauthorized" },
  },
});

// --- Hand-stated step outcomes ---

const StepStatementOrgHeaders = [
  {
    in: "header" as const,
    name: "x-api-key",
    required: true,
    schema: { type: "string" as const },
    description: "API key for authenticating requests",
  },
  {
    in: "header" as const,
    name: "x-org-id",
    required: true,
    schema: { type: "string" as const },
    description: "Internal organization UUID from client-service",
  },
  {
    in: "header" as const,
    name: "x-user-id",
    required: false,
    schema: { type: "string" as const },
    description:
      "Internal user UUID of the person making the statement. Stored verbatim as statedByUserId so a statement can be traced back to whoever made it.",
  },
  {
    in: "header" as const,
    name: "x-brand-id",
    required: false,
    schema: { type: "string" as const },
    description:
      "Brand scope. Same meaning as ?brandId=: which of the row's brands the statement is about. A brand the row is not part of answers 404, exactly as an absent row does.",
  },
];

const LeadRowIdPathParam = z.object({
  id: z.string().openapi({
    param: { name: "id", in: "path" },
    description:
      "The `id` a list row already carries (the leads_campaigns membership row) — so a caller states an outcome for a lead it has already resolved without re-supplying any identity field.",
    example: "40000000-0000-0000-0000-000000000001",
  }),
});

const STEP_ENUM = [
  "signup",
  "meeting_booked",
  "meeting_attended",
  "form_submission",
  "sale",
  "website_visit",
  "purchase",
] as const;

const FUNNEL_KEY_ENUM = [
  "sales_meetings_from_conversation",
  "sales_meetings_from_website",
  "website_purchases",
  "form_magnet",
  "sales_from_conversation",
  "sales_meetings_from_ads",
  "lead_forms_from_ads",
] as const;

const ChainFieldsSchema = {
  funnelKey: z.enum(FUNNEL_KEY_ENUM).openapi({
    description:
      "The sales funnel this lead's CAMPAIGN states it sells through, read from campaign-service and never inferred. It is what gives the steps an order: \"before\" and \"after\" mean nothing without knowing which chain the lead is on.",
    example: "sales_meetings_from_conversation",
  }),
  chain: z.array(z.enum(STEP_ENUM)).openapi({
    description:
      "That funnel's steps, IN ORDER, expressed in this service's step vocabulary. A step of the vocabulary that is not on this chain is constrained by nothing: no chain rule reaches it.",
    example: ["meeting_booked", "meeting_attended", "sale"],
  }),
};

const StepStatementRequestSchema = z
  .object({
    step: z.enum(STEP_ENUM).openapi({
      description:
        "The funnel step being stated. \"meeting_attended\" and \"website_visit\" exist here and nowhere in the tracker: attendance happens off the client's website, and a visit is measured by the delivery layer as a click, so for both only a human can state what those signals missed. A hand-stated visit ADDS to the measured one and never suppresses it: a lead carrying both is counted once, because the hand-stated row is left out of the counts. The legacy spelling \"purchase\" is accepted and normalized to \"sale\".",
      example: "meeting_booked",
    }),
    kind: z.enum(["outcome", "never"]).openapi({
      description:
        "outcome — this happened; it is written to the conversion ledger every consumer already counts, so the brand's counts move on the next read. never — this will NOT happen; it is NOT an outcome, nothing counts it anywhere, and it exists so a consumer can tell a lead that is DEAD at a step from one still PENDING.",
      example: "outcome",
    }),
    valueCents: z.number().int().optional().openapi({
      description:
        "What the outcome was worth, in cents. REQUIRED on a \"sale\" outcome and optional on every other step: a won deal is the one place estimating has no excuse, because with no value every downstream money figure prices it at the brand's average lifetime revenue, which describes no real customer. Stating it early on an unusually large lead, long before it closes, is exactly why the other steps keep it optional. Rejected with 400 on a \"never\" statement rather than silently dropped.",
      example: 490000,
    }),
    note: z.string().optional().openapi({
      description: "Free text the person stating the fact wrote, stored verbatim.",
      example: "Closed on the call, contract signed 2026-08-19.",
    }),
    occurredAt: z.string().optional().openapi({
      description:
        "ISO-8601 timestamp of WHEN the outcome happened, for a fact stated after the fact — it is what the by-day series buckets on. Unparseable values are a 400, never silently replaced by now().",
      example: "2026-08-19T14:30:00.000Z",
    }),
  })
  .openapi("LeadStepStatementRequest", {
    description: "A statement a human makes about one step of one lead's campaign funnel.",
  });

const StepStatementSchema = z.object({
  id: z.string(),
  leadCampaignId: z.string(),
  leadId: z.string(),
  campaignId: z.string().openapi({
    description:
      "The campaign the statement was made on — the row's own campaign, so an outcome stated from a campaign screen is attributable to that campaign and not only to the brand.",
  }),
  brandId: z.string(),
  step: z.enum(STEP_ENUM),
  kind: z.enum(["outcome", "never"]),
  source: z.enum(["tracker", "manual"]),
  valueCents: z.number().int().nullable(),
  note: z.string().nullable(),
  statedByUserId: z.string().nullable(),
  statedAt: z.string().nullable(),
});

const StepStatementResponseSchema = z
  .object({
    statement: StepStatementSchema,
    ...ChainFieldsSchema,
    retractedNever: z.boolean().optional().openapi({
      description:
        "True when this outcome superseded at least one earlier \"never\" — for the same step (the person did the thing after all) or for a step BEFORE it on the chain (a lead that paid necessarily got through the steps that lead to paying). The two cannot both stand, and this is the only direction that can be true — stating \"never\" for a step that already happened, or that a later step on the chain says already happened, is a 409.",
    }),
    retractedNeverSteps: z.array(z.enum(STEP_ENUM)).optional().openapi({
      description:
        "WHICH \"never\" statements this outcome superseded. They are marked retracted and kept, never deleted: what a person actually stated has to survive being superseded, and every read filters retracted statements out.",
      example: ["meeting_booked", "meeting_attended"],
    }),
  })
  .openapi("LeadStepStatementResponse", { description: "The statement as recorded." });

registry.registerPath({
  method: "post",
  path: "/orgs/leads/{id}/step-statements",
  summary: "State by hand what happened to one lead at one funnel step (or that it never will)",
  description:
    "Organisation-authenticated (the customer dashboard and the staff console are both org-authenticated; " +
    "the publishable website-tracker token is deliberately NOT a door to this — it is write-only, " +
    "brand-scoped and meant for a third party's page). The lead is named by the `id` a list row already " +
    "carries, so nothing about the person is re-supplied and nothing is matched or guessed — which is what " +
    "repairs, for hand-stated facts, the ~90% unmatched rate the tracker's identity waterfall carries. " +
    "kind=outcome writes to the conversion ledger tagged source=manual, so the brand's outcome counts move " +
    "on the next read with no consumer change, and restating the same step corrects the first statement " +
    "instead of counting twice. kind=never writes to a separate store that NO count reads, so a \"never\" " +
    "can never move an outcome count; it is what lets a consumer separate a lead that is dead at a step " +
    "from one still pending. A \"sale\" outcome MUST carry valueCents (400 otherwise) — a won deal states " +
    "what it was worth instead of being priced at the brand average; every other step keeps it optional.",
  request: {
    params: LeadRowIdPathParam,
    body: { content: { "application/json": { schema: StepStatementRequestSchema } } },
  },
  parameters: StepStatementOrgHeaders,
  responses: {
    201: {
      description: "The statement as recorded",
      content: { "application/json": { schema: StepStatementResponseSchema } },
    },
    400: { description: "Invalid id, step, kind, occurredAt; valueCents on a \"never\"; or a \"sale\" outcome with no valueCents" },
    401: { description: "Unauthorized" },
    404: { description: "No such lead row for this org (or for the requested brand scope)" },
    409: {
      description:
        "Cannot state \"never\" for a step that already has an outcome, or that a LATER step of the campaign's funnel says already happened (code step_already_happened); or the campaign states no sales funnel, so its steps have no order (code funnel_unstated / campaign_unknown)",
    },
    500: { description: "Internal server error" },
    502: {
      description:
        "campaign-service could not say which funnel the campaign sells through (code campaign_service_unavailable) — no answer is returned rather than one built on a chain nobody stated",
    },
  },
});

const StepStateSchema = z
  .object({
    step: z.enum(STEP_ENUM),
    state: z.enum(["outcome", "never", "pending"]).openapi({
      description:
        "outcome — it happened, either because somebody stated it or because a LATER step of this campaign's chain did; never — it will not happen, either stated or implied by an EARLIER step of the chain being never; pending — neither has been stated and no chain rule reaches it. Nothing counts a \"never\", however it arose.",
    }),
    origin: z.enum(["stated", "implied"]).nullable().openapi({
      description:
        "Whether a PERSON stated this step or the CHAIN implies it. Null exactly when the step is pending. An implied step is not a statement somebody made: it carries no author, no note and no date, and it moves automatically when the statement that implied it is retracted or superseded.",
    }),
    impliedBy: z.enum(STEP_ENUM).nullable().openapi({
      description:
        "The STATED step this one follows from — a later outcome for an implied outcome, an earlier \"never\" for an implied never. Null when nothing implies it.",
    }),
    statedState: z.enum(["outcome", "never"]).nullable().openapi({
      description:
        "What a person actually stated about THIS step, whatever the chain concluded — so a real statement is never lost to satisfy the chain. A \"never\" contradicted by a later outcome reads state=outcome, origin=implied, statedState=never.",
    }),
    inChain: z.boolean().openapi({
      description:
        "Whether this step is part of the lead's funnel chain. A step outside it reads from statements alone: no chain rule reaches it.",
    }),
    chainIndex: z.number().int().nullable().openapi({
      description: "Where the step sits on the chain, or null when the chain does not contain it.",
    }),
    source: z.enum(["tracker", "manual"]).nullable().openapi({
      description:
        "Who said so. Null on a pending step (nobody has said anything) and on an implied one (nobody stated it).",
    }),
    valueCents: z.number().int().nullable(),
    note: z.string().nullable(),
    statedByUserId: z.string().nullable(),
    at: z.string().nullable(),
  })
  .openapi("LeadStepState");

const StepStatementsListSchema = z
  .object({
    leadCampaignId: z.string(),
    leadId: z.string(),
    campaignId: z.string(),
    brandId: z.string(),
    ...ChainFieldsSchema,
    steps: z.array(StepStateSchema).openapi({
      description:
        "One entry per step of the outcome vocabulary, ALWAYS all of them, in a fixed order: signup, meeting_booked, form_submission, sale, meeting_attended, website_visit. Each carries the chain's two rules already applied — a \"never\" makes every LATER step of `chain` never, an outcome makes every EARLIER one reached — with `origin` telling a stated step from an implied one. The website visit additionally reads as an outcome with source=tracker when the delivery layer already measured a click for this lead, so the panel never invites somebody to state a fact the system already holds.",
    }),
  })
  .openapi("LeadStepStatementsResponse", {
    description: "What is known about every step of this lead's funnel.",
  });

registry.registerPath({
  method: "get",
  path: "/orgs/leads/{id}/step-statements",
  summary: "Everything known about every funnel step of one lead",
  description:
    "The read behind the panel a statement is made from: one entry per step, always all of them, each " +
    "either an outcome (with the source that reported or stated it), a \"never\", or pending. A " +
    "tracker-reported outcome is attributed to the person at brand grain, a hand-stated one to the exact " +
    "row it was stated on; both are returned here. A funnel is a CHAIN, so the answer respects it: a " +
    "\"never\" makes every LATER step of that campaign's chain read as never, and an outcome makes every " +
    "EARLIER one read as reached — `origin` tells a step a person STATED from one the chain IMPLIES, and " +
    "`statedState` keeps what somebody really said readable even where the chain concluded otherwise. The " +
    "chain order is per FUNNEL (`funnelKey` + `chain`), read from campaign-service and never guessed: a " +
    "campaign that states no funnel is a 409, not a made-up order.",
  request: { params: LeadRowIdPathParam },
  parameters: StepStatementOrgHeaders,
  responses: {
    200: {
      description: "Per-step state for this lead",
      content: { "application/json": { schema: StepStatementsListSchema } },
    },
    400: { description: "id is not a uuid" },
    401: { description: "Unauthorized" },
    404: { description: "No such lead row for this org (or for the requested brand scope)" },
    409: {
      description:
        "The campaign states no sales funnel this service has a chain for, so its steps have no order (code funnel_unstated / campaign_unknown)",
    },
    500: { description: "Internal server error" },
    502: {
      description:
        "campaign-service could not say which funnel the campaign sells through (code campaign_service_unavailable)",
    },
  },
});

const StepCountsShape = z.object({
  signup: z.number().int(),
  meeting_booked: z.number().int(),
  meeting_attended: z.number().int(),
  form_submission: z.number().int(),
  sale: z.number().int(),
  website_visit: z.number().int(),
});

const StepEmailsShape = z.object({
  signup: z.array(z.string()),
  meeting_booked: z.array(z.string()),
  meeting_attended: z.array(z.string()),
  form_submission: z.array(z.string()),
  sale: z.array(z.string()),
  website_visit: z.array(z.string()),
});

const StepDisqualificationsResponseSchema = z
  .object({
    counts: z
      .object({
        signup: z.number().int(),
        meeting_booked: z.number().int(),
        meeting_attended: z.number().int(),
        form_submission: z.number().int(),
        sale: z.number().int(),
        website_visit: z.number().int(),
      })
      .openapi({
        description:
          "Per step, how many DISTINCT people a human has stated will never reach it. These are not outcomes and are counted as outcomes by nothing — the number exists so a consumer can shrink a still-pending population to the one that can still convert.",
        example: {
          signup: 0,
          meeting_booked: 4,
          meeting_attended: 1,
          form_submission: 0,
          sale: 12,
          website_visit: 0,
        },
      }),
    byStep: z
      .object({
        signup: z.array(z.string()),
        meeting_booked: z.array(z.string()),
        meeting_attended: z.array(z.string()),
        form_submission: z.array(z.string()),
        sale: z.array(z.string()),
        website_visit: z.array(z.string()),
      })
      .openapi({
        description:
          "Per step, the canonical (primary) emails of those people — the SAME join key /converted-lead-emails returns, lowercased and DISTINCT, so a consumer intersects it with audience membership exactly as it already does for conversions. A lead with no email contact method has no join key and is absent here while still counted in `counts`.",
      }),
  })
  .extend({
    impliedCounts: StepCountsShape.optional().openapi({
      description:
        "Only with ?implied=true. Per step, how many DISTINCT people NOBODY stated that step for, whom a \"never\" EARLIER on their campaign's funnel makes never anyway: once a step is false, everything after it is false. Kept apart from `counts` so a reader can always tell what somebody stated from what the chain concluded.",
    }),
    impliedByStep: StepEmailsShape.optional().openapi({
      description: "Only with ?implied=true. The same canonical-email join key, for the implied set.",
    }),
    effectiveCounts: StepCountsShape.optional().openapi({
      description:
        "Only with ?implied=true. Stated and implied together — the answer to \"is this lead dead at this step?\". A \"never\" contradicted by an outcome further down the chain is absent here (the lead demonstrably got there) while remaining in `counts`, which is the record of what was said.",
    }),
    effectiveByStep: StepEmailsShape.optional().openapi({
      description: "Only with ?implied=true. The same canonical-email join key, for the effective set.",
    }),
  })
  .openapi("LeadStepDisqualificationsResponse", {
    description: "Per-brand, who is dead at which funnel step.",
  });

registry.registerPath({
  method: "get",
  path: "/internal/brands/{brandId}/step-disqualifications",
  summary: "People stated to never reach a funnel step, per step, for a brand (internal, service-auth)",
  description:
    "INTERNAL (service-auth: x-api-key — the same tier as the conversion-count reads, NO Clerk). Nothing " +
    "here is an outcome and nothing counts it as one: this is what lets a consumer separate a lead that is " +
    "DEAD at a step from one still PENDING, so a cost-per-acquisition denominator stops waiting forever on " +
    "somebody who is never coming. Never 404 — a brand nobody has disqualified anyone for returns empty " +
    "sets and zero counts.",
  request: {
    params: BrandIdPathParam,
    query: z.object({
      implied: z.literal("true").optional().openapi({
        param: { name: "implied", in: "query" },
        description:
          "Apply each lead's campaign funnel CHAIN as well: a lead that will never book has, by the same statement, never attended and never paid. Opt-in because it needs a campaign-service read per org; without it the response is byte-identical to what this endpoint has always answered.",
      }),
    }),
  },
  parameters: FeatureMembershipApiKeyHeader,
  responses: {
    200: {
      description: "Per-step counts and canonical emails of disqualified leads",
      content: { "application/json": { schema: StepDisqualificationsResponseSchema } },
    },
    401: { description: "Unauthorized" },
    409: {
      description:
        "Only with ?implied=true: some of these leads belong to campaigns that state no sales funnel, so no chain can be applied (code funnel_unstated, with the offending campaignIds)",
    },
    502: {
      description:
        "Only with ?implied=true: campaign-service could not answer (code campaign_service_unavailable)",
    },
  },
});
