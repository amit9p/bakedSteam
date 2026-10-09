# Goal Planning — Individual (draft v0.2, 2026-10-09)

Draft for discussion with Surya, fitted to the 2026 Goal Planning plan in UKG Pro (2026 Review Period, status Employee Planning). Master copy: D:\Cotiviti\claude_output\personal\2026-goal-plan-draft-amit-prasad.docx (Individual template layout); copy-paste version for the UKG form: personal\2026-goal-plan-ukg-entry-sheet.docx. Companion: personal/goal-setting-meeting-prep-surya.md.

Team Member: Amit Prasad · Title: Principal Data Engineer – AI · Team: AI Initiatives (AIINIT) · Manager: Surya Vadlamani

## Department / Team Goals and Priorities (to confirm with Surya)

- Deliver ARIA (risk-adjustment coding assistant) on the agentic hub: CoPilot first, AutoPilot earned on evidence.
- Standardize the data platform on Databricks with production engineering practice (Bitbucket, Jenkins, Asset Bundles, Unity Catalog); be ready for the GCP move.
- Reusable foundations across use cases: common clinical data model, clinical truth graph, policy-to-logic.

Every due date falls by 12/31/2026 (2026 plan); work that continues past year-end is listed under each goal as "Carries into the 2027 plan".

## Individual Performance Goals

### Deliver ARIA data hub — Phase 1

**Description.** Own the data-engineering layer (data hub) of ARIA on the agentic hub: ingress from CIS → bronze → silver (CCDM) → clinical truth graph (CTG) → symbolic auditor → gold determinations → egress back to CIS.
- Present the data-hub design to Reva and Surya and record the decisions: platform and lakehouse location, CTG storage, ingress mechanism, PHI environment.
- Agree the CIS handoff with the CIS owner: what CIS produces today and how it is delivered (CIS owns producing it; the data hub owns consuming it), plus the determination/feedback message back to CIS. Build the data-hub reader for that format; use the sample-event generator until a real feed exists.
- Extend the slice-1 pipeline already running in ai-engineering-dev (bronze + silver CCDM v0.1 with quarantine, deployed as a Databricks Asset Bundle) through CTG and gold on sample data, wiring in the hosted auditor when the POC component is available.

- Category: Performance
- Weight: 40%
- Start: 10/01/2026
- Due: 12/31/2026 (design review and decision log by 11/30/2026)

**Measures**
- Design review held with Reva and Surya; decision log published in Confluence by 11/30/2026.
- Ingress/egress contract document agreed with the CIS owner by 12/31/2026 (or the blocker and its owner named in writing).
- Slice 2 running on sample data by 12/31/2026: CTG node/edge tables and a gold determination table populated from silver for at least 3 sample charts, every gold row carrying rule_set, CTG, model and CCDM version keys.
- Demo of the extended slice to Reva and Surya by 12/31/2026.
- Carries into the 2027 plan: hosted auditor integration, replay from bronze reproducing the same determinations, shadow-mode evaluation against coder decisions.

Results: _filled in during the year_ · Feedback: _manager_

### Establish CCDM v1.0 as ARIA's data model

**Description.** Turn the CCDM from the current v0.1 (constants in code) into governed reference data and the contract every component reads and writes.
- Finalize the model with Reva: vocabulary, source_map and code_reference as governed reference tables seeded from CMS data, plus encounter, provider, condition_mention and evidence_span.
- Silver enforces the model: translation via source_map, HCC from code_reference, validation against the vocabulary, failures to quarantine with reasons.
- Abstractor and auditor consume CCDM names — directly, or through thin adapters until the POC components are changed. Apply the versioning rule (minor vs major bump, rule sets record the version).

- Category: Performance
- Weight: 25%
- Start: 10/01/2026
- Due: 12/31/2026 (reference tables live in silver by 11/30/2026)

**Measures**
- Reference tables (vocabulary, source_map, code_reference) live and silver joining to them (hcc populated from code_reference) by 11/30/2026.
- CCDM v1.0 table definitions and vocabulary reviewed with Reva and published by 12/31/2026.
- 100% of silver rows carry ccdm_version; quarantine rate and reasons reported per run.
- Carries into the 2027 plan: first agentic-hub component (abstractor or auditor) reading the CCDM tables directly.

Results: _filled in during the year_ · Feedback: _manager_

### Make ARIA pipeline production-grade

**Description.** Move the ARIA pipeline from a personal dev deployment to the team's standard engineering path, on whichever platform is chosen (Databricks today; AWS/GCP if the decision changes).
- Source control: repo in Bitbucket with the dev → mvp → prod branch model, README and runbook.
- CI/CD: Jenkins runs the test suite on every merge and deploys to the dev environment of the chosen platform (Databricks Asset Bundle today; equivalent packaging if ARIA moves), following the Confluence CI/CD standard.
- Governance: ARIA data in a team-owned, access-controlled location (Unity Catalog today) with PHI-appropriate permissions documented; nothing left in personal schemas.
- Platform alignment: work with Surya, Nikhil and Yesh to record the platform and lakehouse-location decision for ARIA, including whether it reuses the existing lakehouse ingestion pattern.

- Category: Performance
- Weight: 20%
- Start: 10/01/2026
- Due: 12/31/2026 (repo in Bitbucket target 11/30/2026)

**Measures**
- ARIA repo in Bitbucket with branch model and Jenkinsfile within 4 weeks of Bitbucket access (target 11/30/2026).
- Jenkins job running the test suite on every merge by 12/31/2026; automated deploy to the dev environment by 12/31/2026 if platform access is in place, otherwise carried into the 2027 plan.
- Data location and permissions documented; no PHI in personal schemas — by 12/31/2026.
- Platform and lakehouse-location decision recorded with Surya by 12/31/2026. If ARIA moves off Databricks, the repo, tests and CI practice carry over and only the deploy target changes.
- Carries into the 2027 plan: mvp and prod promotion path through Jenkins.

Results: _filled in during the year_ · Feedback: _manager_

## Individual Development Goals

### Build risk-adjustment and Databricks skills

**Description.** Close the two gaps I brought in: new to the medical/risk-adjustment domain, and new to the Databricks platform beyond the SQL editor.
- Domain: CMS-HCC model (V28), ICD-10 → HCC mapping, MEAT criteria, RADV audits, the CMS submission flow, and Cotiviti's risk-adjustment and payment-integrity LOBs.
- Platform: Structured Streaming, Lakeflow/DLT, Unity Catalog governance, serverless, Asset Bundles — enough to make platform choices for ARIA with confidence.

- Category: Developmental
- Weight: 15%
- Start: 10/01/2026
- Due: 12/31/2026

**Measures**
- A two-page 'ARIA coding flow explained' (chart → mention → MEAT → HCC → RAF) reviewed by Reva by 12/31/2026; able to present it to a new joiner without notes.
- Databricks Academy Data Engineer learning path (or Cotiviti's Databricks path) completed by 12/31/2026; Data Engineer Associate exam scheduled for Q1 2027.
- Shadowed at least one coder/CIS working session or risk-adjustment walkthrough by 12/31/2026.

Results: _filled in during the year_ · Feedback: _manager_

## Dependencies and assumptions

- The ARIA design review with Reva and Surya has not yet taken place; goal 1's first milestone is to hold it.
- I have not yet seen the agentic hub POC code or the CIS NLP output; the CIS contract milestone starts with identifying the CIS owner and obtaining the current output format.
- Platform (Databricks vs AWS/GCP) and lakehouse location are not final; goals are written to be platform-agnostic where possible, with the decision itself as a milestone.
- Access needed: Bitbucket project, Jenkins onboarding, Unity Catalog schemas for ARIA.
- UKG Pro shows the 2026 Goal Planning plan open for me (status Employee Planning) with a goal-setting period of 01/01/2026 – 09/30/2026; I joined 09/08/2026. To confirm with Surya: enter these in the 2026 plan now, or hold them for the 2027 plan.

Weights: 40 / 25 / 20 / 15 = 100%.

