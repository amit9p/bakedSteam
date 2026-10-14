# Reva — goals review and ARIA unblock

_Wed Oct 14, 2026 · 45 min · prepared Oct 12 · Amit's talking sheet (not for sending)_

## 1. Meeting invite (copy into Outlook)

**Title:** ARIA data hub — goals review + next steps (Amit / Reva)

Hi Reva,  
Surya suggested I review my draft goals with you first, since they're all about the ARIA data hub. Proposed agenda (45 min):  
1. Draft goals — walk through the four goals and take your edits (15 min). Draft attached.  
2. Unblocking the data-hub work — chart data I can use now, the CIS contact and what CIS produces today, the agentic-hub POC components and code access, Bitbucket repo / CI/CD, platform direction (25 min).  
3. Next steps and a date for the full design review (5 min).  
For context I have the bronze → silver (CCDM v0.1) slice running on sample data in ai-engineering-dev; I can show it if useful.  
Thanks, Amit  

_Attach: 2026-goal-plan-draft-amit-prasad.docx. 45 minutes, Teams._

## 2. Before the meeting

- Attach 2026-goal-plan-draft-amit-prasad.docx to the invite (same file Surya got).
- Have the slice-1 job open in a browser tab (ai-engineering-dev → Workflows → aria_hello_pipeline) and the silver tables ready, in case he asks what exists.
- Have the CCDM v0.1 design doc open (D:\Cotiviti\files\ccdm-v0-1-design.md).
- Keep this sheet on the second screen; type his answers under "Reva:" as you go — rough is fine.
- Don't try to cover everything. Priority order if time runs out: goals edits → data → CIS contact → POC access → design-review date.

## 3. Opening — what to say (1 minute)

> "Thanks for the time. Two things. First, Surya asked me to review my draft goals with you before he looks at them — they're all ARIA, so you're the right person to say whether they're the right goals."

> "Second, I want to use the rest of the time to unblock the data-hub work. I have the bronze-to-silver slice running on sample data, but I'm working blind on three things: real chart data, what CIS actually produces today, and how your POC components work. I don't want to design in a vacuum."

> Then: "Let me walk the four goals quickly — stop me anywhere."


## 4. The four goals — explain, then ask

### Goal 1 — Deliver ARIA data hub, Phase 1 (40%)

**Say:**

> "This is the main one: own the data layer end to end — take what CIS produces, land it in bronze, standardise it into the CCDM in silver, feed your abstractor and store its facts — conditions, evidence, MEAT — in the clinical truth graph, run the auditor over it, and produce a gold determination row with version keys that goes back to CIS. For this year I've scoped it to three things: hold the design review with you and Surya, agree the CIS handoff, and extend the slice I have through CTG and gold on sample data — at least three charts, demo in December — with your abstractor if I get access to the POC, otherwise a stand-in that produces the same shape. Hosting the auditor properly and replay from bronze are listed for 2027."

**Ask:**

- Is this the right scope for Phase 1 from your side? Anything you expected the data hub to deliver this year that's missing?
- Is "three sample charts through to gold" a meaningful milestone to you, or would you rather see one real (de-identified) chart end to end?
- The design review: can we fix a date now? Who should be in the room — Surya, Yesh, Nikhil?

**Reva:** 

### Goal 2 — Establish CCDM v1.0 as ARIA's data model (25%)

**Say:**

> "Today CCDM v0.1 is constants in code inside my silver layer. The goal is to make it the governed shared schema: vocabulary, source_map and code_reference as real reference tables seeded from CMS data, plus the entity tables — encounter, provider, condition_mention, evidence_span — with a ccdm_version on every row, and your components reading from it, directly or through thin adapters."

**Ask:**

- Does this match how you see the common clinical data model — a table set in silver — or does it live somewhere else, e.g. on top of the 2M-concept taxonomy?
- Who owns the vocabulary and approves changes to it? Is "v1.0 reviewed with you by December" realistic for your time?
- Should abstractor and auditor read CCDM tables directly, or do I build adapters that feed them in their current format?

**Reva:** 

### Goal 3 — Make ARIA pipeline production-grade (20%)

**Say:**

> "Get the code out of my personal dev workspace into Bitbucket with CI/CD and proper data governance, on whichever platform wins. I've written the measures so that if ARIA moves to AWS or GCP, the repo, tests and CI practice carry over and only the deploy target changes."

**Ask:**

- Does ARIA have a Bitbucket repo already? Your POC code must live somewhere — which project (OAI / OAIP / OAI Data Science)?
- Should the data hub live in the same repo as the agentic hub, or its own (e.g. dbx-dataops-aria)?
- Who creates repos and Jenkins jobs for OAI — Yesh, MLPlatform, DevSecOps? Jenkins or Bitbucket Pipelines for our repos?

**Reva:** 

### Goal 4 — Build risk-adjustment and Databricks skills (15%)

**Say:**

> "Development goal: a two-page 'ARIA coding flow explained' — chart → mention → MEAT → HCC → RAF — that I'd like you to review, the Databricks data-engineer learning path, and shadowing one coder or CIS session."

**Ask:**

- Would you review the explainer when it's ready (December)?
- Who could I shadow — a coder, or someone on the CIS side? Any doc or course you'd point me to for risk adjustment, HCC and MEAT?

**Reva:** 

### Wrap-up on goals

> "Anything you'd cut, add, or re-weight? I'd rather fix it now than after Surya has approved it."

**Reva's overall edits (cut / add / re-weight):** 

## 5. Unblocking ARIA — the project questions

_Ask in this order; A, B and C matter most._

### A. Data — the first blocker

_Why: Nothing real has gone through the pipeline yet; everything runs on my generated sample events._

- What chart data can I get now — de-identified samples, synthetic, or real PHI in a controlled environment? Where does it live (workspace / catalog / volume)? Who approves access?
- What did your POC run on — how many charts, from where? Can I use the same set as my test set and the start of the golden dataset?
- PHI rules for ai-engineering-dev: is anything PHI allowed there? (A personal schema is clearly not the place.) Which workspace/catalog is approved for PHI?

**Reva:** 

### B. CIS — who and what

_Why: The data hub starts where CIS output lands, and I don't yet know what that output is or who owns it._

- Who is the CIS point of contact — engineering lead and product owner? Can you introduce me this week?
- What does CIS produce today for a chart: the NLP output format (JSON? FHIR? which fields — entities, codes, text spans, confidence?) and where it goes (table, file, API)?
- Is there any event or Kafka topic today, or was aria.chart.ready only our proposal? If nothing: file drop, API pull, or a table I can read?
- How are charts / PDFs referenced — IDs, storage location?
- In the POC, how does ARIA's result get back into the CIS UI — API, table, or manually?
- Can I get one sample CIS output plus its schema this week?

**Reva:** 

### C. Agentic-hub POC — what exists

_Why: I have never seen the POC code; I can't design the handoff or the adapters until I have._

- Who owns the POC — you alone, or a team? Who else is on ARIA (coders, product owner, clinical SMEs for the MEAT rules)?
- Where is the code — Bitbucket project/repo, workspace, notebooks? Can I get read access and a 30-minute walkthrough?
- Which components exist and run today: clinical abstractor, MEAT evaluation, symbolic auditor, policy-to-logic compiler, truth graph? Notebooks or services?
- For each component: what does it consume and what does it produce (tables, JSON, files)? → this is what tells me which wrappers the data hub needs.
- Which LLMs / gateway do they use (Unity AI Gateway?) and where do they run — Databricks or elsewhere?

**Reva:** 

### D. Integration — data hub ↔ agentic hub

_Why: This is the design decision that shapes goals 1 and 2._

- Tables or messages: should the abstractor read CCDM tables from silver, or receive a payload per chart?
- Who writes the adapters — me on the data-hub side, or changes inside the POC components?
- Where should the clinical truth graph live — Delta node/edge tables in Unity Catalog (my proposal) or a graph store? What queries do you need from it?
- What should gold look like for CIS — one determination row per HCC per date of service, with evidence spans and rule_set / CTG / model / CCDM version keys? Let's agree the shape.

**Reva:** 

### E. Platform and where the data lives

_Why: Databricks vs AWS/GCP is undecided; it affects everything I build after the slice._

- Your view on Databricks vs AWS vs GCP for ARIA? Who decides, and when (GCP account expected around end of October)?
- Should ARIA's data sit in Nikhil's lakehouse / the Edifecs ingestion platform, or in its own catalog? Have you discussed it with Nikhil or Yesh?
- Which workspace and catalog are the long-term home — ai-engineering-dev/prod, or something else?

**Reva:** 

### F. Repo and CI/CD — the requests I can't raise alone

_Why: I found no ARIA repo in Confluence/Rovo; Bitbucket can't be searched from there. These need a lead or the platform team._

- Does an ARIA repo exist anywhere (Bitbucket OAI / OAIP / OAI Data Science, or GitLab MLPlatform)?
- For OAI repos: Jenkins or Bitbucket Pipelines? Who is the release approver for prod?
- Who do I ask for: a repo with branch protection, a Jenkins job + webhook, dev/prod service principals, SonarQube/Veracode registration — MLPlatform, Yesh, DevSecOps?
- Branch model the team actually uses: dev → mvp → prod (Confluence page) or develop → main (the SOP)?

**Reva:** 

### G. Ways of working

_Why: I'm the only one on the data side right now; I need a rhythm with you._

- Weekly 30-minute sync with you? Where do you track work — a Jira board I should be on?
- Where should ARIA docs live — which Confluence space for my design doc and decision log?
- Can you share the architecture doc and Confluence links you mentioned in September?

**Reva:** 

### H. Priorities — next four weeks

_Why: I want to build what you need first, not what's easiest._

- If you had to pick one: CIS ingestion, CCDM reference tables, or CTG + gold on sample data — which first?
- What would you want to see in a demo in mid-November?

**Reva:** 

## 6. Leave the meeting with

- [ ] Reva's edits to the four goals (titles, scope, dates, weights)
- [ ] CIS contact name, and a sample output + schema promised by a date
- [ ] POC repo / workspace location; read-access request raised; walkthrough date
- [ ] List of POC components with their inputs and outputs — enough to name the adapters
- [ ] His view on platform and on where ARIA data lives
- [ ] Bitbucket project for ARIA and who creates repo / Jenkins job / service principals
- [ ] First priority for the next four weeks, and the mid-November demo target
- [ ] Design-review date and attendees; weekly sync agreed
- [ ] Architecture doc / Confluence links

## 7. After the meeting

- Send me your notes the same day — rough is fine (this sheet with the Reva: lines filled, or a voice note transcript).
- I'll produce: goals v0.3 for Surya, the updated UKG text, a CIS / POC fact sheet for the knowledge base, and the action list (access requests to raise, intros to chase).
- Then message Surya: "Reviewed with Reva, updated draft attached — when suits you?"

## Appendix — if he asks "what have you built so far?"

- Running in ai-engineering-dev (my schema amit_prasad): Asset Bundle job aria_hello_pipeline with three tasks — generate_sample_events → bronze_chart_events (Auto Loader) → silver_ccdm.
- Tables: chart_events (bronze), encounter, condition_mention, quarantine (silver, CCDM v0.1 with validation rules and reasons).
- Code: src/aria package (config, sample_data, bronze, rules, silver, quality), pytest suite, databricks.yml with dev/mvp/prod targets, Jenkinsfile placeholder. Not in Bitbucket yet.
- Not built: CTG, auditor integration, gold determinations, CIS ingress/egress, anything on real data.
