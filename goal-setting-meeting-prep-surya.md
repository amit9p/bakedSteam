# Goal-setting meeting with Surya — prep sheet

Prepared 2026-10-08, updated 2026-10-09 after checking UKG Pro. Companion to `2026-goal-plan-draft-amit-prasad.docx` v0.2 (same folder).
Context: 1 month in (started Sep 8); UKG Pro goal-planning email received; course done.

---

## 1. What UKG Pro shows (checked Oct 9) — the period question is answered

Menu → Myself → Career & Education → **Goal Planning** (not "Goals" — that page just lists goals once they exist, so it is empty for now).

- One plan is open: **2026 Goal Planning**, review period **2026 Review Period**, status **Employee Planning**, 0 goals.
- Inside it: *Goal Setting Period 01/01/2026 – 09/30/2026*. That window is past, but the plan is still in Employee Planning with **Add** enabled, so goals can still be entered (HR opened it for new hires).
- Consequence: everything in this plan gets rated in the 2026 review, so the draft (v0.2) dates every goal to **12/31/2026** and lists the 2027 continuation under each goal. The one thing to confirm with Surya is whether to enter them in the 2026 plan now or hold them for the 2027 plan.
- **Status Oct 9:** all four goals entered in the 2026 plan as Private Draft, weights 40/25/20/15 = 100. Nothing submitted.
- Buttons on that page: **Add** = new goal; **Change Status** = move selected goals Private Draft → Ready for Review; **Remove from Plan** = take a goal out without deleting it; **Submit** (top right) = send the whole plan to Surya — don't press it before the meeting.

## 2. Teams message to Surya (send Thu/Fri, meeting Tue Oct 13 or later)

> Hi Surya — I got the goal-planning email from HR and completed the goal-setting course. I've put together a first draft of my goals for the ARIA data hub (3 performance + 1 development goal) so we have something concrete to react to — attached. Could we take 30 min next week to go through it? I'd like your input on the team priorities these should roll up to, the weights, and the dates. Tuesday or Wednesday works for me, happy to fit your calendar. I've also entered them in UKG Pro as private drafts, so once we agree I can submit the same day.
>
> One note: UKG Pro shows the 2026 Goal Planning plan open for me (status Employee Planning), so I've dated everything to 12/31/2026 — let me know if you'd rather these go into the 2027 plan instead.

Attach the .docx. Keep it to that — don't pre-explain the goals in the message.

## 3. What to say in the first 2 minutes

- "I drafted these so you have something to edit, not because I'm attached to them — please change weights, dates, scope, anything."
- One-line status so he knows where things really are: the ARIA data-hub design review with Reva hasn't happened yet (meeting got cancelled); I have a working bronze → silver slice in `ai-engineering-dev` as an Asset Bundle; I haven't seen the CIS output or the agentic-hub POC code yet; platform (Databricks vs AWS/GCP) isn't decided.
- Then walk the 4 goals in order, 2–3 minutes each.

## 4. Questions to ask (bullet list — tick as answered)

**Alignment**
- What are the department / team goals these should roll up to? (So I can fill the "Department / Team Goals" section with his words, not mine.)
- Is ARIA data hub my only accountability this period, or should other work (lakehouse support, other POCs, platform migration) be a goal too?
- Who signs off the ARIA design — Reva alone, or Reva + Surya + a platform owner?

**Scope / status of the POC**
- Who owns the CIS side? Who can give me the current NLP output format and how charts are referenced?
- Where does the agentic-hub POC code live (repo / workspace)? Can I get read access?
- Are abstractor and auditor considered "done" for Phase 1, or is the plan to rebuild them on the hub? (Decides whether goal 2 is "feed them" or "rebuild them".)

**Platform**
- Is there a target date for the Databricks vs AWS vs GCP decision? Who makes it?
- Should the goals assume Databricks (what I've built on) or stay platform-agnostic?
- Should ARIA's data live inside Nikhil's lakehouse / Edifecs ingestion path, or in its own catalog?

**Mechanics of the plan**
- 2026 plan now, or hold for the 2027 plan? (The 2026 goal-setting window shows 01/01–09/30/2026; I joined 09/08.) If 2026: are Oct–Dec goals rated in the 2026 year-end review like everyone else's?
- Weights: is 40 / 25 / 20 / 15 reasonable, or does the team use equal weights / no weights?
- Number of goals: 4 OK? (Course said 3–5.)
- Does he want me to submit via UKG Pro right after the meeting, or send him a revised draft first?
- How often do we check in on goals — monthly 1:1, quarterly?

**Access I need (ask only if not already in motion)**
- Bitbucket project for ARIA, Jenkins onboarding, Unity Catalog schemas beyond my personal `amit_prasad` schema.

## 5. Leave the meeting with

- Agreed list of goals (titles), weights, and due dates — even if rough.
- The team-goal statements for the "Department / Team Goals" section.
- Names: CIS owner, agentic-hub POC owner, platform decision owner.
- Whether the goals go into the 2026 plan (now) or the 2027 plan, and whether to submit right after the meeting or after a revised draft.
- A date for the ARIA design review (reschedule the cancelled one while you have him).

## 6. After the meeting

1. Update the .docx with his changes (save as v0.3 — keep v0.2 as the version he saw).
2. Enter each goal in UKG Pro: Menu → Myself → Career & Education → Goal Planning → **2026 Goal Planning** → **Add**. Fields: *Goal title, *Description, *Category (Performance / Developmental), Weight, *Start, *Due, Measures. Leave Results/Feedback blank. Due dates must sit inside the plan's period — use 12/31/2026.
3. Each goal saves as **Private Draft**; re-read once; tick the goals → **Change Status** → Ready for Review → OK; then **Submit** (top right) when he said to. Check the "Goals" page afterwards — the four should now show there.
4. Send him a 3-line Teams recap: goals entered, weights, next check-in date.
5. Put the final version in `D:\Cotiviti\claude_output\personal\` and tell me — I'll update the project doc and the index.

---

## Background answers (for Amit, not for the meeting)

**"CIS contracts agreed" — what it means.** CIS is the upstream system (Cotiviti's clinical intelligence / NLP layer) that reads the chart and emits the NLP output ARIA starts from. A "contract" is the written, agreed definition of what crosses the boundary between CIS and the data hub, in both directions:
- *Ingress:* the event/message schema for the NLP output (fields, types, required/optional, versioning), how the chart/PDF is referenced (ID, URL, storage path), and the delivery mechanism (Kafka topic such as `aria.chart.ready`, or whatever CIS actually supports — file drop, API, queue).
- *Egress:* the determination / recommendation message back to CIS (`aria.determination.ready`) and the coder-feedback message (`aria.coder.feedback`): fields, codes, confidence, evidence spans, version keys.
- "Agreed" = the CIS owner has reviewed and signed off, and both sides build against the same document (ideally a schema file in the repo, not just a page).
In the proposal these topics were assumptions. The real first step is small: find out who owns CIS, get a sample of the current NLP output, and write down what you get. That is why the goal says "contract defined" with the CIS owner named as a dependency.

**CCDM / Abstractor / Auditor already exist in the POC.** The goal is not to rebuild them. Goal 2 is about making the CCDM the agreed shared schema (reference tables in silver, versioned) and having those existing components read from it — via thin adapters if their inputs differ. If Surya says the POC components will be rebuilt on the hub, goal 2 widens; adjust after the meeting.

**Platform not finalized.** Goals are written platform-agnostic (contracts, schema, CI/CD practice, decision log) with "platform decision recorded" as a milestone in goal 3. If the decision lands on AWS/GCP, the bronze/silver slice is throwaway but the schema, contracts and tests carry over.

**Which plan/period — resolved.** UKG Pro shows the 2026 Goal Planning plan (2026 Review Period) open in Employee Planning status, so that is the plan; the only open point is 2026-now vs 2027, which is one line in the Teams message. Everything else — priorities, weights, scope — belongs in the meeting.
