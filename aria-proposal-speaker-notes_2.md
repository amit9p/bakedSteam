# ARIA Data Pipeline — Speaker Notes

*One block per slide. Say it your way. About 15 minutes plus questions.*

---

## Slide 1 — Title  [under a minute]

Thanks for the time. This is my first pass at how ARIA's data should flow, end to end. I built it from the two docs and our call last week.

I haven't seen the code or the data yet, so treat it as a draft. If something's wrong, tell me — that's what this is for.

---

## Slide 2 — Three things  [1.5 min]

Before the diagram — three things the platform has to do.

One: a loop with CIS. Charts come out, recommendations go back, the coder's decisions come back to us. Has to be event-driven, because a coder is waiting.

Two: one vocabulary. Reva, you said the shared schema was the hardest part. I agree. I'll come back to it — it's where I'd start.

Three: reproducibility. Every recommendation carries the versions that made it. CMS audits a chart two years from now, we re-run it and get the same answer. In this domain that's not optional.

The diagram is built around those three.

---

## Slide 3 — The architecture  [5 min]

Four lanes, four owners. CIS on top — not ours. Our data platform on Databricks. Reva's AI components — they exist as a POC, we host them. Rules and reference data at the bottom — the tables and jobs are ours; what goes into them comes from CMS, the payers, Reva's compiler, and the SMEs.

Solid boxes are Phase 1. Dashed are Phase 2. One chart through it:

**One.** Chart lands in CIS. CIS runs its NLP and publishes one event per chart. That's our front door. Fires when the chart arrives, not when a coder opens it — ten charts at 2 AM, ten events, all in parallel.

**Two.** We land it raw. Bronze. Replay point.

**Three.** Silver. Clean it into the shared schema, add the HCC from reference data. This is where the CCDM is enforced — that amber strip across three to seven.

**Four.** The Abstractor. Is the condition current or history? Who said it? Is there MEAT — did the doctor actually do something about it this visit? Every answer cites a sentence. Reva's model, we host it.

**Five.** The graph. Those facts stored as condition, evidence, doctor, all linked. Versioned. Where the audit trail lives. It's graph-shaped, but it's two Delta tables — nodes and links — not a separate graph database. That can come later if we build the visual explorer.

**Six.** The Auditor. No AI — six yes/no rule checks per code. Same input, same answer. That's what survives an audit. Codeable, needs a coder, or drop.

**Seven.** Gold. One row per code with the evidence and every version. Then one message back to CIS.

**Eight.** The coder opens the chart, sees our cards beside it, clicks a citation, the chart jumps to the sentence. Accept, reject, or override. Every code still gets a human — that's CoPilot. Submit, and the decisions come back.

**Nine.** We store every decision. That's Phase 1. Don't lose anything.

That's the loop. The bottom lane feeds it — reference data yearly, the rulebook compiled whenever a policy changes, not per chart.

---

## Slide 4 — Maria  [1.5 min]

Same thing with an example. Made-up patient, just to show the shape.

Maria, Humana. Her chart mentions diabetes with kidney complications, CKD stage 3, and "history of heart failure, stable on lisinopril."

Diabetes and CKD pass all six checks — codeable. Heart failure fails check two — the Abstractor read "history of" as history — so it goes to the coder with the reason.

The coder accepts the first two. On heart failure, clicks the citation, sees lisinopril is an active med, decides it's being managed, overrides to the specific code. Four minutes instead of twenty.

And that override tells us something — the Abstractor reads "history of" too literally when treatment is ongoing. That's a Phase 2 data point.

---

## Slide 5 — Phases  [1 min]

Phase 1 is the loop. Two CIS contracts, the four layers on Delta, the AI components hosted, rules loaded, feedback stored. Outcome: a chart flows and cards show up in CIS. A few weeks, once the open questions are answered.

Phase 2 is measurement. Nightly, compare what the coder did to what we said. Precision, recall, override rate — per HCC, never one average. Trace every miss to the box that caused it. Golden dataset replayed before any new version goes live.

AutoPilot lives here too. Same pipeline, a mode flag. It's earned on Phase 2 evidence, not assumed.

---

## Slide 6 — CCDM  [1.5 min]

This is where I'd start.

Four pieces of software describe the same fact about Maria — the NLP, the Abstractor, the graph, the rules — and today each uses different words for it. One says "present," one says "affirmed," the policy says "documented diagnosis." A rule written in one vocabulary can't run against data in another. It works in the POC because one person wrote all of it. It won't survive two teams or a second use case.

The fix is a dictionary. One list of field names, one list of allowed values, a version. Silver translates every input into it and rejects what doesn't fit. Everything after silver reads and writes only dictionary words.

The tables on the right are the dictionary. Three define it, four hold each chart in its terms. I have a page on them for when we sit down.

What I'd do: draft it with Reva, small — just what ARIA needs. The Abstractor and Auditor either adopt the names or get thin wrappers until they do. About two weeks.

---

## Slide 7 — Platform  [1 min]

Everything on Databricks. Delta for every layer, streaming for the per-chart path with Kafka on both ends, Workflows for the scheduled work.

Cloud-agnostic on purpose — if the GCP move lands, it's a lift, not a rewrite.

And I'd host the AI components on the same workspace. Moving the POC onto Databricks is a good first project — it forces the split between Abstractor and Auditor and we see the real inputs and outputs.

Surya — you asked me to loop you in on platform choices. This is that. If the platform team is heading somewhere else, I'd rather know now.

---

## Slide 8 — Questions  [rest of the time]

Six things I can't answer myself.

One — CIS. Can they publish the two events?

Two — Reva, are the Abstractor and Auditor separate today, or one script?

Three — is there a draft CCDM, or do we start from the taxonomy?

Four — Kafka. Existing cluster, or which managed one?

Five — Databricks for the AI components too. Does that fit?

Six — which environments can hold real PHI?

Next two weeks if those land: CIS contracts, CCDM v0.1 with Reva, POC onto Databricks.

That's it. What did I get wrong?

---

## If they ask

**Why a graph?** Auditors ask "why this code." The answer is a path — code, evidence, sentence, doctor. A graph makes that a query.

**Why two AI boxes?** Reva's design — extraction and rule-checking split so each can be reused and a miss can be pinned on one of them. The graph is the seam.

**Why Kafka, not a direct write?** So CIS and we can deploy and fail independently. If we're restarting when a coder hits submit, the message waits instead of disappearing.

**Where's AutoPilot?** Same pipeline, a mode flag on the event. Gated on Phase 2 — shadow-mode agreement and proof coders still catch our errors.

**PHI?** Bronze encrypted and access-controlled from day one. Golden set is de-identified. Which environments — question six.

**Who fills the golden dataset?** People — experts pick charts and agree the answers. No pipeline fills it. We build the table, the replay, the comparison.

**What's the confidence number?** The Abstractor scores each fact — how sure it's active, how strong the MEAT match, how credible the doctor. Gold combines them per code. The formula is the AI team's.

**Is Maria real?** No — illustrative. I haven't seen real charts yet.
