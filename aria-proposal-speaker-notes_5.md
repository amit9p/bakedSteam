# ARIA Data Pipeline — Speaker Notes

*Full sentences, written to be read as-is. One section per slide. About fifteen minutes, plus questions.*

---

## Slide 1 — Title

Thanks for making the time. What I want to walk you through today is my first pass at how ARIA's data should flow, end to end. I put this together from the two documents — the Agentic Hub architecture and the ARIA evaluation methodology — and from our conversation last week.

I want to say clearly up front that I haven't seen the code or the data yet. So everything here is a draft based on the documents. If something is wrong or out of date, please say so as we go. That's really what this session is for.

---

## Slide 2 — Three things

Before I show the diagram, I want to set out what I think the data platform has to deliver for ARIA. There are three things.

The first is a loop with CIS. Charts come out of CIS, our recommendations go back into CIS, and the coder's decisions come back to us. That loop has to be event-driven, because on the other end there's a coder waiting to work on the chart. So we're talking minutes, not hours.

The second is one shared vocabulary. Reva, you said on the call that the shared schema was the hardest part of this. I agree with that, and I'll come back to it in a few minutes, because it's the one thing I would start on first.

The third is reproducibility. Every recommendation we make has to carry the versions that produced it — which rules, which model, which schema. If CMS audits one of these charts two years from now, we need to be able to re-run it and get the same answer with the same evidence. In this domain that's not optional. CMS audits, and a code we can't reproduce is a code we can't defend.

Everything on the next slide is built around those three.

---

## Slide 3 — The architecture

This is the whole proposal on one page. There are four lanes, and each lane has a different owner.

The top lane is CIS — the existing coding application. That's not ours. We integrate with it, but we don't build it. The second lane is our data platform, which I'm proposing to build on Databricks. The third lane is the AI components — these are Reva's, they exist today as a POC, and we host them inside the pipeline. The bottom lane is rules and reference data. The tables and jobs there are ours to build, but what goes into them comes from CMS, from the payers, from Reva's compiler, and from the SMEs.

The solid boxes are Phase 1. The dashed boxes are Phase 2. Let me walk one chart through it, following the numbers.

Box one. A chart lands in CIS. CIS runs its own NLP over it — that already exists today — and publishes one event per chart to Kafka. That event is our front door. The only thing we need to agree with the CIS team is the shape of that message. And one thing worth noting: this fires when the chart arrives, not when a coder opens it. So if ten charts arrive at two in the morning, ten events go out and our pipeline processes all ten in parallel, before anyone has logged in.

Box two. We consume that event and land it exactly as received, in a bronze table. That's our replay point. If anything downstream changes later, we start again from here without asking CIS to resend anything.

Box three. Silver. Here we clean the event into the shared clinical schema — standard field names, exact positions in the text, controlled values, and we add the HCC for each code from the reference data. Anything that doesn't fit the schema gets quarantined rather than passed on. This is where the CCDM is enforced — that's the amber strip you see running across boxes three to seven.

Box four. The Abstractor. This is the first AI step. It reads each mention in context and answers the questions the NLP can't. Is this condition current, or is it "history of"? Who asserted it — the attending, or a nurse's note? And is there MEAT evidence — did the doctor actually monitor, evaluate, assess or treat it during this visit? Every answer points at the sentence that supports it. This is Reva's model. We give it a fixed input and a fixed output, and we stamp its version on everything it produces.

Box five. The graph — the Clinical Truth Graph. The Abstractor's facts are stored here as conditions linked to evidence linked to the doctor, all versioned. This is where the audit trail lives. It's graph-shaped, but I'm proposing to store it as two Delta tables — one for the things, one for the links — rather than a separate graph database. A graph database can be added later as a mirror if we build the visual explorer.

Box six. The Auditor. This is the second AI box, but there's actually no AI in it — it's rules. It loads the chart's graph and the active rule set for that client, and runs six yes-or-no checks per code, stopping at the first failure. Same input, same answer, every time. That's what makes a decision defensible in a CMS audit. The output for each code is one of three things: codeable, needs a coder, or drop.

Box seven. Gold. One row per code, with the decision, the cited sentence, which check failed if any, and every version key. Then we bundle the chart's rows into one message and publish it back to CIS.

Box eight. Back in CIS. When the coder opens the chart, the record is on the left and our recommendations are cards on the right. Each card shows the code and the sentence that supports it. Click the citation and the chart jumps to that sentence. The coder accepts, rejects, or overrides each one. Every code still gets a human — that's CoPilot mode. When they submit, CIS publishes their decisions back to us.

Box nine. We store every one of those decisions. In Phase 1, that's all we do with them. We just make sure nothing is lost.

That's the loop. Chart in at the top left, codes out at the top right, feedback back around the bottom. The bottom lane feeds the loop rather than sitting in it — reference data loaded yearly, and the rulebook compiled by Policy-to-Logic whenever a policy document changes. Not per chart.

---

## Slide 4 — Maria

Let me make that concrete with an example. This is a made-up patient — I haven't seen real charts yet — just to show the shape of what comes out.

Maria is seventy-two, on a Humana plan. Her chart mentions three things: diabetes with kidney complications, CKD stage three, and "history of heart failure, stable on lisinopril."

Through the pipeline, diabetes and CKD both pass all six checks, so they come out codeable with high confidence. Heart failure fails the second check — the Abstractor read "history of" as history — so it goes to the coder with that reason attached.

The coder opens the chart and accepts the first two. On heart failure, they click the citation, see that lisinopril is on the active medication list, decide the condition is clearly being managed, and override to the more specific code. That takes about four minutes instead of twenty.

And that override is actually the interesting part. It tells us the Abstractor reads "history of" too literally when treatment is ongoing. That's exactly the kind of thing Phase 2 is designed to surface.

---

## Slide 5 — Phases

I've deliberately split this into two phases.

Phase 1 is the loop — everything that was solid on the diagram. The two event contracts with CIS, the four layers on Delta, the AI components hosted as versioned jobs, the rules and reference data loaded, and every coder decision captured. The outcome is simple: a chart flows end to end and cards appear in CIS. Once the open questions are answered, that's a few weeks of build.

Phase 2 is measurement. Every night, we compare what the coder decided to what we recommended and compute precision, recall, and override rate — per HCC category, per date of service, never as one average number. The evaluation doc is clear that an average hides exactly the weak spot that matters. Every miss gets traced back to the component that caused it. And the golden dataset — a couple of hundred expert-coded charts — gets replayed before any new version goes live.

AutoPilot lives here too. It's the same pipeline with a mode flag on the event. It's earned on Phase 2 evidence, not assumed.

---

## Slide 6 — The CCDM

This is the slide I want to spend a moment on, because it's where I'd start.

Here's the problem in plain terms. Four pieces of software all describe the same fact about Maria — the NLP, the Abstractor, the graph, and the rules — and today each one uses different words for it. The NLP says "present," the Abstractor says "affirmed," the policy says "documented diagnosis." A rule written in one vocabulary can't run against data written in another. It works in the POC because one person built all of it and holds the translation in his head. It won't survive two teams or a second use case.

The fix is a dictionary. One list of field names, one list of allowed values, and a version number. Silver translates every input into the dictionary at the door and rejects anything that doesn't fit. Everything after silver — the Abstractor, the graph, the rules, gold — reads and writes only dictionary words.

The tables on the right are that dictionary. Three of them define it — the allowed values, the translations from each source, and the CMS codes. The other four hold each chart in dictionary terms: the visit, the doctor, each place a condition is mentioned, and each sentence that supports it. That's simply what the Abstractor and the Auditor need to read. The names borrow from OMOP and FHIR — encounter, provider, condition — so we're not inventing vocabulary where one already exists; if the taxonomy or the existing NLP output suggests different names, I'll take those. I have a detail page on them for when we sit down on it; I won't go through it here.

What I'm proposing is this: I draft the dictionary with Reva, keeping it small — just what ARIA needs. We version it and enforce it at silver. The Abstractor and the Auditor either adopt the names, or we wrap them with thin adapters in and out until they do. Reva, that's your call. Roughly two weeks of work.

---

## Slide 7 — Platform

On platform, I'm proposing everything on Databricks. Delta for every layer. Structured Streaming for the per-chart path, with Kafka on both ends. Workflows for the scheduled work — the yearly reference loads, the Policy-to-Logic runs with an approval step, the nightly metrics, the golden replays.

It's cloud-agnostic on purpose. If the GCP move lands, this becomes a lift rather than a rewrite.

I'd also host the AI components on the same workspace. To be honest, moving the POC onto Databricks is a good first project in itself. It forces the clean split between the Abstractor and the Auditor, and it lets us see the real inputs and outputs.

Surya — you asked me to loop you in on platform choices. This is me doing that. If the platform team is heading somewhere different, I'd rather know now than in a month.

---

## Slide 8 — Questions

There are six things I can't answer on my own, and they decide how fast Phase 1 goes.

One — the CIS team. Can they publish the two events, chart-ready out and coder-feedback back? That's the whole front and back door.

Two — Reva, are the Abstractor and the Auditor separately callable today, or are they one script? That decides whether the graph store is real on day one or something we introduce.

Three — is there a draft CCDM schema somewhere, or do we start from the taxonomy?

Four — Kafka. Is there an existing cluster, or which managed option should we use?

Five — Databricks as the runtime for the AI components as well. Does that fit the platform direction?

Six — which environments are approved to hold real PHI?

If those land, the next two weeks look like this: agree the CIS contracts, draft CCDM version 0.1 with Reva, and get the POC running on Databricks.

That's everything I have. What did I get wrong?

---

## If they ask

**Why a graph?** Because the question an auditor asks is "why this code," and the answer is a path — code, to evidence, to sentence, to doctor. If we store the links, that's a query. If we don't, it's a reconstruction.

**Why two AI boxes instead of one?** That's Reva's design. Extraction and rule-checking are split so each can be reused by the next use case, and so a wrong answer can be pinned on one of them. The graph is the seam between them.

**Why Kafka and not a direct write?** So that CIS and our platform can deploy, change, and fail independently. If we're mid-restart when a coder hits submit, the message waits in Kafka instead of disappearing.

**Where's AutoPilot?** Same pipeline, with a mode flag on the event. It's gated on the Phase 2 numbers — the evaluation doc requires shadow-mode agreement and proof that coders are still catching our errors before any code skips review.

**What about PHI?** Bronze is encrypted and access-controlled from day one. The golden dataset is de-identified. Which environments may hold real charts is question six.

**Who fills the golden dataset?** People. Experts pick the charts and agree the answers by hand. No pipeline fills it. We build the table, the replay job, and the comparison.

**What's the confidence number?** The Abstractor scores each fact — how sure it is the condition is active, how strong the MEAT match is, how credible the doctor is. Gold combines those into one number per code. The exact formula is the AI team's.

**Is Maria real?** No. She's illustrative. I haven't seen real charts yet.

**Where did the four chart tables come from?** I worked backwards from what the components need. The Auditor's gates check the date of service, the doctor's credibility, the MEAT spans, and the code — that's the visit, the doctor, the mention, and the evidence. The architecture doc says the Abstractor's input is entities, assertion status, temporal context, and code candidates — the same four things. I named them the way OMOP and FHIR name them. Once I see the POC's actual input format, the adapter maps these tables into it — and if the real inputs show I've missed something, that's a fifth table.
