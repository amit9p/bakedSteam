# Design review — talking notes

Deck: `aria\aria-design-review-deck-cotiviti.pptx`, 13 slides. Slides 1 to 7 are the talk, about 27 minutes; slides 8 to 13 are the appendix, shown only if someone asks, and each has its own paragraph below. Slide numbers match the deck.

Updated Oct 26: every line is now a full sentence you can read out as written, and the compressed points on slides 2, 4 and 5 got one more sentence each so a listener doesn't have to fill the gap. Added the file handoff, where the HCC comes from, the gold grain, who computes the metrics, and "the auditor reads PRM, not our copy". Q&A answers at the end of each slide.

---

## Slide 1 — Title (1 min)

"Thanks everyone for joining.

This meeting is the design review for the ARIA data pipeline. I want three things out of it. First, your comments on the design, so I know what to fix before I build more. Second, a decision on the platform, so I know where to build. Third, agreement on how development goes from here: what I need from each of you, and what is blocked.

I'll go through six slides after this one. First the business, then why the data layer exists, then the architecture, then the platform question, and at the end the two decisions I need. Please stop me whenever you want."

## Slide 2 — What ARIA does (2 to 3 min)

"First the business picture. You know this better than I do, so I'll keep it short. I just want you to see what I'm building against.

A chart is the doctor's notes for one visit. A coder reads it and assigns ICD-10 codes. Some of those codes map to an HCC, which is a payment category. Not all of them do. And CMS only pays for a condition that is present and managed at that visit. So a history of a condition, or a possible condition, never counts, even if the words are on the page.

CMS pays the health plan based on the member's risk score. And years later, CMS can audit. They take a sample of charts, find the error rate, and apply it to the whole contract. Say ten percent of two hundred sampled charts fail; that becomes ten percent of the contract's risk payments, not ten charts. So one wrong code is not one chart's problem. That is why every code ARIA suggests has to come with its proof: which sentence, which page, which rule.

So here is the output. For each code on a chart, ARIA says one of three things: codeable, flag it, or send it to a coder. Always with the evidence. The coder decides. That is CoPilot, which is what we are building now. AutoPilot, where a code goes through without a coder, comes later, and only when the numbers prove it.

The pilot scope is three conditions: diabetes, CKD and heart failure. Aetna first.

Any corrections to this picture before I go on?"

## Slide 3 — Today's picture (1 to 2 min)

"This is how the handoff looks today, and it is correct. CIS sends the chart and its NLP output. The Agentic Hub components give back a recommendation. The coder sees it.

Nothing in this handshake changes with what I show today. CIS still sends a file and gets a file back.

The only question is what happens to all the data that went through. Today, nothing happens to it. That is fine for a demo. It is not enough for a pilot. That is the next slide."

## Slide 4 — Same handshake, plus the data hub (4 to 5 min)

"Same two boxes, same two arrows. One thing is added underneath: the data hub. Everything that goes through gets saved there, every version. The chart doesn't take a detour through the hub. It gets recorded in the hub.

Why do we need this? There are five reasons.

The first is replay. The NLP feed from CIS will keep improving, and the rules will keep changing. Every time that happens, we want to rerun old charts. If we kept the files, we just rerun them. If we didn't, we have to ask CIS to resend, every single time.

The second is reproducibility. The evaluation document says that the same chart with the same rules must give the same answer, every time. And when a rule changes, no chart that was correct before should flip to wrong. Those are both pass or fail checks on every release. To run them, you need the old run and the new run side by side, so you need both saved.

The third is audit. CMS can come back years later and ask why we coded this. By then the NLP has changed, the rules have changed, the models have changed, so we can't just rerun it. We need a frozen record for every code, as it was that day: the sentence, who signed it, the rule and its version, and what the coder did. And someone with no access to ARIA should be able to read it, as a file.

The fourth is the pilot numbers. Precision, recall, false accepts, coder time per chart. Every one of those is what ARIA said, compared with what the coder did, across thousands of charts. Today ARIA's answer is a file that goes to CIS, and the coder's decision stays inside CIS. They never meet. The hub puts them side by side, and that is the only way to get those numbers per HCC, per code, per date of service.

The fifth is feedback to CIS. The hub sees every chart, so it can tell CIS, per feed version, which fields arrived and which didn't. For example, how many mentions came without a signer, or with a confidence score outside zero to one. Right now those gaps are found by reading files by hand. With the hub they become a daily number with an owner.

This isn't only my view. The platform's own build guide says it, in its key notes: the lakehouse is the logical home of all data.

I also want to be clear about what the hub does not do. It doesn't fix the chart or the NLP. It doesn't own the policy tool; that is PRM on the platform side, with the CIS screen in front of it. And it is not on the critical path for the November demo. For the demo, a file handoff is enough."

If someone says this is too much for a pilot: "For the demo, I agree. A file handoff is enough and that is what we'll do. The hub is for the pilot numbers and for audit. Without it, the targets in the evaluation document can't be computed."

## Slide 5 — The architecture (7 to 8 min)

Rhythm for every box: where it sits, what comes in, what it does, what goes out. Point at the box, say the four lines, move on. The handoffs are said once, at the start.

"This is the one picture. Let me set it up in three sentences.

Four lanes. CIS on top, outside our system. The data hub second, ours. The AI components third, from the platform side. Reference data and feedback at the bottom.

Two kinds of handoff. Between CIS and us, it is S3: a file comes in, a file goes out. Between us and the components, for the demo, it is also a file: we write a folder, they read it and write back. Every hub box is a job that fires when a file lands. Nothing polls.

Two doors. Lookup, for codes and reference data. Explain, for why this code. Nobody reads our tables directly.

Now one chart, left to right.

Box one is CIS. One ZIP per chart lands on their S3 bucket: the PDF and the NLP JSON. An event tells us.

Box two is bronze, in the hub. In: that ZIP. We keep it exactly as it came, every version. Out: the raw record we can always replay from.

Box three is silver, in the hub. In: the bronze JSON. We clean it into six tables in the shared clinical schema: chart, note, mention, code candidate, relationship, MEAT evidence. Anything bad goes to a seventh table, quarantine, with a reason and an owner; that becomes a daily report to CIS. Out: clean facts, and one JSON per chart written to the handoff folder.

This is the one place we build tables ourselves, so let me say why we don't just pass the bronze JSON straight to the abstractor. The abstractor is the only consumer that wants the whole chart at once. Everyone else wants a field. The auditor's gates read the candidate codes, the date of service per mention, the signer and credential from the note. The explain API lands on one mention row, page and offsets, to show the sentence. The quality report is a count on the note table. CIS's metrics are a join on the code candidate table. If those live inside a nested JSON, every one of them parses the same blob its own way, and nobody can run a query. So we split it once, by the six sections of the platform's own extraction requirements, and bundle it back into one JSON for the abstractor. The abstractor sees no difference. Everyone else gets a table.

The other two stores are different. The truth graph and the gold table, we don't design. The abstractor's output is the graph; we store it as it comes. The auditor's output is the determination; we store it as it comes. Silver is ours because it's the only layer where the shape of the data is our call.

Box four is the clinical abstractor, on the platform side. In: that JSON. The NLP found words; the abstractor decides which words are true for this patient on this visit. Is it real, is it current, who signed it, is there MEAT proof. Out: the clinical truth graph, one per chart, back to the handoff folder.

Box five is the truth graph store, in the hub. We load that graph and keep it, versioned per run. It is the auditor's set of facts.

Box six is the symbolic auditor, on the platform side. In: the graph, the candidate codes, and the active ruleset from PRM. It runs six gates per code and stops at the first one that fails. No AI; same input, same output. The HCC is attached here, at the first gate, from the crosswalk. Out: one determination per code, back to the handoff folder.

Box seven is gold, in the hub. In: those determinations. One row per code: codeable, flag, coder queue, or drop; which gate failed; the rule and snapshot id; every version. Out: one results file per chart, to S3, for CIS.

Box eight is the coder, in the CIS app. They see the results file and accept, reject, or override each code. If they ask why, that is the explain door.

Box nine is feedback, in the hub. In: the coder's decisions, from CIS, as a file or an API call. We join them to gold and compute the pilot numbers: precision, recall, overrides, coder time, per HCC, per code, per date of service.

That is the chart. Now the rules, in one breath.

A guideline PDF goes into PRM. Policy-to-logic drafts rules from it. Coders review them in the CIS screen through PRM's API. Changed rules go round again. A second person promotes the set. The auditor fetches the active set from PRM at run time. We copy PRM's tables nightly for explain, eval, and audit. The copy never drives a decision. The auditor reads PRM.

The tags: built runs today on synthetic data, that is bronze and silver. POC means the platform code exists. New is PRM. Next is slice two.

Questions on the picture before the platform?"

If asked where the hub is in the platform's architecture page: "It isn't drawn there. That page is the component view. The build guide puts all data in the lakehouse, and this picture is that, drawn out."

If asked why six silver tables when the abstractor reads one JSON anyway: "Three reasons, and none of them are for the abstractor. The gates read fields, not a blob: gate one needs the candidate codes, gate three the date of service per mention, gate four the signer and credential from the note. The explain API and the audit walk-back land on a silver row: chart, mention, page, offsets. And the quality report is a group-by on the note table: how many mentions had no signer this week. The six are the six sections of the extraction requirements, one table each. The abstractor still gets one JSON per chart; the export job bundles the six back together."

If asked what the quarantine checks are: "Six today, all mechanical. Confidence outside zero to one. A code at category level when a specific one exists. A mention with no page or offsets. An assertion that isn't one of the six states. A note with no signer where the note type needs one. An unreadable ZIP, which fails the whole chart. Every check is a range test on the field or a lookup against a reference table we keep. No model makes the call. The list came from the gaps the platform team found in real exports; it will grow when the sample ZIP arrives. Slide 10 has it."

If asked what happens to a quarantined mention: "It sits there with a reason and an owner. Nobody edits it. The counts go to CIS and the NLP team nightly; they fix the export code; the next report shows the reason at zero. When the chart is re-sent or replayed, the mention lands in silver and the quarantine row marks itself resolved. A quarantined mention can cost us a recommendation, never a wrong one."

If asked what's in the ZIP: "It's in the appendix, on the contract slide. One ZIP per chart, the PDF plus the JSON, and the field list showing what's there today and what isn't."

If asked for the blueprint: "First appendix slide. One row per box: what it is, who writes it, who reads it, whether it's PHI, and how long it's kept. The full table is in the document I'll send."

If asked how the abstractor reads our data: "For the demo, from a file. We write one JSON per chart to a folder, in the same shape as the CIS JSON after cleaning. The abstractor reads that, writes the graph back as a file, and we load it. The one thing I still need from the platform side is the field names their parser expects, so we match them."

If asked why we need an explain API when the results file has everything: "The file is the push: every code, every chart, every day. Explain is the pull: one code, when someone asks why. It walks the graph and pulls the sentence, the signer, the rule text, and the guideline page. That's too much to put in every file for codes nobody questions. And the audit package later is that same answer, frozen."

If asked where the rules live: "In PRM. Policy-to-logic writes them, PRM owns them, the auditor asks PRM, and we keep a copy every night. We never write to PRM."

If asked why the lookup bar doesn't reach the auditor: "It should. The auditor uses it for the crosswalk and the chronic list, and so does policy-to-logic. That's a drawing shortcut, and I'll extend the line."

## Slide 6 — Where the hub runs (4 to 5 min)

"This one is for you to decide. I'll give you the facts.

Some things are fixed either way. CIS is on AWS. The components run on Kubernetes on AWS. PRM is Postgres plus S3. There is no GCP environment yet.

There are two options. Databricks on AWS, where the pipeline runs today. Or AWS native, using the existing lakehouse pattern. The table goes row by row: where the tables live, how we read the CIS bucket, how the components and PRM connect, the write API the platform design expects from us, the time to the first real chart, and the risk on each side.

What I'm asking is simple: which of the two the hub should run on. The data layer is the same on both. The contract, the tables, the gold record, the golden dataset, and the metrics all carry over. Only the plumbing changes. I have no view on where it has to live long term. You do."

If asked for your view: "If you ask me, I wouldn't rebuild now. It runs on Databricks today and the data layer carries over. But if it should sit with the lakehouse team, the change is the plumbing, not the design."

If asked about the write API: "The platform design assumes a lakehouse write API for the policy records, owned by data engineering, with no date yet. I don't know what it should be. It could be a SQL endpoint, or Lakebase if we have it, or the records stay in PRM's Postgres and we keep copies. That's decision three."

If asked about buckets and accounts (appendix slide 13 has the picture): "Three handoffs, three owners. CIS owns the chart bucket and we get a role. We own the folder the components read and write, and they get a role. PRM owns the policy store, Postgres plus S3, and we get read access. Nobody shares a bucket. Everything is event driven, nothing polls."

## Slide 7 — Decisions (3 min, then discussion)

"Two decisions I need today, and a short list I'm tracking that isn't for this room.

One. Which platform does the hub run on: Databricks on AWS, or AWS-native in the existing lakehouse.

Two. CIS drops real charts on the thirtieth. For our jobs to read them we need a role into CIS's bucket, and a workspace that is cleared for PHI. Neither exists today and I can't create either. Who sets them up, and by when?

The tracking list, so you've seen it. The platform plan assumes a lakehouse write API from data engineering later, with no date; I'll settle owner and date with Reva once PRM's schema is real. Who provisions the Agentic Hub's AWS account, Yesh or Nikhil, follows from decision one. With Reva: whether CIS pre-runs the NLP or the hub calls it, the golden dataset size and who holds it, and whether a file handoff is acceptable for the November demo. With Binod: one sample ZIP and a field-by-field answer on the contract.

Can we take decision one first? Two depends on it, and the picture for two is in the appendix, slide 13."

Close: "Thanks. I'll send the deck, the contract draft and the blueprint today, with a short note on what we decided and who owns what."

---

## Appendix — only when someone asks. Jump to the slide, read the paragraph, come back.

## Slide 8 — Appendix divider (10 sec)

"These are the zoom-ins behind the picture: the data blueprint, the silver tables, the CIS contract, the policy lane, and the accounts and roles. I'll open whichever one you want."

## Slide 9 — Data blueprint (1 to 2 min)

"This is every kind of data the hub keeps, one row each. For each one: what it is, who writes it, who reads it, whether it holds PHI, how it's versioned, and how long we keep it.

The chart path runs top to bottom: bronze events, the six silver tables, quarantine, the truth graph, gold determinations, the results file, the decision package for audits, coder feedback. Then the policy copy, the reference data, the golden dataset, and the evaluation metrics.

Four rules apply to every row. Nothing that feeds a decision is ever overwritten; reruns create versions. Every decision row carries its version keys, so it can be reproduced. PHI tables are readable only by the pipeline, the components and the explain API; people get masked views. And other systems reach data through the two doors or the S3 handoffs, never by reading tables directly.

The full table with twenty-two rows is in the blueprint document I'll send."

If asked how long PHI is kept: "Bronze, silver, the graph and gold for years, because they are the replay and audit source. Quarantine for one year. The results file ninety days. The handoff folder thirty days; it is not a record."

## Slide 10 — Silver tables (1 to 2 min)

"Silver is six tables plus quarantine. The six come straight from the platform's extraction requirements document, one table per section: chart, note, mention, code candidate, relationship, and MEAT evidence.

Chart holds the envelope: patient, encounters, dates of service, facility, payer, HCC model version. Note holds who signed it and when. Mention is every clinical finding the NLP found, with page and offsets, the section, the six-state assertion, severity. Code candidate is every code proposed for a mention, not just the winner. Relationship links two mentions: this medication treats that condition. MEAT evidence is the proof a condition was managed at the visit.

The seventh table is quarantine. A row goes there when it fails a data-quality check: a confidence outside zero to one, a code at category level, a mention with no page or offsets, an assertion we don't recognise, a note with no signer, an unreadable ZIP. Each row carries a reason code and an owner, CIS or the NLP team. One bad mention doesn't stop the chart. The reasons roll up into a daily report, which is how the feed gets fixed.

Every row is stamped with the schema version, the NLP version and the feed version."

If asked why not just drop bad rows: "Because then a code we couldn't support would either fail a gate for the wrong reason, or pass one it shouldn't. Quarantine keeps the reason honest and gives CIS a number to fix."

## Slide 11 — CIS contract v0.2 (1 to 2 min)

"This is what we've asked CIS to agree to, field by field. It's still our draft; the first real correction comes from a sample ZIP.

The left side is the envelope. One ZIP per chart on their bucket, with the PDF and one JSON, and an S3 event. Re-sends are allowed; we keep every version. Out, one results JSON per chart, always, including 'not processed' with a reason. At coder time, the explain API. Coder decisions come back with a reason code.

The right side is what the platform team found when they checked real exports against what the auditor needs. The biggest gap: no signer, no credential, no co-signature, no signature time. That's the provenance gate. Also missing: facility type, payer, HCC model version. Confidence is a raw match count, not a zero-to-one score. Three true-false assertion flags instead of six states. One winning code instead of all candidates. Relationships and MEAT evidence not seen at all.

The ask of CIS is simple: for each must-have field, present, can add, or not available. Then we build silver against what's real and report the rest."

If asked whether the demo waits on this: "No. The pipeline runs on synthetic data in this shape today. Real charts need the sample ZIP and the bucket access, which are decision two."

## Slide 12 — Policy lane (1 to 2 min)

"This is the platform's PRM, and the shaded row at the bottom is the only part that's ours.

One guideline, eight steps. A PDF goes into PRM. Policy-to-logic drafts rules from it, as a job that takes hours. PRM stores them as a snapshot that can't be changed, with a digest. Quality checks run. Coders review each rule in the CIS screen, through PRM's API; each verdict is tied to the exact rule version they saw. Changed rules go round again. A second person promotes, and PRM marks the set active for that payer and date-of-service period. At run time the auditor asks PRM for the active set and gets the compiled rules.

Our row: we copy PRM's records nightly into lake tables, for the explain API, evaluation and audit walk-back. Manual export first, a job once PRM's schema is frozen. The copy never drives a decision. The auditor reads PRM, and we never write to PRM."

If asked why the hub doesn't store the rules: "A coder saving a review needs an instant, consistent answer, and the lakehouse write API doesn't exist yet. So PRM is the source of truth now, the lake holds a copy, and the move to the lake is the platform's strategic phase. That's decision three."

## Slide 13 — Accounts, buckets and roles (1 to 2 min)

"This is the access picture behind decision two. One rule for reading it: every arrow points at a storage box and says who reads or writes it. Storage lives in its owner's column; the other side reaches across with the role in the yellow box.

Left, CIS. They own the chart bucket. Charts in, results out, decisions back, all in their bucket. Our jobs reach across: read in and decisions, write out. CIS needs no access to anything of ours.

Middle, the hub, wherever decision one puts it. We own the handoff folder. We write the clean chart JSON; the components write the graph and the verdicts back into it. They get one role: read ccdm, write ctg and determinations. We also own the lake tables, reference data included; only our jobs write there, and everyone else comes through the doors.

Right, the Agentic Hub account. PRM is the store of record. The auditor reads its rules from PRM by API, inside that account. Our nightly job reads PRM for the copy. We never write to PRM.

Three cross-account grants in total, the yellow boxes. Every arrow is a file, an event and a job. Nothing polls, nobody shares a bucket, and every PHI location needs a KMS key the other side's role can use. This is what I need from whoever owns the middle column."

If asked "which account is the middle one?": "That's decision one. Databricks on AWS means Yesh's workspace account and a Volume; AWS-native means Nikhil's lakehouse account and an S3 prefix. The three grants are the same either way."

---

## Time

Slide 1: 1 · slide 2: 3 · slide 3: 1.5 · slide 4: 5.5 · slide 5: 8 · slide 6: 4.5 · slide 7: 3. About 26 to 27 minutes. Leaves about 20 for discussion.

If running long: on slide 5, skip the rules paragraph. Say "the policy lane is in the appendix. PRM owns it, we copy it nightly, and the auditor reads PRM."

## Don't

- Give dates for the demo or for PRM. They are not ours to give.
- Say "recommend" on slide 6. Say "your call".
- Say the hub improves data quality. It keeps, flags, and reports.
- Argue the Binod point. Slide 3 already agrees with him; slide 4 adds to it.
- Use gate numbers when talking about the coder's screen. Say "not documented as active", not "L2". The numbers are ours.
- Say "we store the rules". We copy them. PRM stores them.
