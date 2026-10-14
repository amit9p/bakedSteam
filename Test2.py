The S3 feed you and he discussed — the AWS service that pulls from the CIS app and publishes the JSON + chart to S3 with an event notification. His words: CIS is targeting Oct 30 to have that data available to us. He didn't say more; I've asked him for a sample JSON before then. His message below."
÷÷÷÷÷÷
Title: ARIA – CIS dates, tech stack, roles
Body:
Hi Surya, Reva,
30 minutes to align before we talk to Binod/CIS. Binod says CIS plans to have the data on S3 by Oct 30 and leadership expects a demo to internal customers before Thanksgiving; Tejveer, Sundar and Cloud are tracking it. He had not heard about my role.
What we tell CIS about those dates.
Data hub stack: Databricks on the CIS S3 feed, or AWS-native.
Roles (Reva: AI components, me: data in and out) and who speaks to Tejveer and Cloud.
Thanks,
Amit

++±++±++++
To Reva
Not yet. Binod hadn't heard about my role. He says Tejveer, Sundar and Cloud are tracking this, CIS data is targeted for Oct 30, and leadership expects a demo to internal customers before Thanksgiving (they met this morning). I'm taking it to Surya tonight. Can you join a short sync with Surya tomorrow so the three of us say the same thing to Binod and leadership? I'll send the invite once Surya gives me a time.
To Surya
Surya, I need 15 min first thing tomorrow. Binod (CIS) messaged me today — he hadn't heard about my role on ARIA and says Tejveer, Sundar and Cloud are tracking it: CIS data by Oct 30 and a demo to internal customers before Thanksgiving, decided in a leadership meeting this morning. He wants Tejveer and Cloud looped in. I've told him the commitments sit with you and Reva and haven't promised anything. Can you take the Tejveer/Cloud alignment, and can we agree what is actually committed for Thanksgiving before I reply further? Reva is in the loop and also wants to align.
To Binod
Thanks Binod, that's useful context, the dates especially. Surya brought me in last month for the data side of Reva's ARIA work; the commitments and the demo plan sit with Surya and Reva, so I'll make sure they pick this up with Tejveer and Cloud directly. From my side, a sample of the JSON (de-identified is fine) ahead of Oct 30 would help us be ready for your feed. Thanks for the support.
======
Then go to Surya immediately (1:1, and it's fine that it's 5 pm):
Surya, need your steer. Following Reva's suggestion in our 1:1 today, I messaged Binod Chamoli (CIS) about the S3 feed for ARIA. He's confused about my role, says he didn't hear about it from leadership, and asked that Tejveer and Cloud be included in a call. I've told him I'm bringing it to you and Reva. How would you like to handle it — do you want to set up the call, or should I send the invite with you and Reva and include Tejveer and Cloud? I'll hold off on anything further to Binod until I hear from you.
And a short FYI to Reva:
Reva, heads-up: I reached out to Binod as discussed. He hadn't heard about my role from leadership and asked for Tejveer and Cloud to be on a call. I've looped Surya in and told Binod we'll set it up together. Could you back me up with Binod on the "same pipeline, data side" point?

<><><><<><><>
Yes, those three, plus the ingest from your S3 feed on one end and the output back to CIS on the other.
Schema: landing tables for your JSON + chart, the standardised clinical data model Reva's components read, and versioned output tables (recommendations, evidence, reasons).
Logic: ingest, validate, standardise, run Reva's components, store results — replayable for audit.
APIs for CIS to get recommendations and the evidence behind them.
Reva owns the AI logic; I own the data in and out of it.




Short version: the three tables come from the problem, not from the data. The problem in your design doc is "every component spells the same fact differently." Any shared clinical data model solves that with the same three things: a list of allowed words (vocabulary), a translation from each source's words into them (source map), and the official code book (code reference). OMOP has exactly this (vocabulary, concept, source-to-concept map), so does FHIR with value sets; your speaker notes already say the names borrow from OMOP and FHIR. The structure is standard; you didn't need data to know it.
Where the values came from:
vocabulary: industry-standard lists, not guesses. The assertion values (present / history / negated / possible) are the categories clinical NLP engines have used for 20 years (the NegEx/ConText scheme that cTAKES and commercial engines follow). The sections are the standard parts of a clinical note (HPI, Problem List, Medications, Assessment/Plan, Labs, Radiology). MEAT is CMS's own documentation standard for risk adjustment. Disposition is the only home-grown one: it's ARIA's workflow outcome (codeable / coder queue / drop) from Reva's design.
code_reference: public CMS data. The ICD-10-CM code list is published every year (fiscal year Oct 1 to Sep 30, hence valid_from 2025-10-01, valid_to 2026-09-30), and the CMS-HCC model maps each code to an HCC. The example rows follow the V28 model: E11.22 → HCC 37, N18.30 → HCC 329, I50.9 → HCC 226, I10 → no HCC. The RAF weights in the examples are placeholders; the real ones come from CMS's rate-announcement tables per model and population segment.
source_map: this is the one that genuinely depends on data you haven't seen. The nlp_v3 rows are illustrative, showing the shape of the translation. Your own design notes that the POC's Abstractor says status: affirmed where we'd say ACTIVE, which is exactly the kind of row this table holds. Its real rows get written the day you see one CIS payload.
So if Reva asks "how did you build reference tables without data", the answer is:
"The tables are the standard shape any clinical data model has: allowed values, a translation per source, and the code book. The values in vocabulary are industry standards; the code reference is public CMS data loaded yearly; the source map is the only one that depends on CIS's output, and its rows are placeholders until I see one real payload, which is one of my asks today."
Why "loaded once, changed rarely": each has its own slow clock. Vocabulary changes only with a CCDM version bump; source map when a new source or a new NLP version arrives; code reference once a year when CMS publishes. None of them changes per chart, which is what separates Group A from Group B.



Hi Binod, I’m Amit Prasad, Principal Data Engineer in Surya Vadlamani’s group. Reva Sharanbasappa suggested I reach out: I’m building the data layer for ARIA, the part that takes what CIS produces for a chart (NLP output + the chart), runs it through the ARIA pipeline, and sends recommendations back to CIS.

Reva mentioned your team is planning to publish NLP output and charts to an S3 bucket with an event trigger. Could I get 30 minutes with you or the engineer leading that? I’d like to understand the mechanism and timeline, the output format (a sample payload would help a lot), and how CIS wants to receive and show ARIA’s results. I’ll bring a one-page picture of the pipeline so it’s concrete.

Any slot next week works for me. Thanks, Amit

-----

Thanks Binod, that lines up well with my plan.
I'm building ARIA's data layer: it consumes what your service publishes to S3 (the JSON + chart) on the event notification, keeps an untouched copy for replay/audit, standardises the NLP output into our common clinical data model, runs Reva's components on it, and returns a per-chart recommendation package (codes, evidence with page/position, reasons) to CIS. Which platform it runs on (Databricks or AWS native) is being decided with Surya this month; your S3 + event design works for either.
Three things would help me build against it now:
One sample JSON (de-identified is fine) and how the chart is referenced.
The event mechanism (S3 → EventBridge / SNS / SQS?) and the AWS account, for cross-account access.
How CIS wants to receive ARIA's output: S3 drop, API, or a table.
Happy to go through it on a 30-minute call with you or the engineer building the service, any time next week. Thanks, Amit

___

Fair question, and sorry for jumping into detail. Surya is my manager; he and Reva asked me to take on the data-engineering side of Reva's ARIA pipeline, so I'm the same consumer as Reva, not a new one. Nothing is decided; I only wanted to understand what CIS plans to publish. Let me set up a short call with Reva and Surya included so you hear it from them directly. Thanks, Amit


Not on top of it — inside it. Reva builds the AI components; I'm the data engineer on his ARIA effort, handling the data plumbing around them (landing your feed, storing it, returning the results). One pipeline, one team. Surya is my manager and is aware. I'll set up a short call with Reva and Surya so you hear it from them directly; sorry for the confusion.



-------

Understood. I'll take this to Surya and Reva now and we'll set up the call with Tejveer and Cloud included. Thanks for flagging it.
Then go to Surya immediately (1:1, and it's fine that it's 5 pm):
Surya, need your steer. Following Reva's suggestion in our 1:1 today, I messaged Binod Chamoli (CIS) about the S3 feed for ARIA. He's confused about my role, says he didn't hear about it from leadership, and asked that Tejveer and Cloud be included in a call. I've told him I'm bringing it to you and Reva. How would you like to handle it — do you want to set up the call, or should I send the invite with you and Reva and include Tejveer and Cloud? I'll hold off on anything further to Binod until I hear from you.
And a short FYI to Reva:
Reva, heads-up: I reached out to Binod as discussed. He hadn't heard about my role from leadership and asked for Tejveer and Cloud to be on a call. I've looped Surya in and told Binod we'll set it up together. Could you back me up with Binod on the "same pipeline, data side" point?
