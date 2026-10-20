# Binod call script — Tue Oct 20, 9:30

Binod runs the call, Reva covers the policy tool. You talk four times, about four minutes total.
Say each part when the topic comes up. If it's minute 22 and you haven't spoken, use the last section.

---

## 1. Roles — 20 seconds

One quick thing on who does what, since there was some confusion last week.

Reva owns the AI side, including the policy tool.

I own the data side. Your data coming in, our results going out, and everything we store in between.

Same pipeline, same team.

---

## 2. My tasks — 2 minutes

From my side, four things.

First, agree the format of your feed with you. What the file looks like, how the chart is linked, how the event works. I can finish that one week after I get a sample file.

Second, make our pipeline read your real files. One to two weeks after I have the sample and access to your bucket. It already runs on test data today. The access is set up by our platform team, so I'll confirm their timing after our design review.

Third, send the results back to you. One file per chart in a bucket, same way you send to us. The format we agree once we know how your app wants to read it.

Fourth, the storage behind Reva's policy tool, so every rule version and every coder review is kept. Two weeks after Reva confirms the rule format.

---

## 3. What I need from CIS — 1 minute

Three things from your side would help a lot.

One, a sample of the JSON for two or three charts, de-identified is fine, and how the chart file is linked to it.

Two, how the event works, SNS, SQS or EventBridge, and which AWS account the bucket is in, so our platform team can set up access.

Three, how your app wants the results back. Pick up from a bucket, get an event, or call an API.

I've written down what I'm assuming the file looks like. I'll send it after this call. Please correct it against a real sample.

---

## 4. Policy tool — 30 seconds

The tool is Reva's. The storage behind it is mine.

Policy documents, every version of the rules, and the coders' reviews, all kept so we can trace back later.

I'll have those tables ready in dev in two weeks. I just need one sample of the generated rules from Reva.

---

## If asked

**Can you make October 30?**
October 30 is your date for the feed, that's fine. We'll be ready to take it one to two weeks after we get the sample and access. What gets demoed before Thanksgiving, Surya and Reva are working out with Tejveer and Cloud.

**Databricks or AWS?**
Being decided this week with our platform leads. For you it doesn't change anything. S3 in, S3 out.

**What have you built so far?**
A pipeline that takes a chart file, stores it raw, keeps the latest copy per chart, breaks it into a visit record and one row per NLP mention, checks the codes, maps the values to our shared vocabulary, and puts anything bad into a quarantine table with the reason. Runs on Databricks serverless. Safe to re-run.

**What if our JSON looks different?**
That's expected. The mapping is in one place, so it's days, not weeks. The only real problem would be if the output has no page numbers or character positions. Then a lot would go to quarantine and we'd need to talk to the NLP team.

**Where does our data sit? It's PHI.**
In our governed workspace, with access control on every table. Which workspace is approved for PHI is being confirmed. For development a de-identified sample is enough.

**Do you need our word list?**
Yes. Whatever terms and codes the NLP sends, we map to our shared schema. An extract of the word list makes that exact instead of guessed.

**Who else is on your side?**
Just me on data engineering for now, under Surya. Yesh and Nikhil support on the platform.

**Can we call an API for the results?**
First version goes to S3, one file per chart. An API for "why this code?" comes after. Tell us what your screen needs.

---

## If it's minute 22 and you haven't spoken — 2 minutes

Before we close, two quick things from the data side.

My tasks. Agree your feed format, one week after a sample. Read your real files, one to two weeks after sample plus access. Send results back, format after that. Storage for Reva's tool, two weeks after he confirms the rule format.

And three asks. A sample JSON with the chart link. How the event works and which AWS account. How you want the results back. I'll send what I'm assuming right after this. Please correct it.

---

**Don't say:** that Oct 30 or Thanksgiving is our commitment · which platform you prefer · anything about inviting Tejveer or Cloud.

**After the call, today:** send Binod and Venkateshwar the contract draft with one line, "this is what I'm assuming, please correct it" · send the task list with owners and dates to everyone.
