Hi Meghan, thanks for the heads-up — happy to do the interview. You can let Abhishek know there won't be a hands-on coding exercise. The technical portion will focus on architecture and system design (data platform design, pipeline reliability, Databricks/cloud trade-offs), plus how he's led and grown data engineering teams. I've got the resume and will review before the call. Thanks!
"Meghan asked me to interview Abhishek Gakhar for your Director role — I'll focus on architecture/system design + team leadership, no coding exercise. Anything specific you want me to probe?"
Thanks Surya, that's clear enough — I'll cover pipelining architecture, Databricks, native AWS and team leadership. Settling in well, thanks! Separately, could we set up a recurring weekly 30-min catch-up? Happy to send the invite for a day that suits you.

Databricks → SQL Editor, run once:
CREATE VOLUME IF NOT EXISTS ai_engineering_dev_catalog_1688890232235261.amit_prasad.personal;
Unzip on the office laptop. Inside is one folder, aria-pipeline-starter.
In aria_data_pipeline (VS Code Explorer): delete the contents of resources\ and src\. Leave .venv\ alone.
Copy the contents of aria-pipeline-starter into aria_data_pipeline; overwrite databricks.yml when asked. Check the root now shows databricks.yml, resources\aria_hello_pipeline.job.yml, src\aria, src\notebooks, tests, CLAUDE.md, Jenkinsfile.
Databricks panel → Bundle Resource Explorer: the job is now [dev amit_prasad] aria_hello_pipeline, three tasks. Hover → Deploy and Run. If it asks to remove sample_job, yes.
Open the run from the link in the terminal. Three tasks: generate_sample_events → bronze_chart_events → silver_ccdm.
Catalog Explorer → amit_prasad: chart_events 4 rows, encounter 3, condition_mention 8, quarantine 2.
Run once more (hover → Run): chart_events 8, the other three unchanged.
