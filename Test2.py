
PR Title

Make generator functions compatible with Glue job context argument


---

🧩 PR Description

Context:
The Glue orchestration layer appends a job_context object as an additional argument when invoking generator functions through common_utils.execute_function().
Some generator functions, such as get_reportable_accounts, were defined with a fixed number of parameters (3 DataFrames). This caused Glue jobs to fail with
TypeError: get_reportable_accounts() takes 3 positional arguments but 4 were given.

Change:

Updated get_reportable_accounts() to include flexible parameters (*_args, **_kwargs) to absorb the extra job_context argument passed by the framework.

This makes the generator consistent with other framework components (e.g., calculators and consolidators), which already handle optional args gracefully.

Added _args, _kwargs naming convention to avoid SonarQube “unused parameter” warnings.





df = df.filter(F.col("reporting_status") == "R")
from datetime import datetime, timezone
CreditBureauReportingServiceReportingOverride.initiated_date: datetime(2024, 1, 15, tzinfo=timezone.utc)

assert_df_equality(expected_df, result_df, ignore_row_order=True, ignore_nullable=True, ignore_column_order=True, ignore_time_zone=True)

we started testing in vo1x1t02, but the Glue job failed with a subnet IP exhaustion error.
Looks like the Glue connection needs an available subnet — could you please check?




Hi team, our Glue job failed with:
“The specified subnet does not have enough free IP addresses to satisfy the request. Please provide a connection with a subnet which is available.”

Job: ebcrp-dfs11t-latest-load-onelake-job-vo1x1t02
Run ID: jr_cff83331ce14352db4d8d1551e461da112ab9445e191e520acd22361bff47464
Worker type/count: G.4X / 138
Triggered via Step Function.

Looks like subnet exhaustion in the Glue connection for our tenant.
Could you please check IP availability or add another subnet to the connection?
