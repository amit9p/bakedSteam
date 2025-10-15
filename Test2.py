


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
