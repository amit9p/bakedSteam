"Access is required to read Omega input datasets used by the DFSL1 Card Data Ingest pipeline. This access is needed to validate source data, perform development and testing activities, and ensure accurate processing of reportable, consolidator, and calculator account datasets for the DFSL1 workflow."

Hi
Our file puller for the joiner_output dataset failed because the output now includes the customer_id field, but the current Exchange registration doesn’t list it.

We already have enterprise_servicing_customer_id in Exchange, but for this dataset, we need to add customer_id as well to align with the joiner output schema.

Can you please confirm if it’s okay to update the registration to include this column? Once confirmed, I’ll make the change and rerun the test to validate.

Thanks!
