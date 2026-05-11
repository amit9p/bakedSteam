Yes, these 3 fields are expected to be INT in the SBFE QA dataset:

- "ab_delinquency_status"
- "ab_payment_structure"
- "ab_length_of_payment_history"

We also ran the test file puller against this dataset, and the data got uploaded successfully. The generator/file output should still format these numeric fields according to the SBFE fixed-width rules.
