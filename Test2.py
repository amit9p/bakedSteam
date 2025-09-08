
Summary

This PR introduces initial schema definitions for Credit Bureau reporting as part of the DFS → Omega migration work.

Added credit_bureau_account.py schema (with typed columns for account, status, delinquency dates, etc.).

Setup project structure for future schema modules (customer, report_snapshot, event_log, etc.).

Added Pipfile and .gitignore to manage dependencies and keep repo clean.


This lays the groundwork for eCBR self-service so downstream jobs can consume structured schemas.


---

Resolves / Relates To

Relates to DFS migration tasks in CBR modernization.

Sets foundation for later tasks like data ingestion, validation, and Metro2 reporting.



---

Review Focus

Check schema field names and data types for correctness against expected CBR requirements.

Ensure Pipfile dependencies (pyspark, typedspark) are correctly placed in [packages].

Confirm project layout makes sense for additional schema files.



---

Help Wanted

Feedback on whether we should standardize naming convention for schema classes (e.g., Credit_Bureau_Account vs. CreditBureauAccount).

Any missing fields that should be part of the base schema?



---

Implementation Strategy

Used typed-spark to enforce schema at compile-time.

Organized schemas in a dedicated schemas/ folder for clarity.

Dependency management via Pipenv (Pipfile committed for reproducibility).



---

Readiness Checklist

[x] Added schema file with fields needed for initial DFS migration reporting.

[x] Added Pipfile and .gitignore.

[ ] No tests yet – test cases will be added once ingestion/transformation logic is implemented.


