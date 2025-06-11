
Slide 1: What is EDQ?

“EDQ stands for Enterprise Data Quality. It is Capital One’s modern data quality platform, built to replace earlier tools like DataWise and Redrock. EDQ integrates with Exchange for seamless dataset registration and supports both fully managed and self-managed execution models. It allows users to create jobs, define validation rules, set up notifications, and review results, helping ensure data accuracy, completeness, and timely alerting.”


---

Slide 2: Job/Rules Creation – Approach 1: Exchange UI

“EDQ jobs can be created through the Exchange UI for any dataset already registered. Using the interface, users can define jobs and then attach data quality rules directly. It’s a user-friendly approach for those who prefer visual interaction to set up and manage their DQ processes.”


---

Slide 3: Job/Rules Creation – Approach 2: Using API

“For advanced users or automation needs, EDQ also supports job creation via API. This enables programmatic control over job setup, rules, schedules, and dataset configurations, which is especially useful for integrating with CI/CD pipelines or automated data workflows.”


---

Slide 4: Leverage Script – Creating Rules

“This slide shows how we can automate the full EDQ setup. Starting from job creation, we proceed to define individual rules and eventually automate the process using scripts and APIs. This approach reduces manual effort and ensures repeatability across environments or datasets.”





Slide 5: EDQ Rule Sample

"This slide shows a sample EDQ rule defined in JSON format. The rule type used here is REGEX, which checks if the field contains only alphabetic characters. The passingRange section defines the thresholds that determine whether data is valid, using percentage bounds.

EDQ supports a wide range of rule types—like Not Null, Valid Values, Integer Range, and Regex, which are executed row by row. It also supports aggregate-level checks such as Mean, Minimum, and Maximum, and dataset-wide checks like Schema Validation or Column Match.

The JSON rule shown here is one example of how EDQ enforces data quality programmatically."


    
