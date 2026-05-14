
Story 1
Title
Finalize SBFE/Recoveries EDQ Job Configuration
Type
Story
Story Points
2
Business Purpose
Problem Statement
Finalize EDQ job setup for the Recoveries and SBFE datasets so EDQ validations can be executed as part of the QA/CTE readiness flow. This includes EDQ job creation, rule setup, config updates, and PR submission.
Fields/Areas to Cover
Create EDQ job for Recoveries dataset
Create EDQ job for SBFE dataset
Add required EDQ validation rules
Update EDQ config YAML file
Validate configuration end to end
Raise PR for code/config changes
Expected Outcome
EDQ job configuration is ready for Recoveries and SBFE
Required rules are added
YAML config is updated correctly
PR is raised and ready for review
User Impact
Ensures Recoveries and SBFE datasets have EDQ validation in place and are ready for controlled execution in QA/CTE.
Technical Definition
Overview
Create and configure EDQ jobs for Recoveries and SBFE datasets, add validation rules, update YAML configuration, and submit a PR.
Affected Components
EDQ job configuration
EDQ rules
Config YAML
PR / repository changes
Technical Constraints
Rules and config should align with current dataset structure
Config should be reusable for upcoming runs
PR must be ready before planned execution window
Acceptance Criteria
AC1: EDQ Jobs Created
Given Recoveries and SBFE datasets need EDQ validation
When EDQ setup work is completed
Then EDQ jobs exist for both datasets
AC2: Rules Added
Given the EDQ jobs are created
When validation logic is configured
Then required EDQ rules are added for both datasets
AC3: YAML Updated
Given EDQ jobs and rules are identified
When configuration changes are made
Then EDQ config YAML is updated correctly
AC4: PR Raised
Given all config and rule updates are completed
When changes are committed
Then a PR is raised for review
Definition of Done
EDQ jobs created for Recoveries and SBFE
Rules added and reviewed
YAML updated
Basic validation completed
PR raised
Any follow-up gaps captured separately
Story 2
Title
Prepare API Payload and Curl Command for CTE Job Trigger
Type
Story
Story Points
1
Business Purpose
Problem Statement
Prepare the API request payload and curl command required to trigger the job in the CTE environment for the planned runs on 5/18 and 5/19. The goal is to have a ready-to-use command so execution can be started quickly without last-minute setup.
Fields/Areas to Cover
Identify required API inputs for CTE run
Create request payload
Prepare curl command
Validate payload structure
Keep command ready for 5/18 and 5/19 execution
Expected Outcome
Payload is prepared and verified
Curl command is ready to use
Team can trigger the CTE job directly during the run window
User Impact
Reduces execution-day delays and ensures CTE job kickoff can happen smoothly using a prevalidated payload and command.
Technical Definition
Overview
Create the API payload and curl command required to trigger the CTE job. Ensure the payload contains the required run inputs and is ready for immediate use during scheduled execution.
Affected Components
API trigger payload
Curl command
CTE job kickoff process
Technical Constraints
Payload must match API contract
Required runtime inputs must be included
Command should be easy to reuse for multiple run dates
Acceptance Criteria
AC1: Payload Prepared
Given the CTE job requires API input
When setup is completed
Then the request payload is created with all required parameters
AC2: Curl Command Ready
Given the payload is available
When the API trigger command is prepared
Then a reusable curl command is documented and ready
AC3: Execution Readiness Confirmed
Given the payload and curl are prepared
When reviewed before the run
Then the team can use them directly for 5/18 and 5/19 CTE execution
Definition of Done
Payload created
Curl command prepared
Inputs reviewed
Command stored/shared in ready-to-use format
Any missing dependency noted
Suggested short summary for JIRA description header
Story 1
Finalize EDQ setup for Recoveries and SBFE by creating EDQ jobs, adding rules, updating YAML config, and raising a PR.
Story 2
Prepare API payload and curl command required to trigger the CTE job for 5/18 and 5/19 runs.
If you want, I can also make these in a more polished Capital One/JIRA-ready wording with exact sections matching your template line by line.
