
Feature: Critical case for Glue job ETL process

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Critical data ingestion pipeline failure
    Given I have an input file in S3 at "<input_s3_path>"
    When the Glue job "assembler_etl" runs
    Then the job should read the input data from "<input_s3_path>"
    And if the data cannot be read due to missing or corrupted data, raise a critical alert
    And if the "detokenize" or "get_trade_lines" functions fail, raise a critical alert
    And if writing to S3 fails due to permissions or connection issues, raise a critical alert
    And I should see the error details logged in CloudWatch
    And the Jira X-Ray ticket should be created with a "Critical" severity level
