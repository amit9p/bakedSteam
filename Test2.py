glue_lambda.feature

Feature: Trigger Lambda Function that invokes a Glue job

  Scenario: Successfully trigger Glue job on S3 event
    Given I have a Lambda function that triggers a Glue job upon S3 event
    And the Glue job and S3 event are mocked
    When I invoke the Lambda function with the mocked S3 event
    Then the Glue job should be invoked with the correct parameters
    And the Lambda function should return a success response


step.py


import boto3
import os
from unittest.mock import patch, MagicMock
from behave import given, when, then

@given("I have a Lambda function that triggers a Glue job upon S3 event")
def step_impl(context):
    # Set up the environment variable for the Glue job
    os.environ["GLUE_JOB_NAME"] = "test-glue-job"
    context.lambda_function = __import__("lambda_function.trigger_lambda")

@given("the Glue job and S3 event are mocked")
def step_impl(context):
    # Mock the Glue client
    mock_glue_client = MagicMock()
    mock_glue_client.start_job_run.return_value = {"JobRunId": "mock-job-run-id"}

    # Patch boto3 client for Glue
    context.glue_client_patcher = patch("boto3.client", return_value=mock_glue_client)
    context.mock_glue_client = context.glue_client_patcher.start()

    # Mock S3 event (simulate _SUCCESS file drop)
    context.mock_s3_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "path/to/_SUCCESS"}
                }
            }
        ]
    }

@when("I invoke the Lambda function with the mocked S3 event")
def step_impl(context):
    # Invoke the Lambda function with the mocked S3 event
    context.response = context.lambda_function.lambda_handler(context.mock_s3_event, None)

@then("the Glue job should be invoked with the correct parameters")
def step_impl(context):
    # Assert that the Glue client's start_job_run was called with the correct arguments
    context.mock_glue_client.start_job_run.assert_called_once_with(
        JobName="test-glue-job",
        Arguments={}
    )

@then("the Lambda function should return a success response")
def step_impl(context):
    # Assert that the Lambda function returns the expected response
    assert context.response["statusCode"] == 200
    assert context.response["jobRunId"] == "mock-job-run-id"
