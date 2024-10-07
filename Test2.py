
Feature: Happy path for Glue job ETL process

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Successful data ingestion pipeline to S3
    Given I have an input file in S3 at "<input_s3_path>"
    When the Glue job "assembler_etl" runs
    Then the job should read the input data from "<input_s3_path>"
    And call the "get_trade_lines" function to get a dictionary of Metro2 string
    And write the Metro2 string to S3
    Then the status should be successful in CloudWatch logs and console



from behave import given, when, then
import boto3
import logging

# Assume necessary imports and setup here

@given('the Glue job "assembler_etl" is successfully deployed')
def step_impl(context):
    # Logic to ensure the job is deployed
    pass

@given('I have an input file in S3 at "{input_s3_path}"')
def step_impl(context, input_s3_path):
    # Check if file exists in S3
    s3 = boto3.client('s3')
    bucket, key = parse_s3_path(input_s3_path)
    response = s3.head_object(Bucket=bucket, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

@when('the Glue job "assembler_etl" runs')
def step_impl(context):
    # Logic to trigger the Glue job
    glue = boto3.client('glue')
    response = glue.start_job_run(JobName="assembler_etl")
    context.job_run_id = response['JobRunId']

@then('the job should read the input data from "{input_s3_path}"')
def step_impl(context, input_s3_path):
    # Verify that the input data was processed
    pass

@then('call the "get_trade_lines" function to get a dictionary of Metro2 string')
def step_impl(context):
    # Logic to verify function call
    pass

@then('write the Metro2 string to S3')
def step_impl(context):



############
import boto3
from botocore.exceptions import ClientError

@given('the Glue job "assembler_etl" is successfully deployed')
def step_impl(context):
    glue_client = boto3.client('glue')
    job_name = "assembler_etl"
    
    try:
        # Retrieve the Glue job to check if it exists
        response = glue_client.get_job(JobName=job_name)
        if response.get('Job'):
            print(f"Glue job '{job_name}' is successfully deployed.")
            context.is_deployed = True
    except ClientError as e:
        # Handle case where the job is not found or any other issues
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Glue job '{job_name}' is not deployed.")
        else:
            print(f"An error occurred: {e}")
        context.is_deployed = False

    # Ensure the job is actually deployed for the test to proceed
    assert context.is_deployed is True, f"Glue job '{job_name}' is not deployed."
  
    # Verify S3 write
    pass

@then('the status should be successful in CloudWatch logs and console')
def step_impl(context):
    # Check CloudWatch logs and console output for success
    pass

def parse_s3_path(s3_path):
    # Helper function to parse the S3 path into bucket and key
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    return bucket, key



import boto3
from botocore.exceptions import ClientError

@given('the Glue job "assembler_etl" is successfully deployed')
def step_impl(context):
    glue_client = boto3.client('glue')
    job_name = "assembler_etl"
    
    try:
        # Retrieve the Glue job to check if it exists
        response = glue_client.get_job(JobName=job_name)
        if response.get('Job'):
            print(f"Glue job '{job_name}' is successfully deployed.")
            context.is_deployed = True
    except ClientError as e:
        # Handle case where the job is not found or any other issues
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Glue job '{job_name}' is not deployed.")
        else:
            print(f"An error occurred: {e}")
        context.is_deployed = False

    # Ensure the job is actually deployed for the test to proceed
    assert context.is_deployed is True, f"Glue job '{job_name}' is not deployed."

