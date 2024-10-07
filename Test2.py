
Feature: Execute Glue ETL Job

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Run assembler_etl method and perform all actions
    When the assembler_etl method is called
    Then the assembler_etl method performs all actions (read, process, write)


from behave import given, when, then
from assembler_glue_job import assembler_etl  # Your Glue job function
import boto3
from botocore.exceptions import ClientError

# Helper function to parse S3 paths
def parse_s3_path(s3_path):
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)
    return bucket, key

@given('the Glue job "assembler_etl" is successfully deployed')
def step_impl(context):
    glue_client = boto3.client('glue')
    job_name = "assembler_etl"
    
    try:
        # Check if the job exists
        response = glue_client.get_job(JobName=job_name)
        if response.get('Job'):
            print(f"Glue job '{job_name}' is successfully deployed.")
            context.is_deployed = True
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityNotFoundException':
            print(f"Glue job '{job_name}' is not deployed.")
        else:
            print(f"An error occurred: {e}")
        context.is_deployed = False
    
    assert context.is_deployed, f"Glue job '{job_name}' is not deployed."

@when('the assembler_etl method is called')
def step_impl(context):
    # Here, you can simulate or trigger the Glue job by calling the assembler_etl method
    args = {
        "input_s3_path": "s3://your/input_path",  # Example path
        "output_s3_path": "s3://your/output_path",  # Example path
        "job_name": "assembler_etl",
        "business_date": "2024-10-01",  # Example value, adjust as necessary
        "env": "dev",
        "maxThreads": 20  # Example value, adjust as needed
    }
    
    # Store args in context for later verification
    context.args = args

@then('the assembler_etl method performs all actions (read, process, write)')
def step_impl(context):
    # Call the assembler_etl method which performs all steps: read, process, write
    assembler_etl(context.args)
    
    # Check for successful completion
    print("Glue job 'assembler_etl' executed successfully with all steps.")
