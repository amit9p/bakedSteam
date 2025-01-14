
import boto3

def execute_glue_job(job_name, aws_access_key, aws_secret_key, session_token, region, arguments={}):
    try:
        # Initialize Boto3 Glue client with explicit credentials and session token
        glue_client = boto3.client(
            'glue',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=session_token,
            region_name=region
        )

        # Start the Glue job
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )
        print(f"Started Glue job. Job Run ID: {response['JobRunId']}")
        return response['JobRunId']

    except Exception as e:
        print(f"Error executing Glue job: {e}")

if __name__ == "__main__":
    # AWS temporary credentials
    aws_access_key = "YOUR_ACCESS_KEY"
    aws_secret_key = "YOUR_SECRET_KEY"
    session_token = "YOUR_SESSION_TOKEN"  # Temporary session token
    region = "us-east-1"  # Replace with your AWS region

    # Glue job parameters
    job_name = "my-glue-job"  # Replace with your Glue job name
    arguments = {
        '--arg1': 'value1',
        '--arg2': 'value2'
    }

    # Execute the Glue job
    job_run_id = execute_glue_job(job_name, aws_access_key, aws_secret_key, session_token, region, arguments)
    print(f"Glue job executed successfully with Job Run ID: {job_run_id}")
