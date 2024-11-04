import boto3
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Specify the profile name and region
profile_name = 'GR_GG_COF_AWS_592052317603_Developer'
region_name = 'us-east-1'  # Ensure the region is explicitly set here

# Create a session with the specified profile
session = boto3.Session(profile_name=profile_name)

# Log the loaded credentials
credentials = session.get_credentials()
if credentials:
    logger.info(f"Credentials loaded successfully for profile '{profile_name}'")
    logger.info(f"AWS Access Key ID: {credentials.access_key[:4]}...")  # Masked for security
else:
    logger.error("Failed to load credentials")

# Create the Glue client with the specified region
glue_client = session.client('glue', region_name=region_name)

# Verify Glue client creation by making a simple call
try:
    response = glue_client.list_jobs(MaxResults=5)
    logger.info("Glue client created successfully, listing Glue jobs:")
    logger.info(response['JobNames'])
except Exception as e:
    logger.error(f"Failed to create or use Glue client: {e}")

########
import boto3
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Specify the profile name
profile_name = 'profile_name'

# Create a session with the specified profile
session = boto3.Session(profile_name=profile_name)

# Log the loaded credentials and region
credentials = session.get_credentials()
region = session.region_name or 'No region specified'

if credentials:
    logger.info(f"Credentials loaded successfully for profile '{profile_name}'")
    logger.info(f"AWS Access Key ID: {credentials.access_key[:4]}...")  # Partially mask for security
    logger.info(f"AWS Secret Access Key: {credentials.secret_key[:4]}...")  # Partially mask for security
else:
    logger.error("Failed to load credentials")

if region:
    logger.info(f"Region: {region}")
else:
    logger.warning("No region specified in the profile or session")

# Use the session to create a Glue client
glue_client = session.client('glue')

# Verify Glue client creation by making a simple call (e.g., list jobs)
try:
    response = glue_client.list_jobs(MaxResults=5)
    logger.info("Glue client created successfully, listing Glue jobs:")
    logger.info(response['JobNames'])
except Exception as e:
    logger.error(f"Failed to create or use Glue client: {e}")

##########@
import boto3
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define AWS credentials and region (if necessary)
aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
region_name = 'YOUR_AWS_REGION'

# Initialize the Glue client
glue_client = boto3.client(
    'glue',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def start_glue_job(glue_client, job_name, arguments={}):
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments
        )
        job_run_id = response['JobRunId']
        logger.info(f'Started Glue job: {job_name} with JobRunId: {job_run_id}')
        return job_run_id
    except Exception as e:
        logger.error(f'Failed to start Glue job: {e}')
        raise

def wait_for_job_completion(glue_client, job_name, job_run_id):
    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = response['JobRun']['JobRunState']
        
        logger.info(f'Job {job_name} (ID: {job_run_id}) current status: {status}')
        
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        
        time.sleep(30)  # Wait for 30 seconds before polling again
    
    if status == 'SUCCEEDED':
        logger.info(f'Glue job {job_name} completed successfully.')
    else:
        logger.error(f'Glue job {job_name} failed with status: {status}')
        raise Exception(f'Glue job failed with status: {status}')

if __name__ == "__main__":
    job_name = 'your_glue_job_name'
    arguments = {
        '--your_argument_key': 'your_argument_value'
    }
    
    # Start the job and get JobRunId
    job_run_id = start_glue_job(glue_client, job_name, arguments)
    
    # Wait for job completion
    wait_for_job_completion(glue_client, job_name, job_run_id)
