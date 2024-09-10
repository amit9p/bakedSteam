

import unittest
from unittest.mock import patch, MagicMock

class TestBatchProcess(unittest.TestCase):
    @patch('glue_jobs.assembler_glue_job.setup_turing_config')  # Mock setup_turing_config
    @patch('cl_turing_sdk.pyspark.turing_pyspark_client.TuringPySparkClient')  # Mock TuringPySparkClient
    @patch('requests.post')  # Mock the OAuth2 request
    def test_batch_process(self, mock_requests_post, mock_turing_client, mock_setup_turing_config):
        # Mock setup_turing_config return values
        mock_setup_turing_config.return_value = {
            'TURING_API_OAUTH_URL': 'https://api-pre.cede.cloud.capitalone.com',
            'TURING_OAUTH_CLIENT_ID': 'test_id',
            'TURING_OAUTH_CLIENT_SECRET': 'test_secret',
            'TURING_CLIENT_SSL_VERIFY': False,
            'turing3.api.npi.url': 'https://api-turing-precede.cloud.capitalone.com/npi',
            'TURING_API_NPI_SCOPE': 'tokenize:ustaxid',
            'turing3.api.pci.url': 'https://api-turing-precede.cloud.capitalone.com/pci',
            'TURING_API_PCI_SCOPE': 'tokenize:pan'
        }

        # Mock the response from the OAuth2 token request
        mock_response = MagicMock()
        mock_response.status_code = 200  # Simulate a successful response
        mock_response.json.return_value = {
            'access_token': 'mocked_access_token'
        }
        mock_requests_post.return_value = mock_response

        # Mock the TuringPySparkClient and its process method
        mock_client_instance = mock_turing_client.return_value
        mock_client_instance.process.return_value = "mock_df"

        # Simulate a Spark DataFrame
        spark = SparkSession.builder.master("local").appName("test").getOrCreate()
        df = spark.createDataFrame([("1", "test"), ("id", "value")], ["id", "value"])

        # Call the batch_process function
        result = batch_process(
            df, 
            dev_creds={'client_id': 'test_id', 'client_secret': 'test_secret'}, 
            env="qa", 
            tokenization="PAN"
        )

        # Assertions to verify the behavior
        self.assertEqual(result, "mock_df")
        mock_setup_turing_config.assert_called_once_with(
            {'client_id': 'test_id', 'client_secret': 'test_secret'}, 'qa', 'PAN'
        )

        # Ensure OAuth2 token request was made (mocked)
        mock_requests_post.assert_called_once_with(
            'https://api-pre.cede.cloud.capitalone.com/oauth2/token',  # The token URL
            data={'grant_type': 'client_credentials'},  # OAuth2 payload
            auth=('test_id', 'test_secret'),  # Client ID and Secret
            verify=False
        )

        # Ensure the Turing client process was called
        mock_client_instance.process.assert_called_once_with(df, mock.ANY)
