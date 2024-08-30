
import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from aws.glue.context import GlueContext

import assembler_glue_job as job

class TestAssemblerGlueJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize the SparkSession and GlueContext for testing
        cls.spark = SparkSession.builder.master("local").appName("GlueJobTest").getOrCreate()
        cls.sc = SparkContext.getOrCreate()
        cls.glue_context = GlueContext(cls.sc)

    @patch('assembler_glue_job.GlueContext')
    @patch('assembler_glue_job.getResolvedOptions')
    @patch('assembler_glue_job.SparkSession')
    @patch('assembler_glue_job.SparkContext')
    def test_main(self, mock_spark_context, mock_spark_session, mock_get_resolved_options, mock_glue_context):
        # Mock the SparkContext and GlueContext
        mock_spark_context.getOrCreate.return_value = self.sc
        mock_glue_context.return_value = self.glue_context
        mock_spark_session.builder.getOrCreate.return_value = self.spark

        # Mock Java components to avoid 'JavaPackage' object issues
        mock_jvm = MagicMock()
        self.sc._jvm = mock_jvm
        mock_jsc = MagicMock()
        self.glue_context._jsc = mock_jsc  # Mocking the Java SparkContext

        # Directly mock the call to the Java GlueContext or JavaSparkContext
        mock_jvm.JavaSparkContext = MagicMock()
        mock_jsc.GlueContext = MagicMock()

        # Set the mock return values for resolved options
        mock_get_resolved_options.return_value = {
            'input_s3_path': 's3://mock-input-bucket/mock-input-path',
            'output_s3_path': 's3://mock-output-bucket/mock-output-path',
            'env': 'qa',
            'client_id': 'mock-client-id',
            'client_secret': 'mock-client-secret'
        }

        # Mock DataFrame and its methods
        df_mock = MagicMock()
        self.glue_context.read.parquet.return_value = df_mock
        df_mock.withColumn.return_value = df_mock
        df_mock.select.return_value = df_mock
        df_mock.write.mode.return_value = df_mock.write
        df_mock.write.parquet.return_value = None

        # Run the job's main function
        job.main()

        # Assert that the parquet write method was called correctly
        df_mock.write.parquet.assert_called_once_with('s3://mock-output-bucket/mock-output-path')

if __name__ == '__main__':
    unittest.main()
