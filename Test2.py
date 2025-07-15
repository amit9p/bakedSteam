
import sys
import importlib
import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# 1) Prevent real imports of edq_lib, boto3 and oneLake_mini
sys.modules['edq_lib'] = MagicMock()
sys.modules['boto3']    = MagicMock()
sys.modules['oneLake_mini'] = MagicMock()

class TestRunEDQS3Branch(unittest.TestCase):
    def stub_yaml_reads(self, mock_open_file, config_dict, secret_dict):
        """
        Make builtins.open() first return the config YAML, then the secrets YAML.
        """
        handle = mock_open_file()
        handle.read.side_effect = [
            yaml.dump(config_dict),
            yaml.dump(secret_dict),
        ]
        return handle

    @patch('builtins.open', new_callable=mock_open)
    @patch('boto3.Session')
    @patch('ecbr_card_self_service.edq.local_run.runEDQ.SparkSession')
    @patch('ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules')
    @patch('ecbr_card_self_service.edq.local_run.runEDQ.logger')
    def test_main_s3_branch(
        self,
        mock_logger,
        mock_execute_rules,
        mock_spark_cls,
        mock_boto_session,
        mock_open_file
    ):
        # A) Stub out config.yaml & secrets.yaml
        config = {
            'DATA_SOURCE':  'S3',
            'S3_DATA_PATH':'s3://bucket/base_segment.csv',
            'JOB_ID':       'JID_S3',
            'AWS_PROFILE':  'my-profile',
        }
        secrets = {
            'CLIENT_ID':     'CID_S3',
            'CLIENT_SECRET': 'CSEC_S3',
        }
        self.stub_yaml_reads(mock_open_file, config, secrets)

        # B) Stub boto3.Session().get_credentials().get_frozen_credentials()
        fake_creds = MagicMock(access_key='AK', secret_key='SK', token=None)
        fake_sess  = MagicMock()
        fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
        mock_boto_session.return_value = fake_sess

        # C) Stub SparkSession.builder.appName(...).getOrCreate() → fake_spark
        fake_spark   = MagicMock(name='spark_s3')
        fake_builder = MagicMock(name='builder_s3')
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) Stub out spark.read.format(...).option(...).load(...) 
        fake_reader = MagicMock(name='reader_s3')
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        fake_df = MagicMock(name='df_s3')
        fake_reader.load.return_value = fake_df
        # **Attach your reader stub to the spark stub**
        fake_spark.read = fake_reader

        # E) Stub the EDQ engine call
        fake_rows = MagicMock(name='rows_s3')
        fake_rows.show.return_value = None
        mock_execute_rules.return_value = {
            'result_type': 'ExecutionCompleted',
            'job_execution_result': {
                'results':           [],
                'total_DF_rows':     0,
                'row_level_results': fake_rows
            }
        }

        # 2) Reload the module (so our open() patch is picked up inside main())
        import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ
        importlib.reload(runEDQ)

        # 3) Execute
        runEDQ.main()

        # 4) Verify we went down the S3 path
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header", "true")
        fake_reader.load.assert_called_once_with("s3://bucket/base_segment.csv")
        mock_execute_rules.assert_called_once_with(
            fake_df, "JID_S3", "CID_S3", "CSEC_S3", "S3"
        )
        fake_rows.show.assert_called_once_with(0, truncate=False)


if __name__ == "__main__":
    unittest.main()
