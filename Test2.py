

# tests/edq/test_s3.py
import sys
import importlib
import unittest
import yaml
from unittest.mock import patch, MagicMock

# 1) Prevent import‐time failures for edq_lib, boto3, oneLake_mini
sys.modules['edq_lib']      = MagicMock()
sys.modules['boto3']         = MagicMock()
sys.modules['oneLake_mini']  = MagicMock()

class TestRunEDQS3Branch(unittest.TestCase):
    def stub_yaml_reads(self, mock_open_fn, config_dict, secret_dict):
        """
        Configure builtins.open so that:
         - the first call to open(...).read() returns yaml.dump(config_dict)
         - the second call to open(...).read() returns yaml.dump(secret_dict)
        """
        # handle for config.yaml
        cfg_handle = MagicMock()
        cfg_handle.read.return_value = yaml.dump(config_dict)
        # handle for secrets.yaml
        sec_handle = MagicMock()
        sec_handle.read.return_value = yaml.dump(secret_dict)
        # side_effect = these two handles, in sequence
        mock_open_fn.side_effect = [cfg_handle, sec_handle]

    @patch('builtins.open', create=True)
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
        mock_open_fn
    ):
        # A) Fake config.yaml & secrets.yaml
        config = {
            'DATA_SOURCE':  'S3',
            'S3_DATA_PATH': 's3://the-bucket/the_file.csv',
            'JOB_ID':       'JOB-XYZ',
            'AWS_PROFILE':  'my-aws-profile',
        }
        secrets = {
            'CLIENT_ID':     'CID-XYZ',
            'CLIENT_SECRET': 'CSEC-XYZ',
        }
        self.stub_yaml_reads(mock_open_fn, config, secrets)

        # B) Stub boto3.Session().get_credentials().get_frozen_credentials()
        fake_creds = MagicMock(access_key='AK', secret_key='SK', token=None)
        fake_sess  = MagicMock()
        fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
        mock_boto_session.return_value = fake_sess

        # C) Stub SparkSession.builder.appName(...).getOrCreate()
        fake_spark   = MagicMock(name='spark')
        fake_builder = MagicMock(name='builder')
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) Stub spark.read.format(...).option(...).load(...)
        fake_reader = MagicMock(name='reader')
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        fake_df = MagicMock(name='df')
        fake_reader.load.return_value = fake_df
        fake_spark.read = fake_reader

        # E) Stub the EDQ engine call
        fake_rows = MagicMock(name='rows')
        fake_rows.show.return_value = None
        mock_execute_rules.return_value = {
            'result_type': 'ExecutionCompleted',
            'job_execution_result': {
                'results':           [],
                'total_DF_rows':     0,
                'row_level_results': fake_rows
            }
        }

        # F) Now import & reload your module under test
        import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ
        importlib.reload(runEDQ)

        # G) Call main()
        runEDQ.main()

        # H) Assertions: we must have taken the S3 branch
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header", "true")
        fake_reader.load.assert_called_once_with("s3://the-bucket/the_file.csv")
        mock_execute_rules.assert_called_once_with(
            fake_df,           # the DataFrame
            "JOB-XYZ",         # job_id
            "CID-XYZ",         # client_id
            "CSEC-XYZ",        # client_secret
            "S3"               # data_source
        )
        fake_rows.show.assert_called_once_with(0, truncate=False)


if __name__ == "__main__":
    unittest.main()
