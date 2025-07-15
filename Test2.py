
# tests/edq/test_s3.py
import sys
import importlib
import yaml
import pytest
from unittest.mock import patch, MagicMock

# 1) Prevent import‐time failures for edq_lib, boto3, oneLake_mini
sys.modules['edq_lib']     = MagicMock()
sys.modules['boto3']        = MagicMock()
sys.modules['oneLake_mini'] = MagicMock()

@patch('builtins.open', create=True)
@patch('boto3.Session')
@patch('ecbr_card_self_service.edq.local_run.runEDQ.SparkSession')
@patch('ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules')
@patch('ecbr_card_self_service.edq.local_run.runEDQ.logger')
def test_main_s3_branch(
    mock_logger,
    mock_execute_rules,
    mock_spark_cls,
    mock_boto_session,
    mock_open_fn
):
    # A) Fake config.yaml & secrets.yaml
    config = {
        'DATA_SOURCE':  'S3',
        'S3_DATA_PATH': 's3://my-bucket/my-file.csv',
        'JOB_ID':       'JOB-123',
        'AWS_PROFILE':  'my-aws-profile',
    }
    secrets = {
        'CLIENT_ID':     'CID-123',
        'CLIENT_SECRET': 'CSEC-123',
    }

    # set up open() to first return config, then secrets
    cfg_handle = MagicMock()
    cfg_handle.read.return_value = yaml.dump(config)
    sec_handle = MagicMock()
    sec_handle.read.return_value = yaml.dump(secrets)
    mock_open_fn.side_effect = [cfg_handle, sec_handle]

    # B) Stub boto3.Session().get_credentials().get_frozen_credentials()
    fake_creds = MagicMock(access_key='AK', secret_key='SK', token=None)
    fake_sess  = MagicMock()
    fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
    mock_boto = mock_boto_session.return_value
    mock_boto_session.return_value = fake_sess

    # C) Stub SparkSession.builder.appName(...).config(...).config(...).getOrCreate()
    fake_spark   = MagicMock(name='spark')
    fake_builder = MagicMock(name='builder')
    # chain every method back to the same builder
    fake_builder.appName.return_value    = fake_builder
    fake_builder.config.return_value     = fake_builder
    fake_builder.getOrCreate.return_value = fake_spark
    # patch the class so its .builder property is our fake
    mock_spark_cls.builder = fake_builder

    # D) Stub spark.read.format(...).option(...).load(...)
    fake_reader = MagicMock(name='reader')
    fake_reader.format.return_value = fake_reader
    fake_reader.option.return_value = fake_reader
    fake_reader.load.return_value   = fake_spark  # pretend the DF is just the same mock
    fake_spark.read = fake_reader

    # E) Stub engine.execute_rules(...)
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

    # F) Now import & reload the module under test
    import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ
    importlib.reload(runEDQ)

    # G) Call main()
    runEDQ.main()

    # H) Verify we took the S3 branch
    mock_logger.info.assert_any_call("Loading data from S3 URI")
    # The CSV reader should only be called once, with exactly your S3 path:
    fake_reader.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://my-bucket/my-file.csv")

    # And execute_rules must be invoked with the DF and your credentials/job
    mock_execute_rules.assert_called_once_with(
        fake_spark,        # DataFrame
        "JOB-123",         # job_id
        "CID-123",         # client_id
        "CSEC-123",        # client_secret
        "S3"               # data_source
    )

    # Finally the show on your fake row‐level results
    fake_rows.show.assert_called_once_with(0, truncate=False)
