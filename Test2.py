
# tests/edq/test_runEDQ_s3.py
import io
import yaml
import sys
import pytest
from unittest.mock import patch, MagicMock, mock_open

# 1) Stub out edq_lib / boto3 / oneLake so imports will never fail:
sys.modules['edq_lib']      = MagicMock()
sys.modules['boto3']        = MagicMock()
sys.modules['oneLake_mini'] = MagicMock()

# 2) Import the code under test
from ecbr_card_self_service.edq.local_run.runEDQ import main

@pytest.fixture(autouse=True)
def patch_everything():
    # Prepare two YAML docs: config.yml then secrets.yml
    cfg = {
        "DATA_SOURCE":  "S3",
        "S3_DATA_PATH": "s3://bucket/base_segment.csv",
        "JOB_ID":       "JID_S3",
        "AWS_PROFILE":  "my-profile"
    }
    sec = {
        "CLIENT_ID":     "CID_S3",
        "CLIENT_SECRET": "CSEC_S3"
    }

    yaml_cfg = yaml.dump(cfg)
    yaml_sec = yaml.dump(sec)

    # Patch built‐in open so every open(...) yields our two StringIOs
    m = mock_open(read_data=yaml_cfg)
    m.side_effect = [io.StringIO(yaml_cfg), io.StringIO(yaml_sec)]

    patches = [
      patch("builtins.open", m),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession"),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session"),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules"),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.logger"),
    ]
    started = [p.start() for p in patches]
    yield started
    for p in patches:
        p.stop()

def test_main_s3_branch(patch_everything):
    _, mock_spark_cls, mock_boto_sess, mock_exec, mock_log = patch_everything

    # Boto3.credentials stub
    fake_sess  = MagicMock()
    fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
    fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
    mock_boto_sess.return_value = fake_sess

    # SparkSession.builder.appName(...).getOrCreate() → fake_spark
    fake_spark   = MagicMock(name="spark")
    fake_builder = MagicMock()
    fake_builder.appName.return_value.getOrCreate.return_value = fake_spark
    mock_spark_cls.builder = fake_builder

    # spark.read.format("csv")...load(...) → fake_spark
    fake_reader = MagicMock(name="reader_s3")
    fake_spark.read.format.return_value  = fake_reader
    fake_reader.option.return_value.load.return_value = fake_spark

    # engine.execute_rules stub
    fake_rows = MagicMock(name="rows_s3")
    fake_rows.show.return_value = None
    mock_exec.return_value = {
      "result_type": "ExecutionCompleted",
      "job_execution_result": {
        "results": [], 
        "total_DF_rows": 0, 
        "row_level_results": fake_rows
      }
    }

    # RUN!
    main()

    # — verify we took the S3 path —
    mock_log.info.assert_any_call("Loading data from S3 URI")

    # CSV read was called with exactly the S3 path
    fake_spark.read.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://bucket/base_segment.csv")

    # engine.execute_rules got the S3 parameters
    mock_exec.assert_called_once_with(
      fake_spark,
      "JID_S3",      # job id
      "CID_S3",      # client id
      "CSEC_S3",     # client secret
      "S3"           # data source
    )

    # And finally we printed zero rows
    fake_rows.show.assert_called_once_with(0, truncate=False)
