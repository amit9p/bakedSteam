
# tests/edq/test_runEDQ_s3.py
import io
import yaml
import sys
import pytest
from unittest.mock import patch, MagicMock, mock_open

# 1) stub out edq_lib (and boto3 / oneLake_mini) so imports never fail
sys.modules['edq_lib']         = MagicMock()
sys.modules['boto3']           = MagicMock()
sys.modules['oneLake_mini']    = MagicMock()

# now import the function under test
from ecbr_card_self_service.edq.local_run.runEDQ import main

@pytest.fixture(autouse=True)
def all_patches():
    # A) A mock_open whose side_effect returns config.yml then secrets.yml
    cfg = {
        "DATA_SOURCE":  "S3",
        "S3_DATA_PATH": "s3://bucket/base_segment.csv",
        "JOB_ID":       "JID_S3",
        "AWS_PROFILE":  "my-profile"
    }
    sec = {"CLIENT_ID": "CID_S3", "CLIENT_SECRET": "CSEC_S3"}

    yaml_cfg = yaml.dump(cfg)
    yaml_sec = yaml.dump(sec)

    m = mock_open(read_data=yaml_cfg)
    # first call → config, second → secrets
    m.side_effect = [io.StringIO(yaml_cfg), io.StringIO(yaml_sec)]

    patches = [
      patch("ecbr_card_self_service.edq.local_run.runEDQ.open",         m),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session"), 
      patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession"),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules"),
      patch("ecbr_card_self_service.edq.local_run.runEDQ.logger"),
    ]
    mocks = [p.start() for p in patches]
    yield mocks
    for p in patches:
        p.stop()

def test_main_s3_branch(all_patches):
    mock_open_file, mock_boto_sess, mock_spark_cls, mock_exec, mock_log = all_patches

    # 2) boto3.Session().get_credentials().get_frozen_credentials()
    fake_sess  = MagicMock()
    fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
    fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
    mock_boto_sess.return_value = fake_sess

    # 3) SparkSession.builder.appName(...).getOrCreate() → fake_spark
    fake_spark   = MagicMock(name="spark")
    fake_builder = MagicMock()
    fake_builder.appName.return_value.getOrCreate.return_value = fake_spark
    mock_spark_cls.builder = fake_builder

    # 4) spark.read.format("csv")...load(...) → fake_spark
    fake_reader = MagicMock(name="reader_s3")
    fake_spark.read.format.return_value  = fake_reader
    fake_reader.option.return_value.load.return_value = fake_spark

    # 5) engine.execute_rules(...) → stubbed result
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

    # --- Assertions for the S3 branch ---
    mock_log.info.assert_any_call("Loading data from S3 URI")

    # CSV read
    fake_spark.read.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://bucket/base_segment.csv")

    # engine call
    mock_exec.assert_called_once_with(
      fake_spark,
      "JID_S3",      # job_id
      "CID_S3",      # client_id
      "CSEC_S3",     # client_secret
      "S3"           # data_source
    )

    # finally, we showed 0 rows with truncate=False
    fake_rows.show.assert_called_once_with(0, truncate=False)
