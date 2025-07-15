
import sys

# point `import edq_lib` at your real engine module
from ecbr_card_self_service.edq.local_run import engine as _real_engine
sys.modules['edq_lib'] = _real_engine




import io
import yaml
import pytest
from unittest.mock import patch, MagicMock, mock_open

# import your main() from the exact module
from ecbr_card_self_service.edq.local_run.runEDQ import main

@pytest.fixture(autouse=True)
def patch_all():
    # 1) Patch builtins.open *in the runEDQ module*, not globally
    m_open = mock_open()
    with patch("ecbr_card_self_service.edq.local_run.runEDQ.open", new_callable=lambda: m_open) as mock_file, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session")          as mock_boto_sess, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")          as mock_spark_cls, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules") as mock_exec_rules, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")               as mock_logger:
        
        # yield all the mocks so tests can configure them
        yield mock_file, mock_boto_sess, mock_spark_cls, mock_exec_rules, mock_logger

def test_main_s3_branch(patch_all):
    mock_file, mock_boto_sess, mock_spark_cls, mock_exec_rules, mock_logger = patch_all

    # A) Prepare config.yaml & secrets.yaml content
    cfg = {
      "DATA_SOURCE":   "S3",
      "S3_DATA_PATH":  "s3://mybucket/myfile.csv",
      "JOB_ID":        "JID_S3",
      "AWS_PROFILE":   "my-profile"
    }
    sec = {
      "CLIENT_ID":     "CID_S3",
      "CLIENT_SECRET": "CSEC_S3"
    }
    # first open() -> config.yaml, second open() -> secrets.yaml
    mock_file.side_effect = [
      io.StringIO(yaml.dump(cfg)),
      io.StringIO(yaml.dump(sec))
    ]

    # B) Stub boto3 credentials
    fake_sess = MagicMock()
    fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
    # boto3.Session().get_credentials().get_frozen_credentials() => fake_creds
    fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
    mock_boto_sess.return_value = fake_sess

    # C) Stub SparkSession builder -> read -> load
    fake_spark  = MagicMock(name="spark")
    fake_builder = MagicMock(name="builder")
    # SparkSession.builder.appName(...).getOrCreate() => fake_spark
    fake_builder.appName.return_value.getOrCreate.return_value = fake_spark
    mock_spark_cls.builder = fake_builder

    # Spark read chain
    fake_reader = MagicMock(name="reader") 
    fake_spark.read.format.return_value = fake_reader
    fake_reader.option.return_value.load.return_value = fake_spark  # pretend load() gives us a DF

    # D) Stub engine.execute_rules()
    fake_rows = MagicMock(name="row_level_results")
    fake_rows.show.return_value = None
    mock_exec_rules.return_value = {
      "result_type": "ExecutionCompleted",
      "job_execution_result": {
        "results": [],
        "total_DF_rows": 0,
        "row_level_results": fake_rows
      }
    }

    # E) Now call main()
    main()

    # ---- Assertions ----

    # 1) We should have read the config file and secrets file
    assert mock_file.call_count == 2

    # 2) We should log that we're loading from S3
    mock_logger.info.assert_any_call("Loading data from S3 URI")

    # 3) Our Spark read chain must have been invoked with the CSV format and the right S3 path
    fake_spark.read.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://mybucket/myfile.csv")

    # 4) engine.execute_rules gets exactly the signature for S3, including the uppercase "S3"
    mock_exec_rules.assert_called_once_with(
      fake_spark,            # the DF returned by load()
      "JID_S3",              # job_id
      "CID_S3",              # client_id
      "CSEC_S3",             # client_secret
      "S3"                   # data_source
    )

    # 5) And we printed the row-level results
    fake_rows.show.assert_called_once_with(0, truncate=False)



-----
import io
import yaml
import pytest
from unittest.mock import patch, MagicMock, mock_open

# import your main() from the exact module
from ecbr_card_self_service.edq.local_run.runEDQ import main

@pytest.fixture(autouse=True)
def patch_all():
    # 1) Patch builtins.open *in the runEDQ module*, not globally
    m_open = mock_open()
    with patch("ecbr_card_self_service.edq.local_run.runEDQ.open", new_callable=lambda: m_open) as mock_file, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session")          as mock_boto_sess, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")          as mock_spark_cls, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules") as mock_exec_rules, \
         patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")               as mock_logger:
        
        # yield all the mocks so tests can configure them
        yield mock_file, mock_boto_sess, mock_spark_cls, mock_exec_rules, mock_logger

def test_main_s3_branch(patch_all):
    mock_file, mock_boto_sess, mock_spark_cls, mock_exec_rules, mock_logger = patch_all

    # A) Prepare config.yaml & secrets.yaml content
    cfg = {
      "DATA_SOURCE":   "S3",
      "S3_DATA_PATH":  "s3://mybucket/myfile.csv",
      "JOB_ID":        "JID_S3",
      "AWS_PROFILE":   "my-profile"
    }
    sec = {
      "CLIENT_ID":     "CID_S3",
      "CLIENT_SECRET": "CSEC_S3"
    }
    # first open() -> config.yaml, second open() -> secrets.yaml
    mock_file.side_effect = [
      io.StringIO(yaml.dump(cfg)),
      io.StringIO(yaml.dump(sec))
    ]

    # B) Stub boto3 credentials
    fake_sess = MagicMock()
    fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
    # boto3.Session().get_credentials().get_frozen_credentials() => fake_creds
    fake_sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
    mock_boto_sess.return_value = fake_sess

    # C) Stub SparkSession builder -> read -> load
    fake_spark  = MagicMock(name="spark")
    fake_builder = MagicMock(name="builder")
    # SparkSession.builder.appName(...).getOrCreate() => fake_spark
    fake_builder.appName.return_value.getOrCreate.return_value = fake_spark
    mock_spark_cls.builder = fake_builder

    # Spark read chain
    fake_reader = MagicMock(name="reader") 
    fake_spark.read.format.return_value = fake_reader
    fake_reader.option.return_value.load.return_value = fake_spark  # pretend load() gives us a DF

    # D) Stub engine.execute_rules()
    fake_rows = MagicMock(name="row_level_results")
    fake_rows.show.return_value = None
    mock_exec_rules.return_value = {
      "result_type": "ExecutionCompleted",
      "job_execution_result": {
        "results": [],
        "total_DF_rows": 0,
        "row_level_results": fake_rows
      }
    }

    # E) Now call main()
    main()

    # ---- Assertions ----

    # 1) We should have read the config file and secrets file
    assert mock_file.call_count == 2

    # 2) We should log that we're loading from S3
    mock_logger.info.assert_any_call("Loading data from S3 URI")

    # 3) Our Spark read chain must have been invoked with the CSV format and the right S3 path
    fake_spark.read.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://mybucket/myfile.csv")

    # 4) engine.execute_rules gets exactly the signature for S3, including the uppercase "S3"
    mock_exec_rules.assert_called_once_with(
      fake_spark,            # the DF returned by load()
      "JID_S3",              # job_id
      "CID_S3",              # client_id
      "CSEC_S3",             # client_secret
      "S3"                   # data_source
    )

    # 5) And we printed the row-level results
    fake_rows.show.assert_called_once_with(0, truncate=False)
