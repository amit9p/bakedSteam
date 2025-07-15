
# tests/edq/test_runEDQ.py
import sys, unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# stub out missing imports
sys.modules["edq_lib"]      = MagicMock()
sys.modules["boto3"]        = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

from ecbr_card_self_service.edq.local_run.runEDQ import main, engine, SparkSession, logger

class TestRunEDQ(unittest.TestCase):

    def _setup_config_and_secrets(self, mock_open_file, config, secrets):
        """Helper to wire open() to return our two YAML blobs."""
        mock_open_file.side_effect = [
            mock_open(read_data=yaml.dump(config)).return_value,
            mock_open(read_data=yaml.dump(secrets)).return_value
        ]

    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_local(self, mock_logger, mock_exec, mock_spark_cls, mock_open_file):
        # -- A) YAML stubs --
        cfg = {"DATA_SOURCE":"LOCAL","LOCAL_DATA_PATH":"base_segment.csv","JOB_ID":"JID"}
        sec = {"CLIENT_ID":"CID","CLIENT_SECRET":"CSEC"}
        self._setup_config_and_secrets(mock_open_file, cfg, sec)

        # -- B) Fake SparkSession.builder.getOrCreate() --
        fake_spark = MagicMock()
        fake_builder = MagicMock()
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # -- C) Fake read.format(..).option(..).load(..) --
        fake_df = MagicMock()
        rdr = fake_spark.read.format.return_value
        rdr.option.return_value.load.return_value = fake_df

        # -- D) Stub engine.execute_rules() →
        fake_rows = MagicMock()
        fake_rows.show.return_value = None
        mock_exec.return_value = {
          "result_type":"ExecutionCompleted",
          "job_execution_result":{
            "results":[{"rule_id":"1","rule_type":"A","field_name":"f","status":"ok"}],
            "total_DF_rows":1,
            "row_level_results":fake_rows
          }
        }

        # -- E) Run & F) Assert --
        main()

        mock_logger.info.assert_any_call("Loading data from local file")
        fake_spark.read.format.assert_called_once_with("csv")
        rdr.option.assert_called_once_with("header","true")
        rdr.load.assert_called_once_with("base_segment.csv")
        mock_exec.assert_called_once_with(fake_df, "JID","CID","CSEC","LOCAL")
        fake_rows.show.assert_called_once_with(1, truncate=False)


    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_s3(self, mock_logger, mock_exec, mock_spark_cls, mock_boto_sess, mock_open_file):
        # A) config/secrets for S3
        cfg = {
          "DATA_SOURCE":"S3",
          "S3_DATA_PATH":"s3://bucket/base_segment.csv",
          "JOB_ID":"J2",
          "AWS_PROFILE":"myp"
        }
        sec = {"CLIENT_ID":"CID2","CLIENT_SECRET":"CSEC2"}
        self._setup_config_and_secrets(mock_open_file, cfg, sec)

        # B) Fake boto3 credentials...
        fake_creds = MagicMock(access_key="A", secret_key="K", token=None)
        sess = MagicMock()
        sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
        mock_boto_sess.return_value = sess

        # C) Fake SparkSession builder
        fake_spark = MagicMock()
        fake_builder = MagicMock()
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) Fake read
        fake_df = MagicMock()
        rdr = fake_spark.read.format.return_value
        rdr.option.return_value.load.return_value = fake_df

        # E) Stub exec_rules
        fake_rows = MagicMock()
        fake_rows.show.return_value = None
        mock_exec.return_value = {
          "result_type":"ExecutionCompleted",
          "job_execution_result":{"results":[],"total_DF_rows":0,"row_level_results":fake_rows}
        }

        # F) Run & Assert
        main()
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_spark.read.format.assert_called_with("csv")
        mock_exec.assert_called_once_with(fake_df, "J2","CID2","CSEC2","S3")
        fake_rows.show.assert_called_once_with(0, truncate=False)


    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.OneLakeSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_onelake(self, mock_logger, mock_exec, mock_spark_cls, mock_ol_sess, mock_open_file):
        # A) config/secrets for ONE LAKE
        cfg = {
          "DATA_SOURCE":"ONE_LAKE",
          "ONELAKE_CATALOG_ID":"c",
          "ONELAKE_LOAD_PARTITION_DATE":"2025-04-01",
          "JOB_ID":"J3"
        }
        sec = {"CLIENT_ID":"CID3","CLIENT_SECRET":"CSEC3"}
        self._setup_config_and_secrets(mock_open_file, cfg, sec)

        # B) Fake OneLakeSession.get_dataset(...) → returns an object with .location
        fake_ds = MagicMock(location="s3://some/path/to/2025-04-01/")
        mock_ol_sess.return_value.get_dataset.return_value = fake_ds

        # C) Fake SparkSession.builder
        fake_spark = MagicMock()
        fake_sparkbuilder = MagicMock()
        mock_spark_cls.builder = fake_sparkbuilder
        fake_sparkbuilder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) Fake read.parquet load
        fake_df = MagicMock()
        fake_spark.read.format.return_value.load.return_value = fake_df

        # E) Stub exec_rules
        fake_rows = MagicMock(); fake_rows.show.return_value=None
        mock_exec.return_value = {
          "result_type":"ExecutionCompleted",
          "job_execution_result":{"results":[],"total_DF_rows":0,"row_level_results":fake_rows}
        }

        # F) Run & Assert
        main()
        mock_logger.info.assert_any_call("Loading data from OneLake partition")
        fake_spark.read.format.assert_called_with("parquet")
        mock_exec.assert_called_once_with(fake_df, "J3","CID3","CSEC3","ONE_LAKE")
        fake_rows.show.assert_called_once_with(0, truncate=False)


if __name__ == "__main__":
    unittest.main()
