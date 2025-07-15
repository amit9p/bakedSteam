
@patch("builtins.open", new_callable=mock_open)
    @patch("oneLake_mini.OneLakeSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_onelake(self, mock_logger, mock_exec, mock_spark_cls, mock_ol, mock_open):
        # … set up config, secrets, stubs, etc …

        main()

        # grab just the messages
        calls = [args[0] for args, _ in mock_logger.info.call_args_list]
        assert any("OneLake partition" in msg for msg in calls), \
            f"expected a OneLake partition log in {calls!r}"

        # then the rest of your assertions…


______
import sys, os, unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# 1) Prevent ImportErrors from edq_lib, boto3 and oneLake_mini
sys.modules["edq_lib"]      = MagicMock()
sys.modules["boto3"]        = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

# 2) Import the code under test
from ecbr_card_self_service.edq.local_run.runEDQ import main, engine, SparkSession, logger, OneLakeSession

class TestRunEDQ(unittest.TestCase):
    def stub_yaml_reads(self, mock_open_file, config_dict, secret_dict):
        """Make builtins.open() first return config, then secrets YAML."""
        handle = mock_open_file()
        handle.read.side_effect = [
            yaml.dump(config_dict),
            yaml.dump(secret_dict),
        ]
        return handle

    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_local(self, mock_logger, mock_exec_rules, mock_spark_cls, mock_open_file):
        # A) stub config & secrets
        cfg = {"DATA_SOURCE":"LOCAL", "LOCAL_DATA_PATH":"base_segment.csv", "JOB_ID":"JID_LOCAL"}
        sec = {"CLIENT_ID":"CID_L","CLIENT_SECRET":"CSEC_L"}
        self.stub_yaml_reads(mock_open_file, cfg, sec)

        # B) stub SparkSession.builder.getOrCreate() → fake_spark
        fake_spark = MagicMock(name="spark_local")
        fake_builder = MagicMock(name="builder_local")
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # C) stub spark.read.format(...).option(...).load(...) → fake_df
        fake_df = MagicMock(name="df_local")
        fake_reader = MagicMock(name="reader_local")
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        fake_reader.load.return_value   = fake_df
        fake_spark.read = fake_reader

        # D) stub engine.execute_rules(...) → return the shape your code expects
        fake_rows = MagicMock(name="rows_local")
        fake_rows.show.return_value = None
        mock_exec_rules.return_value = {
            "result_type":"ExecutionCompleted",
            "job_execution_result":{
                "results":[],
                "total_DF_rows":0,
                "row_level_results":fake_rows
            }
        }

        # E) run & F) assert
        main()
        mock_logger.info.assert_any_call("Loading data from local file")
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header","true")
        fake_reader.load.assert_called_once_with("base_segment.csv")
        mock_exec_rules.assert_called_once_with(
            fake_df, "JID_LOCAL", "CID_L", "CSEC_L", "LOCAL"
        )
        fake_rows.show.assert_called_once_with(0, truncate=False)


    @patch("builtins.open", new_callable=mock_open)
    @patch("boto3.Session")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_s3(self, mock_logger, mock_exec_rules, mock_spark_cls, mock_boto_sess, mock_open_file):
        # A) S3 config & secrets
        cfg = {
          "DATA_SOURCE":"S3",
          "S3_DATA_PATH":"s3://bucket/base_segment.csv",
          "JOB_ID":"JID_S3",
          "AWS_PROFILE":"my-profile"
        }
        sec = {"CLIENT_ID":"CID_S3","CLIENT_SECRET":"CSEC_S3"}
        self.stub_yaml_reads(mock_open_file, cfg, sec)

        # B) boto3.Session().get_credentials().get_frozen_credentials()
        fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
        sess = MagicMock()
        sess.get_credentials.return_value.get_frozen_credentials.return_value = fake_creds
        mock_boto_sess.return_value = sess

        # C) SparkSession stub
        fake_spark = MagicMock(name="spark_s3")
        fake_builder = MagicMock(name="builder_s3")
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) read CSV stub
        fake_df  = MagicMock(name="df_s3")
        fake_reader = MagicMock(name="reader_s3")
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        fake_reader.load.return_value   = fake_df
        fake_spark.read = fake_reader

        # E) engine stub
        fake_rows = MagicMock(name="rows_s3"); fake_rows.show.return_value=None
        mock_exec_rules.return_value = {
            "result_type":"ExecutionCompleted",
            "job_execution_result":{"results":[], "total_DF_rows":0, "row_level_results":fake_rows}
        }

        # F) run & assert
        main()
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.load.assert_called_once_with("s3://bucket/base_segment.csv")
        mock_exec_rules.assert_called_once_with(
            fake_df, "JID_S3", "CID_S3", "CSEC_S3", "S3"
        )
        fake_rows.show.assert_called_once_with(0, truncate=False)


    @patch("builtins.open", new_callable=mock_open)
    @patch("oneLake_mini.OneLakeSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_onelake(self, mock_logger, mock_exec_rules, mock_spark_cls, mock_ol_sess, mock_open_file):
        # A) OneLake config & secrets
        cfg = {
          "DATA_SOURCE":"ONE_LAKE",
          "ONELAKE_CATALOG_ID":"cat123",
          "ONELAKE_LOAD_PARTITION_DATE":"2025-04-01",
          "JOB_ID":"JID_OL"
        }
        sec = {"CLIENT_ID":"CID_OL","CLIENT_SECRET":"CSEC_OL"}
        self.stub_yaml_reads(mock_open_file, cfg, sec)

        # B) OneLakeSession().get_dataset(...)
        fake_ds = MagicMock(location="s3://some/2025-04-01/")
        mock_ol_sess.return_value.get_dataset.return_value = fake_ds

        # C) SparkSession stub
        fake_spark  = MagicMock(name="spark_ol")
        fake_builder = MagicMock(name="builder_ol")
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # D) parquet read stub
        fake_df     = MagicMock(name="df_ol")
        fake_reader = MagicMock(name="reader_ol")
        fake_reader.format.return_value = fake_reader
        fake_reader.load.return_value   = fake_df
        fake_spark.read = fake_reader

        # E) engine stub
        fake_rows = MagicMock(name="rows_ol"); fake_rows.show.return_value=None
        mock_exec_rules.return_value = {
            "result_type":"ExecutionCompleted",
            "job_execution_result":{"results":[], "total_DF_rows":0, "row_level_results":fake_rows}
        }

        # F) run & assert
        main()
        mock_logger.info.assert_any_call("Loading data from OneLake partition")
        fake_reader.format.assert_called_once_with("parquet")
        fake_reader.load.assert_called_once_with("s3://some/2025-04-01/")
        mock_exec_rules.assert_called_once_with(
            fake_df, "JID_OL", "CID_OL", "CSEC_OL", "ONE_LAKE"
        )
        fake_rows.show.assert_called_once_with(0, truncate=False)


if __name__ == "__main__":
    unittest.main()
