
# tests/edq/test_runEDQ.py
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# 1) Prevent import errors for these modules in your test
sys.modules["edq_lib"]       = MagicMock()
sys.modules["boto3"]         = MagicMock()
sys.modules["oneLake_mini"]  = MagicMock()

# now import your production code under test
from ecbr_card_self_service.edq.local_run.runEDQ import main, engine, SparkSession, logger

class TestRunEDQ(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_local(self,
                        mock_logger,
                        mock_execute_rules,
                        mock_spark_cls,
                        mock_open_file):
        # ──── A) Prepare fake config.yml + secrets.yml ─────────────────────────
        fake_config = {
            "DATA_SOURCE":    "LOCAL",
            "LOCAL_DATA_PATH":"base_segment.csv",
            "JOB_ID":         "mock_job_id"
        }
        fake_secrets = {
            "CLIENT_ID":     "mock_client_id",
            "CLIENT_SECRET": "mock_client_secret"
        }

        # First call to open() → config.yml, second → secrets.yml
        mock_open_file.side_effect = [
            mock_open(read_data=yaml.dump(fake_config)).return_value,
            mock_open(read_data=yaml.dump(fake_secrets)).return_value
        ]

        # ──── B) Fake out SparkSession.builder → .getOrCreate() ──────────────
        fake_spark = MagicMock(name="spark_session")
        fake_builder = MagicMock(name="spark_builder")
        # SparkSession.builder.appName(...).getOrCreate() → our fake_spark
        mock_spark_cls.builder = fake_builder
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # ──── C) Fake the DataFrame returned by read.format(...).option(...).load(...)
        fake_df = MagicMock(name="dataframe")
        fake_reader = fake_spark.read.format.return_value
        fake_reader.option.return_value.load.return_value = fake_df

        # ──── D) Stub engine.execute_rules to return a proper row_level_results
        fake_row_df = MagicMock(name="row_level_results_df")
        fake_row_df.show.return_value = None

        mock_execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [
                    {
                      "rule_id":    "1",
                      "rule_type":  "type1",
                      "field_name": "field1",
                      "status":     "success"
                    }
                ],
                "total_DF_rows":   1,
                "row_level_results": fake_row_df
            }
        }

        # ──── E) Run!
        main()

        # ──── F) Assertions ───────────────────────────────────────────────────
        # 1) We logged that we were loading local
        mock_logger.info.assert_any_call("Loading data from local file")

        # 2) Spark read chain happened
        fake_spark.read.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header", "true")
        fake_reader.load.assert_called_once_with("base_segment.csv")

        # 3) engine.execute_rules was invoked with exactly the stubbed args
        mock_execute_rules.assert_called_once_with(
            fake_df,
            fake_config["JOB_ID"],
            fake_secrets["CLIENT_ID"],
            fake_secrets["CLIENT_SECRET"],
            fake_config["DATA_SOURCE"]
        )

        # 4) Finally, we printed out the row‐level results
        fake_row_df.show.assert_called_once_with(1, truncate=False)


if __name__ == "__main__":
    unittest.main()
