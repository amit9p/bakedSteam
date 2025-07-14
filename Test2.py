import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# 1) Prevent import errors for these modules in your test
sys.modules["edq_lib"]       = MagicMock()
sys.modules["boto3"]         = MagicMock()
sys.modules["oneLake_mini"]  = MagicMock()

# 2) Import the code under test
#    Adjust this to wherever your runEDQ.py actually lives.
from ecbr_card_self_service.edq.local_run.runEDQ import main, engine

class TestRunEDQLocal(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_local(self,
                        mock_logger,
                        mock_execute_rules,
                        mock_spark_cls,
                        mock_open_file):
        """
        Verifies that:
         - config.yaml & secrets.yml are read in correct order,
         - spark.read.format(...).option(...).load(...) is invoked,
         - engine.execute_rules(...) is called with the expected args
           (including the literal "NonProd" from your code),
         - df.show(truncate=False) is called at the end.
        """

        # ─── A) Stub out config.yaml then secrets.yml ─────────────────────────
        fake_config = {
            "DATA_SOURCE":     "LOCAL",
            "LOCAL_DATA_PATH": "base_segment.csv",
            "JOB_ID":          "mock_job_id"
        }
        fake_secrets = {
            "CLIENT_ID":     "mock_client_id",
            "CLIENT_SECRET": "mock_client_secret",
            # your code hard-codes "NonProd" here; we leave this out
        }

        handle = mock_open_file()
        # first read() => config, second read() => secrets
        handle.read.side_effect = [
            yaml.dump(fake_config),
            yaml.dump(fake_secrets),
        ]

        # ─── B) Stub SparkSession.builder.getOrCreate() ──────────────────────
        fake_spark   = MagicMock(name="spark_session")
        fake_builder = MagicMock(name="spark_builder")

        # SparkSession.builder → fake_builder
        mock_spark_cls.builder = fake_builder
        # builder.appName(...).config(...).config(...).getOrCreate() → fake_spark
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # ─── C) Stub spark.read.format().option().load() → fake_df ──────────
        fake_df     = MagicMock(name="dataframe")
        fake_reader = MagicMock(name="reader")

        # chain each call to return the same reader
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        # finally, load(...) returns our fake_df
        fake_reader.load.return_value   = fake_df

        # attach reader to our fake spark session
        fake_spark.read = fake_reader

        # ─── D) Stub engine.execute_rules(...) → known dict ────────────────
        mock_execute_rules.return_value = {
            "result_type":       "ExecutionCompleted",
            "job_execution_result": {"results": []},
            "total_DF_rows":     0,
            "row_level_results": fake_df
        }

        # ─── E) Run the code under test ─────────────────────────────────────
        main()

        # ─── F) Assertions ─────────────────────────────────────────────────

        # 1) We logged loading from local file
        mock_logger.info.assert_any_call("Loading data from local file")

        # 2) Spark read chain happened
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header", "true")
        fake_reader.load.assert_called_once_with("base_segment.csv")

        # 3) engine.execute_rules got the literal "NonProd" as the 5th arg
        mock_execute_rules.assert_called_once_with(
            fake_df,
            "mock_job_id",
            "mock_client_id",
            "mock_client_secret",
            "NonProd"
        )

        # 4) Finally, we printed the row-level results
        fake_df.show.assert_called_once_with(truncate=False)


if __name__ == "__main__":
    unittest.main()
