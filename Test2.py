
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import yaml

# 1) Prevent import errors for these modules in your test
sys.modules["edq_lib"]      = MagicMock()
sys.modules["boto3"]        = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

# 2) Import the code under test (adjust path if needed)
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
        Full end-to-end test of `main()` in local mode, covering:
        - config.yaml and secrets.yml reads,
        - spark.read.format(...).option(...).load(...),
        - engine.execute_rules(...) call with literal "NonProd",
        - final df.show(truncate=False).
        """

        # ─── A) Stub config.yaml then secrets.yml ────────────────────────────
        fake_config = {
            "DATA_SOURCE":     "LOCAL",
            "LOCAL_DATA_PATH": "base_segment.csv",
            "JOB_ID":          "mock_job_id"
        }
        fake_secrets = {
            "CLIENT_ID":     "mock_client_id",
            "CLIENT_SECRET": "mock_client_secret"
            # note: ENVIRONMENT is hard-coded as "NonProd" in code
        }

        handle = mock_open_file()
        handle.read.side_effect = [
            yaml.dump(fake_config),   # first open() → config.yaml
            yaml.dump(fake_secrets),   # second open() → secrets.yml
        ]

        # ─── B) Stub SparkSession.builder.getOrCreate() ──────────────────────
        fake_spark   = MagicMock(name="spark_session")
        fake_builder = MagicMock(name="spark_builder")

        # SparkSession.builder → fake_builder
        mock_spark_cls.builder = fake_builder
        # builder.appName(...).getOrCreate() → fake_spark
        fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

        # ─── C) Stub spark.read.format().option().load() → fake_df ──────────
        fake_df     = MagicMock(name="dataframe")
        fake_reader = MagicMock(name="reader")

        # chaining: format() → reader, option() → reader, load() → fake_df
        fake_reader.format.return_value = fake_reader
        fake_reader.option.return_value = fake_reader
        fake_reader.load.return_value   = fake_df

        # attach to the spark session
        fake_spark.read = fake_reader

        # ─── D) Stub engine.execute_rules(...) → nested dict as code expects ─
        mock_execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],              # code loops over this
                "total_DF_rows":  0,        # code reads this into total_rows
                "row_level_results": fake_df  # code calls .show on this
            }
        }

        # ─── E) Invoke main() ─────────────────────────────────────────────────
        main()

        # ─── F) Assertions ────────────────────────────────────────────────────

        # 1) Logger should note loading from local
        mock_logger.info.assert_any_call("Loading data from local file")

        # 2) Spark read chain happened exactly once each
        fake_reader.format.assert_called_once_with("csv")
        fake_reader.option.assert_called_once_with("header", "true")
        fake_reader.load.assert_called_once_with("base_segment.csv")

        # 3) engine.execute_rules was called with the literal "NonProd"
        mock_execute_rules.assert_called_once_with(
            fake_df,
            "mock_job_id",
            "mock_client_id",
            "mock_client_secret",
            "NonProd"
        )

        # 4) Finally, show() was invoked on the returned dataframe
        fake_df.show.assert_called_once_with(0, truncate=False)


if __name__ == "__main__":
    unittest.main()
```0
