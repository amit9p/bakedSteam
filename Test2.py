
"""
Test the S3 execution branch of runEDQ.main() by **over-writing**
the module-level `config` and `secrets` that were populated at import-time.

No files are read during the test; heavy dependencies are stubbed.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# 1)  Stub heavy libraries *before* importing the code under test so that
#     the import doesn’t try to pull real boto3 / edq_lib / onelake packages.
# ---------------------------------------------------------------------------
sys.modules["edq_lib"] = MagicMock()
sys.modules["boto3"] = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

# ---------------------------------------------------------------------------
# 2)  Import the module we want to test
#     (update the import path if your project structure is different)
# ---------------------------------------------------------------------------
from ecbr_card_self_service.edq.local_run import runEDQ  # noqa: E402

# ---------------------------------------------------------------------------
# 3)  Test class
# ---------------------------------------------------------------------------
class TestRunEDQS3(unittest.TestCase):
    """Validate that main() takes the S3 branch when DATA_SOURCE == 's3'."""

    def setUp(self) -> None:
        # Desired YAML content for this test run
        self.cfg = {
            "DATA_SOURCE": "s3",
            "S3_DATA_PATH": "s3://bucket/base_segment.csv",
            "JOB_ID": "JOB_S3",
            "AWS_PROFILE": "my-profile",
        }
        self.sec = {
            "CLIENT_ID": "CID_S3",
            "CLIENT_SECRET": "CSEC_S3",
        }

        # ------------------------------------------------------------------
        # **Key step** – overwrite the already-loaded dicts inside runEDQ
        # ------------------------------------------------------------------
        # If the attributes don’t exist (rare), create them as empty dicts.
        if not hasattr(runEDQ, "config"):
            runEDQ.config = {}
        if not hasattr(runEDQ, "secrets"):
            runEDQ.secrets = {}

        runEDQ.config.clear()
        runEDQ.config.update(self.cfg)

        runEDQ.secrets.clear()
        runEDQ.secrets.update(self.sec)

    # ----------------------------------------------------------------------
    # Patch internal objects used during the run:
    #   • logger      – to watch for branch-specific log lines
    #   • boto3       – to supply fake AWS credentials
    #   • SparkSession– to avoid starting a real Spark JVM
    #   • engine      – to short-circuit the EDQ engine call
    # ----------------------------------------------------------------------
    @patch.object(runEDQ, "logger")
    @patch.object(runEDQ, "boto3")
    @patch.object(runEDQ, "SparkSession")
    @patch.object(runEDQ, "engine")
    def test_main_s3(
        self,
        mock_engine,
        mock_spark_cls,
        mock_boto3,
        mock_logger,
    ):
        # ---------- boto3 stub ----------
        fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
        mock_boto3.Session.return_value.get_credentials.return_value.\
            get_frozen_credentials.return_value = fake_creds

        # ---------- Spark stub ----------
        fake_df = MagicMock(name="df_s3")
        fake_spark = MagicMock(name="spark_s3")

        # Builder chain: SparkSession.builder.appName(...).config(...).config(...).getOrCreate()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value
            .config.return_value
            .getOrCreate.return_value
        ) = fake_spark

        # read.format("csv").option(...).load(...) → fake_df
        fake_read_chain = (
            fake_spark.read.format.return_value.option.return_value
        )
        fake_read_chain.load.return_value = fake_df

        # ---------- EDQ engine stub ----------
        mock_engine.execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 0,
                "row_level_results": MagicMock(),
            },
        }

        # ---------- Run the function ----------
        runEDQ.main()

        # ---------- Assertions ----------
        # 1. Correct branch taken?
        mock_logger.info.assert_any_call("Loading data from S3 URI")

        # 2. Spark read-path sanity check
        fake_spark.read.format.assert_called_once_with("csv")

        # 3. Engine called with expected arguments
        mock_engine.execute_rules.assert_called_once_with(
            fake_df,           # dataframe loaded from S3
            "JOB_S3",          # job id from cfg
            "CID_S3",          # client id from secrets
            "CSEC_S3",         # client secret from secrets
            "S3",              # env argument hard-coded in source
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
