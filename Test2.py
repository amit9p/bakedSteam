
"""
Tests for ecbr_card_self_service.edq.local_run.runEDQ.main()

We overwrite the module-level `config` & `secrets` dicts that were populated
at import-time, so main() behaves as if it had read the desired YAML values.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# ─────────────────────────────────────────────────────────────────────────────
# 1)  Stub heavy libs *before* we import the code under test
# ─────────────────────────────────────────────────────────────────────────────
sys.modules["edq_lib"] = MagicMock()
sys.modules["boto3"] = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

from ecbr_card_self_service.edq.local_run import runEDQ  # noqa: E402

# Helpers ─────────────────────────────────────────────────────────────────────
def _inject_cfg(cfg_dict, sec_dict):
    """Overwrite cached YAML dicts inside runEDQ."""
    if not hasattr(runEDQ, "config"):
        runEDQ.config = {}
    if not hasattr(runEDQ, "secrets"):
        runEDQ.secrets = {}
    runEDQ.config.clear(), runEDQ.config.update(cfg_dict)
    runEDQ.secrets.clear(), runEDQ.secrets.update(sec_dict)


class TestRunEDQ(unittest.TestCase):
    # ──────────────────────────────────────────────────────────────────────
    # S3 BRANCH  – already validated earlier, kept here for completeness
    # ──────────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger")
    @patch.object(runEDQ, "boto3")
    @patch.object(runEDQ, "SparkSession")
    @patch.object(runEDQ, "engine")
    def test_main_s3(self, mock_engine, mock_spark_cls, mock_boto3, mock_logger):

        cfg = {
            "DATA_SOURCE": "s3",
            "S3_DATA_PATH": "s3://bucket/base_segment.csv",
            "JOB_ID": "JOB_S3",
            "AWS_PROFILE": "my-profile",
        }
        sec = {"CLIENT_ID": "CID_S3", "CLIENT_SECRET": "CSEC_S3"}
        _inject_cfg(cfg, sec)

        fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
        mock_boto3.Session.return_value.get_credentials.return_value.\
            get_frozen_credentials.return_value = fake_creds

        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.option.return_value.load.return_value = (
            fake_df
        )

        mock_engine.execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {"results": [], "total_DF_rows": 0},
        }

        runEDQ.main()

        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_spark.read.format.assert_called_once_with("csv")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_S3", "CID_S3", "CSEC_S3", "NonProd"
        )

    # ──────────────────────────────────────────────────────────────────────
    # LOCAL BRANCH
    # ──────────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger")
    @patch.object(runEDQ, "SparkSession")
    @patch.object(runEDQ, "engine")
    def test_main_local(self, mock_engine, mock_spark_cls, mock_logger):

        cfg = {
            "DATA_SOURCE": "local",
            "LOCAL_DATA_PATH": "base_segment.csv",
            "JOB_ID": "JOB_LOCAL",
        }
        sec = {"CLIENT_ID": "CID_L", "CLIENT_SECRET": "CSEC_L"}
        _inject_cfg(cfg, sec)

        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.option.return_value.load.return_value = (
            fake_df
        )

        mock_engine.execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {"results": [], "total_DF_rows": 0},
        }

        runEDQ.main()

        mock_logger.info.assert_any_call("loading data from local file")
        fake_spark.read.format.assert_called_once_with("csv")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_LOCAL", "CID_L", "CSEC_L", "NonProd"
        )

    # ──────────────────────────────────────────────────────────────────────
    # ONELAKE BRANCH
    # ──────────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger")
    @patch.object(runEDQ, "SparkSession")
    @patch.object(runEDQ, "engine")
    @patch.object(runEDQ, "OneLakeSession")
    def test_main_onelake(
        self,
        mock_ol_cls,
        mock_engine,
        mock_spark_cls,
        mock_logger,
    ):

        cfg = {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID": "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID": "JOB_OL",
        }
        sec = {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"}
        _inject_cfg(cfg, sec)

        # ----- stub OneLake session / dataset / partition discovery -----
        mock_session = MagicMock(name="onelake_session")
        mock_ol_cls.return_value = mock_session

        mock_dataset = MagicMock(name="dataset")
        mock_session.get_dataset.return_value = mock_dataset

        mock_s3fs = MagicMock(name="s3fs")
        mock_dataset.get_s3fs.return_value = mock_s3fs

        # first ls() => directories; second ls() => files in that directory
        mock_s3fs.ls.side_effect = [
            ["s3://bucket/path/2025-04-10/"],
            ["s3://bucket/path/2025-04-10/part-00000.parquet"],
        ]

        # ----- stub Spark -----
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        mock_engine.execute_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {"results": [], "total_DF_rows": 0},
        }

        runEDQ.main()

        # validate Onelake-specific interactions
        mock_session.get_dataset.assert_called_once_with("CAT-123")
        fake_spark.read.format.assert_called_once_with("parquet")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
