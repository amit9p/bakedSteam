
# ──────────────────────────────────────────────────────────────────────
    # ONELAKE BRANCH
    # ──────────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger",         create=True)
    @patch.object(runEDQ, "SparkSession",   create=True)
    @patch.object(runEDQ, "OneLakeSession", create=True)
    @patch.object(runEDQ, "engine",         create=True)
    def test_main_onelake(
        self,
        mock_engine,
        mock_onelake_cls,
        mock_spark_cls,
        mock_logger,
    ):
        """
        Validate the onelake execution path:
        * dataset + partition discovery happens
        * parquet read is triggered
        * engine.execute_rules is called with expected args
        """

        # ------------------------------------------------------------------
        # 1. Inject config / secrets that drive the 'onelake' branch
        # ------------------------------------------------------------------
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID": "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID": "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # ------------------------------------------------------------------
        # 2. Stub OneLake session, dataset and s3fs behaviour
        # ------------------------------------------------------------------
        fake_session = MagicMock(name="onelake_session")
        mock_onelake_cls.return_value = fake_session

        fake_dataset = MagicMock(name="dataset")
        fake_session.get_dataset.return_value = fake_dataset

        fake_s3fs = MagicMock(name="s3fs")
        fake_dataset.get_s3fs.return_value = fake_s3fs
        fake_dataset.location = "s3://bucket/path/"

        def ls_side_effect(path: str):
            """
            First call lists partition folders under base path.
            Second call lists files inside the chosen partition.
            """
            if path.endswith("2025-04-10/"):
                return ["s3://bucket/path/2025-04-10/part-00000.parquet"]
            return ["s3://bucket/path/2025-04-10/"]

        fake_s3fs.ls.side_effect = ls_side_effect

        # ------------------------------------------------------------------
        # 3. Stub Spark so no JVM starts
        # ------------------------------------------------------------------
        fake_df, fake_spark = MagicMock(name="df_ol"), MagicMock(name="spark_ol")
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        # ------------------------------------------------------------------
        # 4. Stub EDQ engine call
        # ------------------------------------------------------------------
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # ------------------------------------------------------------------
        # 5. Run and assert
        # ------------------------------------------------------------------
        runEDQ.main()

        fake_session.get_dataset.assert_called_once_with("CAT-123")
        fake_s3fs.ls.assert_any_call("s3://bucket/path/")
        fake_s3fs.ls.assert_any_call("s3://bucket/path/2025-04-10/")
        fake_spark.read.format.assert_called_once_with("parquet")

        mock_engine.execute_rules.assert_called_once_with(
            fake_df,
            "JOB_OL",
            "CID_OL",
            "CSEC_OL",
            "NonProd",
        )


______

- sys.modules["oneLake_mini"] = MagicMock()       # wrong spelling / case
+ sys.modules["onelake_mini"] = MagicMock()       # correct module name



"""
Validate all three branches of ecbr_card_self_service.edq.local_run.runEDQ.main()
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# ─── 1. stub heavy deps BEFORE importing runEDQ ──────────────────────────────
sys.modules["edq_lib"] = MagicMock()
sys.modules["boto3"] = MagicMock()
sys.modules["oneLake_mini"] = MagicMock()

from ecbr_card_self_service.edq.local_run import runEDQ   # noqa: E402

# ─── helper to overwrite cached YAML dicts ───────────────────────────────────
def _inject_cfg(cfg, sec):
    for name, d in (("config", cfg), ("secrets", sec)):
        if not hasattr(runEDQ, name):
            setattr(runEDQ, name, {})
        ref = getattr(runEDQ, name)
        ref.clear()
        ref.update(d)


# ─── common stub for EDQ engine output ───────────────────────────────────────
def _fake_engine_result():
    rows = MagicMock(name="row_level_results")   # has .show()
    return {
        "result_type": "ExecutionCompleted",
        "job_execution_result": {
            "results": [],
            "total_DF_rows": 0,
            "row_level_results": rows,
        },
    }


class TestRunEDQ(unittest.TestCase):
    # S3 branch ───────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger",           create=True)
    @patch.object(runEDQ, "SparkSession",     create=True)
    @patch.object(runEDQ, "boto3",            create=True)
    @patch.object(runEDQ, "engine",           create=True)
    def test_main_s3(self, mock_engine, mock_boto, mock_spark_cls, mock_logger):

        _inject_cfg(
            {
                "DATA_SOURCE": "s3",
                "S3_DATA_PATH": "s3://bucket/base_segment.csv",
                "JOB_ID": "JOB_S3",
                "AWS_PROFILE": "my-profile",
            },
            {"CLIENT_ID": "CID_S3", "CLIENT_SECRET": "CSEC_S3"},
        )

        # boto3 creds
        mock_boto.Session.return_value.get_credentials.return_value \
              .get_frozen_credentials.return_value = MagicMock(
                  access_key="AK", secret_key="SK", token=None
              )

        # Spark stub
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.option.return_value \
                       .load.return_value = fake_df

        # engine stub
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # run + asserts
        runEDQ.main()
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        fake_spark.read.format.assert_called_once_with("csv")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_S3", "CID_S3", "CSEC_S3", "NonProd"
        )

    # Local branch ────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger",       create=True)
    @patch.object(runEDQ, "SparkSession", create=True)
    @patch.object(runEDQ, "engine",       create=True)
    def test_main_local(self, mock_engine, mock_spark_cls, mock_logger):

        _inject_cfg(
            {
                "DATA_SOURCE": "local",
                "LOCAL_DATA_PATH": "base_segment.csv",
                "JOB_ID": "JOB_LOCAL",
            },
            {"CLIENT_ID": "CID_L", "CLIENT_SECRET": "CSEC_L"},
        )

        fake_df, fake_spark = MagicMock(), MagicMock()
        mock_spark_cls.builder.appName.return_value.getOrCreate.return_value = (
            fake_spark
        )
        fake_spark.read.format.return_value.option.return_value \
                       .load.return_value = fake_df

        mock_engine.execute_rules.return_value = _fake_engine_result()

        runEDQ.main()
        mock_logger.info.assert_any_call("Loading data from local file")
        fake_spark.read.format.assert_called_once_with("csv")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_LOCAL", "CID_L", "CSEC_L", "NonProd"
        )

    # OneLake branch ──────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger",           create=True)
    @patch.object(runEDQ, "SparkSession",     create=True)
    @patch.object(runEDQ, "OneLakeSession",   create=True)
    @patch.object(runEDQ, "engine",           create=True)
    def test_main_onelake(
        self, mock_engine, mock_ol_cls, mock_spark_cls, mock_logger
    ):
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID": "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID": "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # OneLake session / dataset / ls() chain
        mock_sess = MagicMock()
        mock_ol_cls.return_value = mock_sess
        mock_ds = MagicMock()
        mock_sess.get_dataset.return_value = mock_ds
        mock_s3fs = MagicMock()
        mock_ds.get_s3fs.return_value = mock_s3fs
        mock_s3fs.ls.side_effect = [
            ["s3://bucket/path/2025-04-10/"],
            ["s3://bucket/path/2025-04-10/part-0.parquet"],
        ]

        # Spark stub
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        mock_engine.execute_rules.return_value = _fake_engine_result()

        runEDQ.main()
        mock_sess.get_dataset.assert_called_once_with("CAT-123")
        fake_spark.read.format.assert_called_once_with("parquet")
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
