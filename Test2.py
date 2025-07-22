
"""
OneLake branch test

Prereqs already in your test-suite:
  • _inject_cfg(cfg_dict, secrets_dict)  – helper that writes YAML mocks
  • _fake_engine_result()               – returns the fake ExecuteRules result
Both are reused unchanged.
"""
import unittest
from unittest.mock import patch, MagicMock

# import the module under test *once* so we can reference its globals
import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ


class TestRunEDQ(unittest.TestCase):
    # ──────────────────────────────────────────────────────────────────
    #        ONE-LAKE  branch (patch inner helper get_partition)
    # ──────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "SparkSession",   create=True)   # stub Spark
    @patch.object(runEDQ, "OneLakeSession", create=True)   # stub OneLake SDK
    @patch.object(runEDQ, "engine",         create=True)   # stub EDQ engine
    def test_main_onelake(self, mock_engine, mock_ol_cls, mock_spark_cls):
        # 1️⃣  feed config & secrets so main() chooses the onelake path
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID":          "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID":                      "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2️⃣  stub OneLake session  ➜  dataset
        sess = mock_ol_cls.return_value
        dataset = MagicMock()
        sess.get_dataset.return_value = dataset

        # 3️⃣  stub Spark (returning a fake dataframe)
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        # 4️⃣  stub EDQ engine result
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # 5️⃣  bypass the inner helper  ➜  patch main().__globals__ slot
        fake_partition = "bucket/path/2025-04-10"
        with patch.dict(runEDQ.main.__globals__,  # ← **key line**
                        {"get_partition": MagicMock(return_value=fake_partition)}):
            runEDQ.main()

        # 6️⃣  lightweight assertions
        sess.get_dataset.assert_called_once_with("CAT-123")
        runEDQ.main.__globals__["get_partition"].assert_called_once_with(
            dataset, "2025-04-10"
        )
        fake_spark.read.format.assert_called_once_with("parquet")
        fake_spark.read.format.return_value.load.assert_called_once_with(
            "s3a://bucket/path/2025-04-10"
        )
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )


# --------------------------------------------------------------------
# run with:  pytest -q  (should report 3 passed including local & s3)
# --------------------------------------------------------------------
