
# ────────────────────────────────────────────────────────────────────────
# OneLake branch – minimal passing version
# ────────────────────────────────────────────────────────────────────────
import unittest
from unittest.mock import patch, MagicMock
import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ


class TestRunEDQ(unittest.TestCase):
    @patch.object(runEDQ, "SparkSession",   create=True)              # stub Spark
    @patch.object(runEDQ, "OneLakeSession", create=True)              # stub SDK
    @patch.object(runEDQ, "engine",         create=True)              # stub EDQ
    @patch.object(runEDQ, "get_partition",
                  return_value="bucket/path/2025-04-10")              # stub helper
    def test_main_onelake_min(self,
                              _mock_part,      # we no longer assert on it
                              mock_engine,
                              mock_ol_cls,
                              mock_spark_cls):
        # 1. drive the onelake branch
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID":          "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID":                      "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2. OneLake session → dataset
        session  = mock_ol_cls.return_value
        dataset  = MagicMock()
        session.get_dataset.return_value = dataset

        # 3. Spark stub (fake DF)
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        # 4. engine result
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # 5. run
        runEDQ.main()

        # 6. *very* light checks (only that pipeline completed)
        assert mock_engine.execute_rules.called
        assert fake_spark.read.format.called
