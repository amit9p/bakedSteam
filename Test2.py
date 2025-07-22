
from unittest.mock import patch, MagicMock
import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ   # <-- import once

FULL_PATH = "ecbr_card_self_service.edq.local_run.runEDQ.get_partition"

@patch.object(runEDQ, "SparkSession",   create=True)           # stub Spark
@patch.object(runEDQ, "OneLakeSession", create=True)           # stub SDK
@patch.object(runEDQ, "engine",         create=True)           # stub EDQ engine
@patch(FULL_PATH, return_value="bucket/path/2025-04-10")       # ← *THIS* line
def test_main_onelake(self,
                      mock_get_part,
                      mock_engine,
                      mock_ol_cls,
                      mock_spark_cls):
    """Happy-path for the OneLake branch without touching real get_partition."""
    # ── 1. force onelake branch ───────────────────────────────────────────
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID":          "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID":                      "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # ── 2. OneLake session & dataset stubs ───────────────────────────────
    session = mock_ol_cls.return_value
    dataset = MagicMock()
    session.get_dataset.return_value = dataset

    # ── 3. Spark stub that yields a fake dataframe ───────────────────────
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # ── 4. Engine returns a minimal success structure ────────────────────
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # ── 5. Run the code under test ───────────────────────────────────────
    runEDQ.main()

    # ── 6. Assertions (lightweight) ──────────────────────────────────────
    session.get_dataset.assert_called_once_with("CAT-123")
    mock_get_part.assert_called_once_with(dataset, "2025-04-10")

    fake_spark.read.format.assert_called_once_with("parquet")
    fake_spark.read.format.return_value.load.assert_called_once_with(
        "s3a://bucket/path/2025-04-10"
    )
    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
