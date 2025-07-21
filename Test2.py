
# ────────────────────────────────────────────────────────────────
# ONE-LAKE happy-path – works with get_partition() as in source
# ────────────────────────────────────────────────────────────────
@patch.object(runEDQ, "SparkSession",   create=True)      # ① stub Spark
@patch.object(runEDQ, "OneLakeSession", create=True)      # ② stub SDK
@patch.object(runEDQ, "engine",         create=True)      # ③ stub EDQ engine
def test_main_onelake(self,
                      mock_engine,      # ③
                      mock_ol_cls,      # ②
                      mock_spark_cls):  # ①
    # 1️⃣  Drive the onelake path
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID":         "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE":"2025-04-10",
            "JOB_ID":                     "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # 2️⃣  OneLake session → dataset → s3fs
    session_mock = mock_ol_cls.return_value
    dataset_mock = MagicMock()
    dataset_mock.location = "s3://bucket/path"
    session_mock.get_dataset.return_value = dataset_mock

    s3fs_mock = MagicMock()
    dataset_mock.get_s3fs.return_value = s3fs_mock   # ← crucial line

    # ls() needs to succeed twice (folder, then file)
    s3fs_mock.ls.side_effect = [
        ["s3://bucket/path/2025-04-10/"],                    # first call
        ["s3://bucket/path/2025-04-10/part-0.parquet"],      # second call
    ]

    # 3️⃣  Spark stub
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # 4️⃣  Engine stub
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # 5️⃣  Run code
    runEDQ.main()

    # 6️⃣  Assertions (lightweight)
    session_mock.get_dataset.assert_called_once_with("CAT-123")
    self.assertEqual(s3fs_mock.ls.call_count, 2)

    fake_spark.read.format.assert_called_once_with("parquet")
    fake_spark.read.format.return_value.load.assert_called_once_with(
        "s3a://bucket/path/2025-04-10"
    )

    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
