
# ────────────────────────────────────────────────────────────────
    # ONE-LAKE  (simple happy path)
    # ────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "SparkSession",   create=True)   # ① stub Spark
    @patch.object(runEDQ, "OneLakeSession", create=True)   # ② stub OneLake
    @patch.object(runEDQ, "engine",         create=True)   # ③ stub EDQ engine
    @patch.object(                                   # ④ short-circuit the
        runEDQ,                                     #    directory-scan helper
        "get_partition",
        return_value="bucket/path/2025-04-10",      #    → just hand back a path
        create=True,
    )
    def test_main_onelake_simple(
        self,
        mock_get_partition,    # ④
        mock_engine,           # ③
        mock_ol_cls,           # ②
        mock_spark_cls,        # ①
    ):
        # 1️⃣  inject YAML so main() chooses the OneLake branch
        _inject_cfg(
            {
                "DATA_SOURCE":               "onelake",
                "ONELAKE_CATALOG_ID":        "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE":"2025-04-10",
                "JOB_ID":                    "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2️⃣  OneLake session ⇢ dataset
        session_mock          = mock_ol_cls.return_value
        dataset_mock          = MagicMock()
        dataset_mock.get_location.return_value = "s3://bucket/path"
        session_mock.get_dataset.return_value = dataset_mock

        # 3️⃣  Spark stub (parquet read)
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        # 4️⃣  engine.execute_rules fake result (needs .row_level_results.show())
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # 5️⃣  run the code
        runEDQ.main()

        # 6️⃣  assertions – keep them lightweight
        session_mock.get_dataset.assert_called_once_with("CAT-123")
        mock_get_partition.assert_called_once_with(dataset_mock, "2025-04-10")

        fake_spark.read.format.assert_called_once_with("parquet")
        fake_spark.read.format.return_value.load.assert_called_once_with(
            "s3a://bucket/path/2025-04-10"
        )

        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )
