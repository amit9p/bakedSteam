
# ────────────────────────────────────────────────────────────────
    # ONE-LAKE branch  (matches new source that builds the path
    #                   and calls spark.read.parquet())
    # ────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "SparkSession",   create=True)   # ①
    @patch.object(runEDQ, "OneLakeSession", create=True)   # ②
    @patch.object(runEDQ, "engine",         create=True)   # ③
    def test_main_onelake(
        self,
        mock_engine,        # ③
        mock_ol_cls,        # ②
        mock_spark_cls,     # ①
    ):
        """
        Happy-path test for the onelake execution branch with the current
        implementation that:  
            • calls session.get_dataset(id)  
            • builds “…/load_partition_date=YYYY-MM-DD”  
            • reads via spark.read.parquet(...)
        """

        # 1️⃣  Inject YAML so runEDQ.main() takes the onelake path
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID":        "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE":"2025-04-10",
                "JOB_ID":                    "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2️⃣  Stub OneLake session  →  dataset  →  get_location()
        session_mock  = mock_ol_cls.return_value        # OneLakeSession()
        dataset_mock  = MagicMock(name="dataset")
        dataset_mock.get_location.return_value = "s3://bucket/path"
        session_mock.get_dataset.return_value = dataset_mock

        # 3️⃣  Stub Spark so no JVM starts
        fake_df, fake_spark = MagicMock(), MagicMock()
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.parquet.return_value = fake_df   # ← new read style

        # 4️⃣  Stub engine result (row_level_results.show() must exist)
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # 5️⃣  Run the code under test
        runEDQ.main()

        # 6️⃣  Assertions
        session_mock.get_dataset.assert_called_once_with("CAT-123")

        expected_path = (
            "s3://bucket/path/load_partition_date=2025-04-10"
        )
        fake_spark.read.parquet.assert_called_once_with(expected_path)

        mock_engine.execute_rules.assert_called_once_with(
            fake_df,               # df read from parquet
            "JOB_OL",              # job id
            "CID_OL",              # client id
            "CSEC_OL",             # client secret
            "NonProd",             # env (hard-coded in main)
        )
