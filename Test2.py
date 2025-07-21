
# ──────────────────────────────────────────────────────────────────────
    # ONELAKE BRANCH
    # ──────────────────────────────────────────────────────────────────────
    @patch.object(runEDQ, "logger",         create=True)
    @patch.object(runEDQ, "SparkSession",   create=True)
    @patch.object(runEDQ, "OneLakeSession", create=True)
    @patch.object(runEDQ, "engine",         create=True)
    @patch.object(runEDQ, "get_partition",  return_value="s3://bucket/path/2025-04-10",
                   create=True)
    def test_main_onelake(
        self,
        mock_get_part,
        mock_engine,
        mock_onelake_cls,
        mock_spark_cls,
        mock_logger,
    ):
        """
        Happy-path test for the OneLake branch:
        * session.get_dataset() is called
        * Spark reads Parquet from the partition path returned by get_partition()
        * engine.execute_rules receives expected arguments
        """

        # 1️⃣  Drive the 'onelake' path via config / secrets
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID": "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID": "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2️⃣  Stub OneLake session & dataset (only to the extent main() touches)
        fake_session  = MagicMock(name="onelake_session")
        fake_dataset  = MagicMock(name="dataset")
        mock_onelake_cls.return_value            = fake_session
        fake_session.get_dataset.return_value    = fake_dataset

        # 3️⃣  Stub Spark machinery
        fake_df, fake_spark = MagicMock(name="df_ol"), MagicMock(name="spark_ol")
        (
            mock_spark_cls.builder.appName.return_value
            .config.return_value.config.return_value.getOrCreate.return_value
        ) = fake_spark
        fake_spark.read.format.return_value.load.return_value = fake_df

        # 4️⃣  Stub engine result
        mock_engine.execute_rules.return_value = _fake_engine_result()

        # 5️⃣  Run
        runEDQ.main()

        # 6️⃣  Assertions
        fake_session.get_dataset.assert_called_once_with("CAT-123")
        mock_get_part.assert_called_once_with(fake_dataset, "2025-04-10")
        fake_spark.read.format.assert_called_once_with("parquet")
        fake_spark.read.format.return_value.load.assert_called_once_with(
            "s3://bucket/path/2025-04-10"
        )
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )
