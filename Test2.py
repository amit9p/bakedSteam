

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
        """Happy-path test for the onelake execution branch."""

        # 1️⃣  Force the 'onelake' branch
        _inject_cfg(
            {
                "DATA_SOURCE": "onelake",
                "ONELAKE_CATALOG_ID": "CAT-123",
                "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
                "JOB_ID": "JOB_OL",
            },
            {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
        )

        # 2️⃣  Stub OneLake session ➜ dataset ➜ s3fs
        fake_session  = MagicMock(name="onelake_session")
        mock_onelake_cls.return_value = fake_session

        fake_dataset  = MagicMock(name="dataset")
        fake_session.get_dataset.return_value = fake_dataset
        fake_dataset.location = "s3://bucket/path/"

        fake_s3fs = MagicMock(name="s3fs")
        fake_dataset.get_s3fs.return_value = fake_s3fs

        # Provide two different directory listings on two consecutive calls
        call_counter = {"n": 0}

        def ls_side_effect(_path):
            call_counter["n"] += 1
            # 1st call: list of partition directories
            if call_counter["n"] == 1:
                return ["s3://bucket/path/2025-04-10/"]
            # 2nd call: files inside the chosen partition
            return ["s3://bucket/path/2025-04-10/part-00000.parquet"]

        fake_s3fs.ls.side_effect = ls_side_effect

        # 3️⃣  Stub Spark
        fake_df, fake_spark = MagicMock(), MagicMock()
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
        self.assertEqual(fake_s3fs.ls.call_count, 2)               # two listings
        fake_spark.read.format.assert_called_once_with("parquet")  # parquet read
        mock_engine.execute_rules.assert_called_once_with(
            fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
        )
