
@patch.object(runEDQ, "SparkSession",   create=True)      # ①
@patch.object(runEDQ, "OneLakeSession", create=True)      # ②
@patch.object(runEDQ, "engine",         create=True)      # ③
def test_main_onelake(
    self,
    mock_engine,          # ③
    mock_ol_cls,          # ②
    mock_spark_cls,       # ①
):
    """
    End-to-end happy-path for the OneLake branch.
    """

    # 1️⃣  Drive the branch
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID": "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID": "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # 2️⃣  Stub OneLakeSession → dataset → s3fs
    fake_session = MagicMock()
    mock_ol_cls.return_value = fake_session

    fake_dataset = MagicMock()
    fake_dataset.location = "s3://bucket/path/"
    fake_session.get_dataset.return_value = fake_dataset

    fake_s3fs = MagicMock()
    fake_dataset.get_s3fs.return_value = fake_s3fs

    # ls() needs to succeed twice
    def ls_side_effect(path):
        # first call: list directories under base path
        if path.rstrip("/") == "s3://bucket/path":
            return ["s3://bucket/path/2025-04-10/"]
        # second call: list parquet files in that directory
        return ["s3://bucket/path/2025-04-10/part-00000.parquet"]

    fake_s3fs.ls.side_effect = ls_side_effect

    # 3️⃣  Stub Spark
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # 4️⃣  Stub engine result (row_level_results.show() must exist)
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # 5️⃣  Run
    runEDQ.main()

    # 6️⃣  Checks
    fake_session.get_dataset.assert_called_once_with("CAT-123")
    # Two calls to s3.ls – base path then partition path
    self.assertEqual(fake_s3fs.ls.call_count, 2)
    fake_spark.read.format.assert_called_once_with("parquet")
    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
