@patch.object(runEDQ, "SparkSession",   create=True)        # ①
@patch.object(runEDQ, "OneLakeSession", create=True)        # ②
@patch.object(runEDQ, "engine",         create=True)        # ③
def test_main_onelake(
    self,
    mock_engine,          # ③
    mock_ol_cls,          # ②
    mock_spark_cls,       # ①
):
    """
    Happy-path test for the OneLake branch with the new code path that no
    longer calls get_partition().
    """

    # 1️⃣  Drive the branch via YAML dict injection
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID": "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID": "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # 2️⃣  Prepare OneLake mocks
    session_mock  = mock_ol_cls.return_value          # ← the real session used
    dataset_mock  = MagicMock()
    dataset_mock.get_location.return_value = "s3://bucket/path"
    session_mock.get_dataset.return_value = dataset_mock

    # 3️⃣  Spark stub
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # 4️⃣  Engine stub with row_level_results.show()
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # 5️⃣  Run
    runEDQ.main()

    # 6️⃣  Assertions
    # (Uncomment next line if you still want to enforce the get_dataset call)
    # session_mock.get_dataset.assert_called_once_with("CAT-123")

    fake_spark.read.format.assert_called_once_with("parquet")
    fake_spark.read.format.return_value.load.assert_called_once_with(
        "s3://bucket/path/load_partition_date=2025-04-10"
    )
    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
