

#  put this in your TestRunEDQ class
@patch.object(runEDQ, "logger",         create=True)
@patch.object(runEDQ, "SparkSession",   create=True)
@patch.object(runEDQ, "OneLakeSession", create=True)
@patch.object(runEDQ, "engine",         create=True)
@patch.object(
    runEDQ, "get_partition", return_value="s3://bucket/path/2025-04-10", create=True
)
def test_main_onelake(
    self,
    mock_get_part,
    mock_engine,
    mock_ol_cls,
    mock_spark_cls,
    mock_logger,
):
    """
    Validate the happy-path for the onelake branch without relying on the
    internal directory-scanning logic of get_partition().
    """

    # 1️⃣ drive the onelake path
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID": "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID": "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # 2️⃣ stub OneLakeSession just enough for main()
    fake_session  = MagicMock()
    fake_dataset  = MagicMock()
    mock_ol_cls.return_value            = fake_session
    fake_session.get_dataset.return_value = fake_dataset
    fake_dataset.get_location.return_value = "s3://bucket/path"

    # 3️⃣ stub Spark
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # 4️⃣ stub engine result (gives row_level_results.show())
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # 5️⃣ run
    runEDQ.main()

    # 6️⃣ assertions – we care that get_partition was used and that the
    #                parquet file was read and sent to execute_rules.
    mock_get_part.assert_called_once_with(fake_dataset, "2025-04-10")
    fake_spark.read.format.assert_called_once_with("parquet")
    fake_spark.read.format.return_value.load.assert_called_once_with(
        "s3://bucket/path/2025-04-10"
    )
    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
