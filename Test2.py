
# ----------------------------------------------------------------------
# ONE-LAKE happy-path – bypass get_partition completely
# ----------------------------------------------------------------------
@patch.object(runEDQ, "SparkSession",        create=True)                # ① Spark
@patch.object(runEDQ, "OneLakeSession",      create=True)                # ② SDK
@patch.object(runEDQ, "engine",              create=True)                # ③ EDQ
@patch.object(runEDQ, "get_partition",       create=True,
              return_value="bucket/path/2025-04-10")                     # ④ BYPASS
def test_main_onelake(self,
                      mock_get_part,   # ④
                      mock_engine,     # ③
                      mock_ol_cls,     # ②
                      mock_spark_cls): # ①
    # 1️⃣  drive the onelake branch
    _inject_cfg(
        {
            "DATA_SOURCE": "onelake",
            "ONELAKE_CATALOG_ID":          "CAT-123",
            "ONELAKE_LOAD_PARTITION_DATE": "2025-04-10",
            "JOB_ID":                      "JOB_OL",
        },
        {"CLIENT_ID": "CID_OL", "CLIENT_SECRET": "CSEC_OL"},
    )

    # 2️⃣ fake OneLake session & dataset (only get_dataset is used)
    sess_mock       = mock_ol_cls.return_value
    dataset_mock    = MagicMock()
    sess_mock.get_dataset.return_value = dataset_mock

    # 3️⃣ Spark stub (parquet load)
    fake_df, fake_spark = MagicMock(), MagicMock()
    (
        mock_spark_cls.builder.appName.return_value
        .config.return_value.config.return_value.getOrCreate.return_value
    ) = fake_spark
    fake_spark.read.format.return_value.load.return_value = fake_df

    # 4️⃣ EDQ engine stub
    mock_engine.execute_rules.return_value = _fake_engine_result()

    # 5️⃣ run code
    runEDQ.main()

    # 6️⃣ assertions
    sess_mock.get_dataset.assert_called_once_with("CAT-123")
    mock_get_part.assert_called_once_with(dataset_mock, "2025-04-10")

    fake_spark.read.format.assert_called_once_with("parquet")
    fake_spark.read.format.return_value.load.assert_called_once_with(
        "s3a://bucket/path/2025-04-10"
    )
    mock_engine.execute_rules.assert_called_once_with(
        fake_df, "JOB_OL", "CID_OL", "CSEC_OL", "NonProd"
    )
