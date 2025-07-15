
@patch("builtins.open", new_callable=mock_open)
@patch("boto3.Session")
@patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
@patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
@patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
def test_main_s3_branch(
    mock_logger, mock_exec_rules, mock_spark_cls, mock_boto_sess, mock_open_file
):
    # … your YAML stubbing, boto3 stubbing, spark‐builder stubbing …

    fake_spark   = MagicMock(name="spark_s3")
    fake_builder = MagicMock(name="builder_s3")
    mock_spark_cls.builder = fake_builder
    fake_builder.appName.return_value.getOrCreate.return_value = fake_spark

    # D) CSV reader stub
    fake_reader = MagicMock(name="reader_s3")
    fake_reader.format.return_value = fake_reader
    fake_reader.option.return_value = fake_reader
    fake_reader.load.return_value   = "DF_S3"

+   # —— attach it here ——
+   fake_spark.read = fake_reader

    # E) engine stub …

    import importlib
    import ecbr_card_self_service.edq.local_run.runEDQ as runEDQ
    importlib.reload(runEDQ)

    runEDQ.main()

    # now these will pass:
    fake_reader.format.assert_called_once_with("csv")
    fake_reader.option.assert_called_once_with("header", "true")
    fake_reader.load.assert_called_once_with("s3://bucket/base_segment.csv")
    mock_exec_rules.assert_called_once_with("DF_S3", "JID_S3", "CID_S3", "CSEC_S3", "S3")
    fake_rows.show.assert_called_once_with(0, truncate=False)
