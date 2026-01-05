

mock_detokenize.assert_not_called()
mock_validate_detokenized_df.assert_not_called()



mock_get_args_for_job.assert_called_once()
mock_reinitialize_logger.assert_called_once()

mock_read_parquet.assert_called_once()
mock_create_s3_client.assert_called_once()

mock_detokenize.assert_not_called()
mock_validate_detokenized_df.assert_not_called()

mock_get_trade_lines.assert_called_once()

# If you returned only one bureau
self.assertEqual(mock_write_to_efg.call_count, 1)
