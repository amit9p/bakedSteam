def validate_detokenized_dataframe(self, df, logger) -> int:
    """
    Validate the detokenized dataframe for embedded errors in the formatted column.

    Returns:
        int: error_count (0 if no errors)
    Raises:
        Exception: re-raises unexpected failures
    """
    try:
        # If formatted column is missing, skip validation
        if "formatted" not in df.columns:
            logger.warning("Column 'formatted' not found in dataframe. Skipping error validation.")
            return 0

        error_df = df.filter(
            upper(col("formatted")).contains("ERROR")
        )

        error_count = error_df.count()

        if error_count > 0:
            logger.error(
                f"turingError - Found {error_count} rows with ERROR in formatted column after detokenization"
            )

            # Sample errors for logging (limit)
            sample_rows = error_df.select("formatted").limit(10).collect()
            error_strings = [row["formatted"] for row in sample_rows if row and row["formatted"]]

            # unique() suggestion handled in comment #3 below
            unique_error_strings = list(dict.fromkeys(error_strings))  # preserves order
            for idx, err in enumerate(unique_error_strings, 1):
                logger.error(f"turingError - Sample error {idx}: {err}")

        return error_count

    except Exception as exc:
        logger.error(f"turingError - Error during detokenization validation: {str(exc)}")
        raise



error_count = common.validate_detokenized_dataframe(detokenized_df, logger)
if error_count > 0:
    metrics.set_detokenization_error_count(error_count)  # if you track it
    raise ValueError(
        f"Detokenization validation failed: {error_count} rows with ERROR in formatted column"
    )
