
This dataset provides consolidated account-level data for Card Small Business Financial Exchange reporting. It supports Enterprise Credit Bureau Reporting teams by preparing calculated reporting fields, account status details, suppression indicators, and validation outputs needed for downstream credit bureau submission. The dataset is used by data quality, operations, compliance, and reporting teams to review account eligibility, reporting accuracy, traceability, and audit readiness. It is specific to the Card Small Business Financial Exchange workflow and is maintained separately from temporary ingestion datasets or other credit reporting datasets.


def sbfe_fields(consolidated_df: DataFrame) -> DataFrame:
    """
    Builds the full SBFE calculated dataset by joining all SBFE field calculators
    on the standard ECBR join keys.
    """

    logger.info("Starting SBFE field calculations")

    input_count = consolidated_df.count()
    logger.info(f"Input count before deduplication: {input_count}")

    # Deduplicate once at the beginning.
    # This reduces duplicate records before applying all calculator functions.
    deduped_consolidated_df = consolidated_df.dropDuplicates(JOIN_KEYS)

    deduped_input_count = deduped_consolidated_df.count()
    logger.info(f"Input count after deduplication: {deduped_input_count}")

    join_type = "inner"

    field_calculator_functions = [
        get_account_closure_date,
        calculate_account_status_1,
        calculate_account_status_2,
        account_update_deletion_indicator,
        amount_charged_off_by_creditor,
        calculate_basis_for_account_closure,
        business_name_of_account_holder,
        date_account_was_charged_off,
        date_account_was_originally_opened,
        date_of_most_recent_payment,
        calculate_delinquency_status,
        remaining_total_balance,
        type_of_account_being_reported,
        total_charge_off_recoveries_to_date,
        total_amount_past_due,
        get_current_credit_limit,
        get_original_credit_limit,
        sbfe_country_code,
        previous_account_number,
    ]

    # Use deduped input here
    result_df = passthrough_fields(deduped_consolidated_df).dropDuplicates(JOIN_KEYS)

    for calculator_fn in field_calculator_functions:
        logger.info(
            f"Result count before applying {calculator_fn.__name__}: {result_df.count()}"
        )

        # Use deduped input for each calculator
        deduplicated_fn_output = calculator_fn(deduped_consolidated_df).dropDuplicates(JOIN_KEYS)

        logger.info(
            f"Output count when applying {calculator_fn.__name__}: "
            f"{deduplicated_fn_output.count()}"
        )

        result_df = result_df.join(
            deduplicated_fn_output,
            on=JOIN_KEYS,
            how=join_type,
        )

    result_df = result_df.dropDuplicates(JOIN_KEYS)

    logger.info(f"Final partitions: {result_df.rdd.getNumPartitions()}")
    logger.info(f"Final columns: {len(result_df.columns)}")

    return result_df
