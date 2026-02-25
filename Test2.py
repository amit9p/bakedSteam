
def passthrough_field(
    self,
    df: DataFrame,
    account_id_column: str,
    customer_id_column: str,
    target_field_name: str,
    *,
    source_field_name: Optional[str] = None,
    hardcoded_value: Optional[Union[str, int, float, bool, date]] = None,
    date_format_str: Optional[str] = None,
) -> DataFrame:

    if source_field_name and hardcoded_value is not None:
        raise ValueError(
            "Provide either source_field_name OR hardcoded_value, not both"
        )

    # -----------------------
    # Build target column
    # -----------------------
    if source_field_name:
        value_col = col(source_field_name)

        if date_format_str:
            value_col = date_format(value_col, date_format_str)

    elif hardcoded_value is not None:
        value_col = lit(hardcoded_value)

    else:
        raise ValueError(
            "Either source_field_name or hardcoded_value must be provided"
        )

    # -----------------------
    # FINAL projection (single exit)
    # -----------------------
    return df.select(
        col(account_id_column).alias("account_id"),
        col(customer_id_column).alias("customer_id"),
        value_col.alias(target_field_name),
    )


from typing import Optional, Union
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, date_format


class DataFramePassthrough:
    def passthrough_field(
        self,
        df: DataFrame,
        account_id_column: str,
        customer_id_column: str,
        target_field_name: str,
        *,
        source_field_name: Optional[str] = None,
        hardcoded_value: Optional[Union[str, int, float, bool, date]] = None,
        date_format_str: Optional[str] = None,
    ) -> DataFrame:
        """
        Generic passthrough utility.

        - Rename a source column → target column
        - OR assign a hardcoded value
        - Optionally format date/timestamp columns into string (e.g. ddMMyyyy)
        """

        if source_field_name and hardcoded_value is not None:
            raise ValueError(
                "Provide either source_field_name OR hardcoded_value, not both"
            )

        # -------------------------
        # Build target column logic
        # -------------------------
        if source_field_name:
            value_col = col(source_field_name)

            # Optional date formatting
            if date_format_str:
                value_col = date_format(value_col, date_format_str)

        elif hardcoded_value is not None:
            value_col = lit(hardcoded_value)

        else:
            raise ValueError(
                "Either source_field_name or hardcoded_value must be provided"
            )

        # -------------------------
        # Final projection
        # -------------------------
        return df.select(
            col(account_id_column).alias("account_id"),
            col(customer_id_column).alias("customer_id"),
            value_col.alias(target_field_name),
        )


def closed_date(ecbr_accounts_primary_df: DataFrame) -> DataFrame:
    return DataFramePassthrough().passthrough_field(
        df=ecbr_accounts_primary_df,
        account_id_column=ECBRCardDFSAccountsPrimary.account_id.str,
        customer_id_column=ECBRCardDFSAccountsPrimary.customer_id.str,
        target_field_name=EcbrCalculatorOutput.closed_date.str,
        source_field_name=ECBRCardDFSAccountsPrimary.account_closed_date.str,
        date_format_str="ddMMyyyy",   # ← ONLY change needed
    )
