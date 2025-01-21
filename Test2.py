
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.utils import AnalysisException

def calculate_current_balance(input_df: DataFrame) -> DataFrame:
    try:
        # Validate required columns
        required_columns = [
            "PIF Notification", "SIF Notification", "Asset Sales Notification",
            "Charge Off Reason Code", "Current Balance of the Account",
            "Bankruptcy Status", "Bankruptcy Chapter"
        ]
        
        # Check if all required columns are present
        for col_name in required_columns:
            if col_name not in input_df.columns:
                raise KeyError(f"Missing required column: {col_name}")
        
        # Adding the logic for calculating the current balance
        calculated_df = input_df.withColumn(
            "Calculated Current Balance",
            when(
                col("PIF Notification") |
                col("SIF Notification") |
                col("Asset Sales Notification") |
                (col("Charge Off Reason Code") == "STL") |
                (col("Current Balance of the Account") < 0)
                , 0
            )
            .when(
                (col("Bankruptcy Status") == "Open") &
                (col("Bankruptcy Chapter") == "BANKRUPTCY_CHAPTER_13"), 0
            )
            .when(col("Bankruptcy Status") == "Discharged", 0)
            .otherwise(col("Current Balance of the Account"))
        )

        return calculated_df.select("account_id", "Calculated Current Balance")

    except KeyError as e:
        print(f"KeyError: {e}")
        raise

    except AnalysisException as e:
        print(f"AnalysisException: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
