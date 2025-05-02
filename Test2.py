
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("FieldMigration").getOrCreate()

# Load CSV files
account_df = spark.read.csv("input/account_dataset.csv", header=True, inferSchema=True)
recoveries_df = spark.read.csv("input/recoveries_dataset.csv", header=True, inferSchema=True)
misc_df = spark.read.csv("input/misc_dataset.csv", header=True, inferSchema=True)
ecbr_generated_df = spark.read.csv("input/eCBR_generated_fields_dataset.csv", header=True, inferSchema=True)

# Step 1: Remove 'is_account_reactivated' from account dataset
account_df_updated = account_df.drop("is_account_reactivated")

# Step 2: Add 'is_account_reactivated' to recoveries dataset (join on account_id)
if "is_account_reactivated" in account_df.columns and "account_id" in account_df.columns and "account_id" in recoveries_df.columns:
    account_reactivated_df = account_df.select("account_id", "is_account_reactivated")
    recoveries_df_updated = recoveries_df.join(account_reactivated_df, on="account_id", how="left")
else:
    recoveries_df_updated = recoveries_df

# Step 3: Remove 'has_financial_liability' from misc dataset
misc_df_updated = misc_df.drop("has_financial_liability")

# Step 4: Add 'has_financial_liability' to eCBR_generated_fields dataset (join on account_id)
if "has_financial_liability" in misc_df.columns and "account_id" in misc_df.columns and "account_id" in ecbr_generated_df.columns:
    financial_liability_df = misc_df.select("account_id", "has_financial_liability")
    ecbr_generated_df_updated = ecbr_generated_df.join(financial_liability_df, on="account_id", how="left")
else:
    ecbr_generated_df_updated = ecbr_generated_df

# Optional: Save results
account_df_updated.write.csv("output/account_dataset_updated", header=True, mode='overwrite')
recoveries_df_updated.write.csv("output/recoveries_dataset_updated", header=True, mode='overwrite')
misc_df_updated.write.csv("output/misc_dataset_updated", header=True, mode='overwrite')
ecbr_generated_df_updated.write.csv("output/ecbr_generated_fields_dataset_updated", header=True, mode='overwrite')
