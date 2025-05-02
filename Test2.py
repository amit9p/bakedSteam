
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FieldReallocation").getOrCreate()

# Load datasets
account_df = spark.read.csv("input/account_dataset.csv", header=True, inferSchema=True)
recoveries_df = spark.read.csv("input/recoveries_dataset.csv", header=True, inferSchema=True)
misc_df = spark.read.csv("input/misc_dataset.csv", header=True, inferSchema=True)
ecbr_generated_df = spark.read.csv("input/eCBR_generated_fields_dataset.csv", header=True, inferSchema=True)

# Step 1: Remove 'is_account_reactivated' from account
account_df_updated = account_df.drop('is_account_reactivated')

# Step 2: Add 'is_account_reactivated' to recoveries
if 'is_account_reactivated' in account_df.columns:
    is_account_reactivated_col = account_df.select('is_account_reactivated')
    recoveries_df_updated = recoveries_df.withColumn('is_account_reactivated', is_account_reactivated_col['is_account_reactivated'])
else:
    recoveries_df_updated = recoveries_df

# Step 3: Remove 'has_financial_liability' from misc
misc_df_updated = misc_df.drop('has_financial_liability')

# Step 4: Add 'has_financial_liability' to ECBRGeneratedFields
if 'has_financial_liability' in misc_df.columns:
    has_financial_liability_col = misc_df.select('has_financial_liability')
    ecbr_generated_df_updated = ecbr_generated_df.withColumn('has_financial_liability', has_financial_liability_col['has_financial_liability'])
else:
    ecbr_generated_df_updated = ecbr_generated_df

# Save updated DataFrames (optional)
account_df_updated.write.csv("output/account_dataset_updated", header=True, mode='overwrite')
recoveries_df_updated.write.csv("output/recoveries_dataset_updated", header=True, mode='overwrite')
misc_df_updated.write.csv("output/misc_dataset_updated", header=True, mode='overwrite')
ecbr_generated_df_updated.write.csv("output/ecbr_generated_fields_updated", header=True, mode='overwrite')
