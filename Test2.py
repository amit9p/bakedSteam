print("consolidated_data type:", type(consolidated_data))
print("unified_data type:", type(unified_data))

print("first consolidated record type:", type(consolidated_data[0]))
print("first unified record type:", type(unified_data[0]))

print("first consolidated record:", consolidated_data[0])
print("first unified record:", unified_data[0])

print("consolidated key types:", {type(k) for k in consolidated_data[0].keys()})
print("unified key types:", {type(k) for k in unified_data[0].keys()})

print("consolidated value types:", {type(v) for v in consolidated_data[0].values()})
print("unified value types:", {type(v) for v in unified_data[0].values()})

______________
print("consolidated schema type:", type(consolidated_schema))
print("calculated schema type:", type(calculated_schema))
print("consolidated structtype:", consolidated_schema.get_structtype())
print("calculated structtype:", calculated_schema.get_structtype())


print("Spark alive:", context.spark.sparkContext._jsc is not None)
print("Spark type:", type(context.spark))

context.consolidated_df = context.spark.createDataFrame(...)



from pyspark.sql import SparkSession

def before_all(context):
    context.spark = (
        SparkSession.builder
        .appName("Reportable Accounts Component Test")
        .master("local[1]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    context.spark.sparkContext.setLogLevel("ERROR")


def before_scenario(context, scenario):
    # Only reset data, NOT Spark
    context.error_raised = None
    context.result = None
    context.consolidated_df = None
    context.calculated_df = None

    # Clear cache safely
    context.spark.catalog.clearCache()


def after_all(context):
    if hasattr(context, "spark") and context.spark is not None:
        try:
            context.spark.stop()
        except:
            pass
