
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
