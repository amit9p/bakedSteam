
from pyspark.sql.types import StructType, StructField, StringType, LongType
from ecbr_logging import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Declare string variables at the top
ACCOUNT_ID = 'account_id'
RUN_ID = 'run_id'
SEGMENT = 'segment'
ATTRIBUTE = 'attribute'
VALUE = 'value'
ROW_SEQUENCE = 'row_sequence'
COLUMN_SEQUENCE = 'column_sequence'
SITE_TYPE = 'site_type'
BUSINESS_DATE = 'business_date'
INDEX_LEVEL = '__index_level_0__'
TOKENIZATION_TYPE = 'tokenization_type'

def read_parquet_file(spark, path):
    try:
        schema = StructType([
            StructField(ACCOUNT_ID, StringType(), True),
            StructField(RUN_ID, StringType(), True),
            StructField(SEGMENT, StringType(), True),
            StructField(ATTRIBUTE, StringType(), True),
            StructField(VALUE, StringType(), True),
            StructField(ROW_SEQUENCE, LongType(), True),
            StructField(COLUMN_SEQUENCE, LongType(), True),
            StructField(SITE_TYPE, StringType(), True),
            StructField(BUSINESS_DATE, StringType(), True),
            StructField(INDEX_LEVEL, LongType(), True),
            StructField(TOKENIZATION_TYPE, StringType(), True),
        ])
        df = spark.read.schema(schema).parquet(path)
        df.printSchema()
        return df
    except Exception as e:
        logger.error(e)
