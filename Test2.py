
df = spark.read.json("input.json").repartition(8)

spark = SparkSession.builder \
  .appName("MyJob") \
  .master("local[8]") \
  .config("spark.sql.shuffle.partitions", "16") \
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
  .getOrCreate()


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "16") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()


spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.sql.adaptive.enabled", "true")

print(spark.sparkContext.getConf().getAll())


.config("spark.sql.shuffle.partitions", "8") \
.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")




export HTTP_PROXY=http://username:password@chipproxy.kdc.capitalone.com:8099
export HTTPS_PROXY=http://username:password@chipproxy.kdc.capitalone.com:8099


import aiohttp

async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
    async with session.get(url) as response:
        # your logic



from pyspark.sql.types import DateType
from typespark import Column

class ABSegment(Schema):
    ...
    balloon_payment_due_date: Column[DateType]


def get_balloon_payment_due_date(ccaccount_df: DataFrame) -> DataFrame:
    """
    :param ccaccount_df: Input DataFrame
    :return: Output DataFrame with account_id and balloon_payment_due_date set to null
    """
    result_df = ccaccount_df.withColumn(
        ABSegment.balloon_payment_due_date.str,
        lit(None).cast(DateType())
    )

    return result_df.select(
        ABSegment.account_id,
        ABSegment.balloon_payment_due_date
    )



def test_get_balloon_payment_due_date(spark: SparkSession):
    ccaccount_data = create_partially_filled_dataset(
        spark,
        CCAccount,
        data=[
            {CCAccount.account_id: "1"},
            {CCAccount.account_id: "X004"}
        ],
    )

    result_df = get_balloon_payment_due_date(ccaccount_data)

    expected_data = create_partially_filled_dataset(
        spark,
        ABSegment,
        data=[
            {ABSegment.account_id: "1", ABSegment.balloon_payment_due_date: None},
            {ABSegment.account_id: "X004", ABSegment.balloon_payment_due_date: None},
        ],
    ).select(ABSegment.account_id, ABSegment.balloon_payment_due_date)

    assert_df_equality(
        result_df,
        expected_data,
        ignore_row_order=True,
        ignore_nullable=True,
    )
