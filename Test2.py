

from pyspark.sql import functions as F
mapping_data = [
  (4146147380, 1001315206, 7777771001),
  (4146146886, 1004043965, 7777771002),
  (3477618751, 1004043969, 7777771003),
  (3477618760, 1004043979, 7777771004),
  (4454193610, 1004043992, 7777771005),
  (4454193717, 1004043993, 7777771006),
  (3477618797, 1004044027, 7777771007),
  (3477618801, 1004044038, 7777771008),
  (4454193181, 1004044027, 7777771009),
  (3477618821, 1004044038, 7777771010),
]
mapping_df = (spark.sparkContext.parallelize(mapping_data)
              .toDF(["customer_pk_id","new_customer_id","new_account_id"])
              .selectExpr(
                  "cast(customer_pk_id as long) customer_pk_id",
                  "cast(new_customer_id as long) new_customer_id",
                  "cast(new_account_id  as long) new_account_id"
              ))
