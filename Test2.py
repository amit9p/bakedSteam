


# Build a 3-column DF: (customer_pk_id, new_customer_id, new_account_id)
mapping_df = spark.sql("""
SELECT * FROM VALUES
 (4146147380L, 1001315206L, 7777771001L),
 (4146146886L, 1004043965L, 7777771002L),
 (3477618751L, 1004043969L, 7777771003L),
 (3477618760L, 1004043979L, 7777771004L),
 (4454193610L, 1004043992L, 7777771005L),
 (4454193717L, 1004043993L, 7777771006L),
 (3477618797L, 1004044027L, 7777771007L),
 (3477618801L, 1004044038L, 7777771008L),
 (4454193181L, 1004044027L, 7777771009L),
 (3477618821L, 1004044038L, 7777771010L)
AS m(customer_pk_id, new_customer_id, new_account_id)
""")



# also works in most envs
mapping_data = [
    (4146147380, 1001315206, 7777771001),
    (4146146886, 1004043965, 7777771002),
    # ...
]
mapping_df = spark.sparkContext.parallelize(mapping_data) \
    .toDF(["customer_pk_id", "new_customer_id", "new_account_id"]) \
    .select(*(F.col(c).cast("long") for c in ["customer_pk_id","new_customer_id","new_account_id"]))
