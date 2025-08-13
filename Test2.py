
^\+?[1-9]\d{1,14}$               # after removing all non-digits except leading +

Thanks, Tyler — yes, CSV can store "null" text and escape JSON for nested data, but those are workarounds rather than native type support.

In CSV, all fields are still strings, and parsing null or JSON requires extra logic in every read.

Parquet stores real NULLs, schema, and nested structures natively — no custom parsing needed.


_____
Here’s a brief but solid reasoning why Parquet is generally better than CSV for most analytical workloads — especially when dealing with NULLs and complex types like structs:


---

1️⃣ Null Handling

CSV: No native NULL type — missing values are just empty text or special markers (NA, null), which are parser-dependent. This causes ambiguity when reading the file into different systems.

Parquet: Stores NULLs explicitly in the file’s schema and encodes them efficiently with definition levels. Any system reading Parquet knows exactly which values are NULL without guessing.



---

2️⃣ Data Types & Schema

CSV: Everything is text. The schema has to be inferred or provided at read time. This means 123 might be read as a string in one system, an integer in another.

Parquet: Schema is stored with the file (column names, types, nullability). Ensures type consistency across reads without manual definitions.



---

3️⃣ Support for Complex Types

CSV: Flat text only — no native support for arrays, maps, or nested structures. Complex data must be flattened or serialized as JSON strings, which hurts query performance.

Parquet: Natively supports struct, array, and map types. Nested data can be stored and queried directly without conversion.



---

4️⃣ Performance

CSV: Row-based → Reads all columns even if you query only one; slower and larger on disk.

Parquet: Columnar → Reads only needed columns, compressed per column, much smaller storage footprint and faster I/O.



---

✅ In short: Parquet is better because it:

Stores real NULLs without ambiguity.

Preserves data types and schema.

Handles structs and nested data natively.

Is columnar & compressed, making it more efficient for analytics.





from pyspark.sql import functions as F

orig_cols = df1.columns  # preserves df1's column order

df2_k = (df2.select(
            F.col("consumer_account_number").cast(dict(df1.dtypes)["consumer_account_number"]).alias("consumer_account_number"),
            F.col("identification_number").alias("id_new"))
        )

df_updated = (
    df1.alias("l")
      .join(F.broadcast(df2_k).alias("r"), "consumer_account_number", "left")
      .select(*[
          # keep df1 order; replace only this column
          F.coalesce(F.col("r.id_new"), F.col("l.identification_number")).alias("identification_number")
          if c == "identification_number" else F.col(f"l.{c}")
          for c in orig_cols
      ])
)

# df_updated now has the SAME column order as df1, with identification_number in its original position.
