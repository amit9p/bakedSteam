
Using coalesce(1) instead of collect() will indeed reduce the memory footprint on the driver by limiting the number of partitions to one. However, it may not fully resolve the out-of-memory issue since coalesce(1) only reduces the number of partitions on the executor side, and bringing all the data to the driver as a single partition can still cause memory overload if the data size is large.

Here’s an alternative approach to avoid collecting all data at once on the driver:

1. Write each partition to a temporary file in S3 (or a local storage if that’s an option).


2. Once all partitions are written, concatenate the files from S3 to form the final output string.


3. Clean up temporary files after concatenation.
