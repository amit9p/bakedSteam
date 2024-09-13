
1. Copy Lambda: Copies data from S3 Bucket 1 to S3 Bucket 2 and drops a _SUCCESS file.


2. S3 Event Notification: Detects the creation of the _SUCCESS file in S3 Bucket 2.


3. Trigger Lambda: Invoked by the S3 event, this Lambda triggers the Glue job.


4. DLQ: Configured for the Trigger Lambda to handle any failed Glue job invocations.
