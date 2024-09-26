Feature: Non-critical warning for Glue job exceeding expected execution time

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Glue job execution time exceeds 25 minutes
    Given the Glue job "assembler_etl" has started processing
    When the Glue job execution time exceeds 25 minutes
    Then log a non-critical warning that the job is taking longer than expected
    And I should see the warning details logged in CloudWatch
    And the Jira X-Ray ticket should be created with a "Non-Critical" severity level


Feature: Critical failure in reading data from S3

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Failure to read input data from S3
    Given I have an input file in S3 at "<input_s3_path>"
    When the Glue job "assembler_etl" runs
    Then the job should attempt to read the input data from "<input_s3_path>"
    And if the data cannot be read due to missing or corrupted files, raise a critical alert
    And I should see the error details logged in CloudWatch
    And the Jira X-Ray ticket should be created with a "Critical" severity level



    Feature: Critical failure in processing trade lines

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Failure in processing trade lines using get_trade_lines
    Given the Glue job successfully reads data from S3
    When the Glue job "assembler_etl" processes the data using the "detokenize" and "get_trade_lines" functions
    Then if the "detokenize" or "get_trade_lines" function fails, raise a critical alert
    And I should see the error details logged in CloudWatch
    And the Jira X-Ray ticket should be created with a "Critical" severity level


Feature: Critical failure in writing data to S3

  Background:
    Given the Glue job "assembler_etl" is successfully deployed

  Scenario: Failure to write output data to S3
    Given the Glue job "assembler_etl" has successfully processed the data
    When the Glue job "assembler_etl" attempts to write the output data to the destination S3 bucket
    Then if writing to S3 fails due to permissions or connection issues, raise a critical alert
    And I should see the error details logged in CloudWatch
    And the Jira X-Ray ticket should be created with a "Critical" severity level
    


#######
If you have a large string in your code that you need to write to S3, there are several ways you can accelerate the process. Here are some techniques and strategies to optimize the upload of large strings to S3 using boto3:

1. Use Multipart Upload

Multipart Upload allows you to upload large files (or strings) by breaking them into smaller parts and uploading them in parallel. This method is especially useful for large data, as it accelerates the upload by using multiple threads to handle different parts of the data concurrently.

You can use multipart uploads for strings by converting the string into an in-memory byte stream and uploading it in parts.


Implementation:

import boto3
import io

s3_client = boto3.client('s3')

# Convert the string to bytes
large_string = "your_very_large_string_here"
bytes_data = io.BytesIO(large_string.encode('utf-8'))

# Initiate a multipart upload
multipart_upload = s3_client.create_multipart_upload(Bucket='your-bucket', Key='your-key')

# Upload in chunks (example: 10 MB per part)
part_size = 10 * 1024 * 1024  # 10 MB
parts = []
part_number = 1

while bytes_data.tell() < len(large_string.encode('utf-8')):
    part_data = bytes_data.read(part_size)
    response = s3_client.upload_part(
        Body=part_data,
        Bucket='your-bucket',
        Key='your-key',
        UploadId=multipart_upload['UploadId'],
        PartNumber=part_number
    )
    parts.append({
        'PartNumber': part_number,
        'ETag': response['ETag']
    })
    part_number += 1

# Complete the multipart upload
s3_client.complete_multipart_upload(
    Bucket='your-bucket',
    Key='your-key',
    UploadId=multipart_upload['UploadId'],
    MultipartUpload={'Parts': parts}
)

Benefit: Multipart uploads allow large strings to be uploaded in parallel, reducing the overall time to upload large data.

2. Use upload_fileobj() with a Byte Stream

If you can convert the string to a byte stream, you can use upload_fileobj() to upload it directly to S3. This is a simple approach but still allows you to take advantage of streaming to S3 without having to write a temporary file.

Implementation:

import io
import boto3

s3_client = boto3.client('s3')

large_string = "your_very_large_string_here"
bytes_data = io.BytesIO(large_string.encode('utf-8'))

# Upload the byte stream directly to S3
s3_client.upload_fileobj(bytes_data, 'your-bucket', 'your-key')

Benefit: Avoids the need to write the string to a file on disk and uploads it directly from memory.

3. Enable Transfer Acceleration

S3 Transfer Acceleration uses Amazon's global network of edge locations to upload data to the nearest AWS location, improving upload speeds, especially when the data is being uploaded from a geographically distant region.


Implementation:

s3_client = boto3.client('s3', config=boto3.session.Config(s3={'use_accelerate_endpoint': True}))
s3_client.put_object(Bucket='your-bucket', Key='your-key', Body=large_string)

Benefit: Transfer Acceleration is particularly useful for large data transfers over long distances, reducing latency and improving performance.

4. Use Threading for Parallel Uploads

You can speed up writing a large string to S3 by splitting it into chunks and uploading each chunk in parallel using threads.

Implementation:

import boto3
import threading

s3_client = boto3.client('s3')

def upload_part(data, bucket, key):
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)

large_string = "your_very_large_string_here"
chunk_size = 5 * 1024 * 1024  # 5 MB chunks
threads = []

for i in range(0, len(large_string), chunk_size):
    part = large_string[i:i+chunk_size]
    thread = threading.Thread(target=upload_part, args=(part, 'your-bucket', f'your-key-part-{i//chunk_size}'))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

Benefit: By splitting the large string and uploading each part in parallel, you can fully utilize network bandwidth and reduce upload time.

5. Use Compression (Optional)

Compressing the string before uploading can reduce the size of the data to be uploaded, speeding up the process, especially if the string is highly compressible (e.g., JSON or text data).

Implementation:

import gzip
import boto3
import io

s3_client = boto3.client('s3')

large_string = "your_very_large_string_here"
compressed_data = io.BytesIO()

# Compress the string using gzip
with gzip.GzipFile(fileobj=compressed_data, mode='w') as gz:
    gz.write(large_string.encode('utf-8'))

compressed_data.seek(0)  # Reset the buffer to the beginning

# Upload compressed data
s3_client.upload_fileobj(compressed_data, 'your-bucket', 'your-key.gz')

Benefit: Compression reduces the amount of data transferred to S3, speeding up the upload, especially for text-based data.

6. Optimize for Small Files (if Applicable)

If you're splitting the string into many small parts (e.g., smaller than 100 MB), the overhead from individual S3 upload requests may become significant. In such cases:

Batch your uploads: Group small parts together.

Use parallel uploads: Upload multiple parts simultaneously using threads or multiprocessing, as described above.


7. Tune S3 Write Configuration (Similar to S3 Read Tuning)

You can configure the S3 connection and thread limits in the same way as reading from S3 to optimize upload speed.

Implementation:

from boto3.s3.transfer import TransferConfig

config = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,  # Trigger multipart uploads for files > 5MB
    multipart_chunksize=5 * 1024 * 1024,  # Set each part to 5MB
    use_threads=True,
    max_concurrency=10  # Control the level of parallelism
)

s3_client.upload_fileobj(bytes_data, 'your-bucket', 'your-key', Config=config)

Benefit: Using the TransferConfig with optimized concurrency and chunk size ensures efficient uploading for large strings.

Summary of Key Techniques:

Multipart Upload: For large strings or files, breaking them into parts and uploading in parallel.

S3 Transfer Acceleration: Use AWS edge locations to speed up data transfer.

Parallelism with Threads: Upload parts of the string in parallel using threads or asynchronous programming.

Compression: Compress the string to reduce the size of data uploaded.

Direct Upload with Streams: Use in-memory byte streams to avoid file writes.


By applying these strategies, you can significantly reduce the time it takes to write large strings to S3 using boto3.

