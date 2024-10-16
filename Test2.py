
try:
    response = glue_client.start_job_run(
        JobName=glue_job,
        Arguments={
            '--input_s3_path': path,
            '--env': environment,
            '--output_s3_path': output_s3_path
        }
    )
except Exception as e:
    logger.exception("Error starting Glue job")
    raise
