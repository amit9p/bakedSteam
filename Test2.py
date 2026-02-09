




Hi team — quick confirmation needed.
In QA reporting config we see 7 datasets, but in OneLake only joiner_output is getting written.
j2_customer and primary_customer look like intermediate calculator outputs, and we don’t see Glue jobs triggered or data present for them under the unpartitioned/calculator_outputs path in S3.
Can you confirm if only joiner_output is supported for OneLake reporting, and intermediate calculator outputs are not expected to be materialized?
Thanks!

spark = (
    SparkSession.builder
    .appName("testing")
    .config(
        "spark.jars",
        ",".join([
            "/opt/spark/jars/hadoop-aws-3.3.3.jar",
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.481.jar",
            "/opt/spark/jars/onelake-s3-client_hadoop-3.3.3_2.12-2.12.2.jar"
        ])
    )
    .getOrCreate()
)





yarn add-container \
  -g cof-sandbox/credit_credit-bureau-report#feature/j2-container-fix \
  --domain tutorial \
  --container example


yarn add container https://github.com/your-org/empath-containers.git#feature/my-container-setupThis dataset produces a calculated account-level summary for each reportable United States Card account and its associated Joint Account Holder (J2 customer). It applies configured Line of Business rules and suppression criteria to determine which accounts are eligible for credit bureau reporting and ensures that only one finalized record is created per account. The output is structured to support Metro 2 segment field population and related downstream reporting formats.
The dataset is primarily used by credit bureau reporting teams, credit risk partners, data engineering teams, and audit stakeholders who require a reliable and repeatable view of calculated account and customer attributes. It supports operational reporting, reconciliation activities, monitoring, and validation of bureau-ready outputs prior to submission. The data enables consumers to trace how reporting values were derived for compliance and accuracy checks.
Source data is obtained from United States Card systems of record, including account servicing platforms and joint customer reference data. The data is processed through Enterprise Credit Bureau Reporting transformation workflows, where records are validated, standardized, and aligned to Credit Reporting Resource Guide requirements before final delivery. These steps ensure consistency and readiness for external bureau submission.
