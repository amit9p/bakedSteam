
from pyspark.sql import functions as F, types as T

int32_cols = ["account_id", "instnc_id"]  # add any others that must be 32-bit
df = df.withColumns({c: F.col(c).cast(T.IntegerType()) for c in int32_cols})

nc -vz <phone_ip> 8080       # should say 'succeeded'
curl -I --proxy http://<phone_ip>:8080 http://example.com
curl -I --proxy http://<phone_ip>:8080 https://example.com






Your current names vs. fixed names

Current name	Suggested name

ECBR_Calculator_Output	EcbrCalculatorOutput
ECBR_Consolidator_Account_Service_Account	EcbrConsolidatorAccountServiceAccount
ECBR_Consolidator_Account_Service_Address	EcbrConsolidatorAccountServiceAddress
ECBR_Consolidator_Account_Service_Customer	EcbrConsolidatorAccountServiceCustomer
ECBR_Consolidator_Credit_Bureau_Account_Dm_Os	EcbrConsolidatorCreditBureauAccountDmOs
ECBR_Consolidator_Customer_Dm_Os	EcbrConsolidatorCustomerDmOs
ECBR_Consolidator_Service_Collector_Configuration	EcbrConsolidatorServiceCollectorConfiguration
ECBR_Consolidator_Trxn_Service_Full_Extract	EcbrConsolidatorTrxnServiceFullExtract



---

How to apply changes safely

1. Rename class definitions in your schema files (change the class keyword).
Example:

# before
class ECBR_Calculator_Output(Schema):
    ...

# after
class EcbrCalculatorOutput(Schema):
    ...


2. Update all imports across your repo (tests, consolidators, etc.).
Example:

from schemas.ecbr_calculator_dfs_output import EcbrCalculatorOutput


3. Run tests again (pipenv run pytest -q) to confirm everything still works.








import pytest
from typedspark import create_schema

from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_consolidator_dfs_account_service_account import (
    ECBR_Consolidator_Account_Service_Account,
)
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_calculator_dfs_output import (
    ECBR_Calculator_Output,
)

CANDIDATES = [
    ECBR_Consolidator_Account_Service_Account,
    ECBR_Calculator_Dfs_Output,   # add others as needed
]

def test_build_all_schemas():
    built = 0
    failures = {}
    for cls in CANDIDATES:
        try:
            create_schema(cls)
            built += 1
        except Exception as e:
            failures[cls.__name__] = str(e)

    if built == 0:
        pytest.skip(f"No typed-spark schemas built; failures={failures}")
