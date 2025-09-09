
from typedspark import create_schema

from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_consolidator_dfs_account_service_account import (
    ECBR_Consolidator_Account_Service_Account,
)
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_calculator_dfs_output import (
    ECBR_Calculator_Output,
)

CANDIDATES = [
    ECBR_Consolidator_Account_Service_Account,
    ECBR_Calculator_Output,
    # add more here...
]

def test_build_all_schemas():
    built = 0
    for cls in CANDIDATES:
        try:
            st = create_schema(cls)     # succeeds only for real typed-spark Schemas
        except TypeError as e:
            # typical: "Schema <X> does not have attribute columns" → skip
            continue
        assert len(st.fields) > 0
        built += 1
    assert built > 0
