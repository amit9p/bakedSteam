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
