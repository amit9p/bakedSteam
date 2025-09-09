
# tests/test_sanity.py
from typedspark import create_schema, Column

# import only the classes you want to consider
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

def _looks_like_typedspark_schema(cls) -> bool:
    # must be a class and have Column[...] annotations
    if not isinstance(cls, type):
        return False
    ann = getattr(cls, "__annotations__", {})
    if not ann:
        return False
    return any(getattr(t, "__origin__", None) is Column for t in ann.values())

ALL_SCHEMAS = [cls for cls in CANDIDATES if _looks_like_typedspark_schema(cls)]

def test_build_all_schemas():
    built = 0
    for schema_cls in ALL_SCHEMAS:
        st = create_schema(schema_cls)
        assert len(st.fields) > 0
        built += 1
    assert built > 0
