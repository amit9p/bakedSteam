
from typedspark import create_schema, Schema, Column

# import your actual schema classes
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_consolidator_dfs_account_service_account import (
    ECBR_Consolidator_Account_Service_Account,
)
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_calculator_dfs_output import (
    ECBR_Calculator_Output,
)

CANDIDATES = [
    ECBR_Consolidator_Account_Service_Account,
    ECBR_Calculator_Output,
]

def _is_typedspark_schema(cls) -> bool:
    if not (isinstance(cls, type) and issubclass(cls, Schema) and cls is not Schema):
        return False
    ann = getattr(cls, "__annotations__", {})
    return any(getattr(t, "__origin__", None) is Column for t in ann.values())

ALL_SCHEMAS = [cls for cls in CANDIDATES if _is_typedspark_schema(cls)]

def test_build_all_schemas():
    for schema_cls in ALL_SCHEMAS:
        st = create_schema(schema_cls)
        assert len(st.fields) > 0
