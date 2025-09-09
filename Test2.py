
# tests/test_sanity.py
from typedspark import create_schema, Schema, Column

# ---- import ONLY schema classes here ----
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_consolidator_dfs_account_service_account import (
    ECBR_Consolidator_Account_Service_Account,
)
from ecbr_tenant_card_dfs_l1_self_service.schemas.ecbr_calculator_dfs_output import (
    ECBR_Calculator_Output,
)
# add more schema classes as needed ...

CANDIDATES = [
    ECBR_Consolidator_Account_Service_Account,
    ECBR_Calculator_Output,
    # add others here...
]

def _is_typedspark_schema(cls) -> bool:
    """True only for classes that are actual typed-spark Schemas with Column[...] annotations."""
    if not (isinstance(cls, type) and issubclass(cls, Schema) and cls is not Schema):
        return False
    ann = getattr(cls, "__annotations__", {})
    return any(getattr(t, "__origin__", None) is Column for t in ann.values())

ALL_SCHEMAS = [cls for cls in CANDIDATES if _is_typedspark_schema(cls)]

def test_build_all_schemas():
    built = 0
    for schema_cls in ALL_SCHEMAS:
        st = create_schema(schema_cls)
        assert len(st.fields) > 0
        built += 1
    assert built > 0



[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"

[project]
name = "ecbr-tenant-card-dfs-l1-self-service"
version = "0.0.1"
packages = ["ecbr_tenant_card_dfs_l1_self_service"]











[pytest]
pythonpath = .


from typedspark import create_schema

# ---- Import your schema classes (list them here) ----
from schemas.credit_bureau_account import Credit_Bureau_Account
from schemas.ecbr_calculator_dfs_output import ECBR_Calculator_Output
# from schemas.credit_bureau_customer import Credit_Bureau_Customer
# from schemas.reporting_override import Reporting_Override
# ...add other schema classes as you create them

# Put all schema classes in one list
ALL_SCHEMAS = [
    Credit_Bureau_Account,
    ECBR_Calculator_Output,
    # Credit_Bureau_Customer,
    # Reporting_Override,
]

def test_build_all_schemas():
    # If this runs, imports worked and schemas are exercised
    for schema_cls in ALL_SCHEMAS:
        st = create_schema(schema_cls)
        assert len(st.fields) > 0




import pytest

def test_basic_math():
    # simple sanity test
    assert 2 + 2 == 4

def test_string_check():
    # check string content
    name = "Mahindra"
    assert "Mahi" in name

@pytest.mark.parametrize("a,b,expected", [
    (2, 3, 5),
    (10, 5, 15),
    (-1, 1, 0),
])
def test_parametrized_addition(a, b, expected):
    # parametric example
    assert a + b == expected
