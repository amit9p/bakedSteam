
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
