
def test_generic_exception():
    # Pass a completely invalid object instead of a DataFrame to trigger a generic exception
    invalid_input = "This is not a DataFrame"

    # Use pytest to assert that a generic exception is raised
    with pytest.raises(Exception) as exc_info:
        calculate_current_balance(invalid_input)  # This should raise a generic exception
    
    # Assert the exception message contains the expected generic error
    assert "unexpected error occurred" in str(exc_info.value).lower()
    print("Generic exception test case passed!")
