
import pytest
from token_util import read_parquet_based_on_date_and_runid

def test_read_parquet_based_on_date_and_runid_exception(capfd, caplog):
    # Arrange
    input_df = None  # Replace with actual DataFrame if needed
    business_date = '2024-07-03'
    run_id = 'test_run_id'
    file_type = 'parquet'

    # Temporarily modify the function to raise an exception
    def raise_exception(*args, **kwargs):
        logger.info("read based on business date and run id")
        raise Exception("Test Exception")

    # Act
    original_function = read_parquet_based_on_date_and_runid
    try:
        read_parquet_based_on_date_and_runid = raise_exception
        with caplog.at_level("INFO"):
            result = read_parquet_based_on_date_and_runid(input_df, business_date, run_id, file_type)
    except Exception as e:
        result = None
        print("Error occurred:", e)
    finally:
        read_parquet_based_on_date_and_runid = original_function

    # Assert
    out, err = capfd.readouterr()
    assert result is None
    assert "read based on business date and run id" in caplog.text
    assert "Error occurred:" in out
    assert "Test Exception" in out

if __name__ == "__main__":
    pytest.main()
