
def test_portfolio_o_zero_filled(spark_session):
    # If portfolio_type is 'O', the calculated credit limit should be '0'.
    data = [
        ("A001", "O", "500"),
        ("A002", "O", "999"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": "0"},
                {"account_id": "A002", "calculated_credit_limit": "0"}]

    assert set([row.asDict() for row in result]) == set(expected)


def test_portfolio_r_numeric_limit(spark_session):
    # If portfolio_type is 'R' and assigned_credit_limit is numeric,
    # we should round to the nearest whole dollar.
    data = [
        ("A001", "R", "129.2"),
        ("A002", "R", "456.7"),
        ("A003", "R", "123.5"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": "129"},
                {"account_id": "A002", "calculated_credit_limit": "457"},
                {"account_id": "A003", "calculated_credit_limit": "124"}]

    assert set([row.asDict() for row in result]) == set(expected)


def test_portfolio_r_npsl(spark_session):
    # If portfolio_type is 'R' and assigned_credit_limit is 'NPSL', the result should remain 'NPSL'.
    data = [
        ("A001", "R", "NPSL"),
        ("A002", "R", "npsl"),  # Case sensitivity test
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": "NPSL"},
                {"account_id": "A002", "calculated_credit_limit": None}]  # Case-sensitive mismatch

    assert set([row.asDict() for row in result]) == set(expected)


def test_unrecognized_portfolio_type(spark_session):
    # If portfolio_type is not 'O' or 'R', we expect None for calculated_credit_limit.
    data = [
        ("A001", "X", "100"),
        ("A002", "Y", "200"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": None},
                {"account_id": "A002", "calculated_credit_limit": None}]

    assert set([row.asDict() for row in result]) == set(expected)


def test_invalid_assigned_value(spark_session):
    # If assigned_credit_limit is not numeric or 'NPSL', it casts to float => fails => returns None.
    data = [
        ("A001", "R", "abc"),
        ("A002", "R", "---"),
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": None},
                {"account_id": "A002", "calculated_credit_limit": None}]

    assert set([row.asDict() for row in result]) == set(expected)


def test_mixed_values(spark_session):
    # Test a single DataFrame with a variety of scenarios.
    data = [
        ("A001", "O", "500"),   # Should result in '0'
        ("A002", "R", "129.5"), # Should round to 130
        ("A003", "R", "NPSL"),  # Should remain 'NPSL'
        ("A004", "X", "100"),   # Unrecognized portfolio_type => None
        ("A005", "R", "invalid"),  # Invalid assigned_credit_limit => None
    ]
    cols = ["account_id", "portfolio_type", "assigned_credit_limit"]
    df_in = spark_session.createDataFrame(data, cols)

    result = calculate_credit_limit_spark(df_in).select("account_id", "calculated_credit_limit").collect()
    expected = [{"account_id": "A001", "calculated_credit_limit": "0"},
                {"account_id": "A002", "calculated_credit_limit": "130"},
                {"account_id": "A003", "calculated_credit_limit": "NPSL"},
                {"account_id": "A004", "calculated_credit_limit": None},
                {"account_id": "A005", "calculated_credit_limit": None}]

    assert set([row.asDict() for row in result]) == set(expected)
