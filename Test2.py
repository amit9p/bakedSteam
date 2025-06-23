
BaseSegment.date_of_first_delinquency: [
    None,  # id 1
    datetime(2024, 1, 13).date(),  # id 2
    datetime(2024, 1, 13).date(),  # id 3
    None,  # id 4
    datetime(2024, 1, 12).date(),  # id 5
    datetime(2024, 1, 12).date(),  # id 6
    datetime(2024, 1, 13).date(),  # id 7
    datetime(2024, 1, 12).date(),  # id 8
    datetime(2024, 1, 13).date(),  # id 9
    DEFAULT_ERROR_DATE,           # ✅ id 10 (should be 1900-01-01)
],
