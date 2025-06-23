
.when(
    CustomerInformation.bankruptcy_first_filed_date.isNotNull() &
    CCAccount.date_of_first_delinquency.isNull() &
    CCAccount.account_open_date.isNull(),
    lit(DEFAULT_ERROR_DATE)
)



BaseSegment.date_of_first_delinquency: [
    None,  # id 1
    datetime(2024, 1, 13).date(),  # id 2
    datetime(2024, 1, 13).date(),  # id 3
    None,  # id 4
    datetime(2024, 1, 12).date(),  # id 5
    datetime(2024, 1, 12).date(),  # id 6
    datetime(2024, 1, 13).date(),  # id 7
    datetime(2024, 1, 13).date(),  # id 8
    datetime(2024, 1, 13).date(),  # id 9
    DEFAULT_ERROR_DATE,  # ✅ id 10
]


_____
expected_data = create_partially_filled_dataset(
    spark,
    BaseSegment,
    data={
        BaseSegment.account_id: ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
        BaseSegment.date_of_first_delinquency: [
            None,  # id 1: still None
            datetime(year=2024, month=1, day=13).date(),  # id 2
            datetime(year=2024, month=1, day=13).date(),  # id 3
            None,  # id 4
            datetime(year=2024, month=1, day=12).date(),  # id 5
            datetime(year=2024, month=1, day=12).date(),  # id 6
            datetime(year=2024, month=1, day=13).date(),  # id 7
            datetime(year=2024, month=1, day=13).date(),  # id 8
            datetime(year=2024, month=1, day=13).date(),  # id 9
            DEFAULT_ERROR_DATE,  # id 10: invalid input scenario
        ],
    }
)
