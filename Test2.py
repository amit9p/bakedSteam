
return final_df.select([F.col(c) for c in EcbrCalculatorOutput.__annotations__.keys()])

calc_cols = list(Calc.__annotations__.keys())
assert_df_equality(
    actual_df.select(*calc_cols),
    expected_df.select(*calc_cols),
    ignore_row_order=True,
    ignore_column_order=True,
)

calc_cols = list(CalcSchema.__annotations__.keys())


