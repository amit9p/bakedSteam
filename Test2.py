
rows = pivot_df.select(concat_ws(field_separator, *pivot_df.columns).alias("metro2_string"))
final_output_string = record_separator.join([row.metro2_string for row in rows.take(1000)])
logger.debug(f"Final output string has {len(final_output_string)} characters")
return final_output_string
