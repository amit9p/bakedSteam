
# List of tuples with column names and their aliases
columns_with_aliases = [
    ("enriched_df.business_date", "business_date"),
    ("enriched_df.run_identifier", "run_identifier"),
    ("enriched_df.output_file_type", "output_file_type"),
    ("enriched_df.output_record_sequence", "output_record_sequence"),
    ("enriched_df.output_field_sequence", "output_field_sequence"),
    ("enriched_df.attribute", "attribute"),
    ("final_formatted", "formatted"),
    ("enriched_df.tokenization", "tokenization"),
    ("enriched_df.account_number", "account_number"),
    ("enriched_df.segment", "segment")
]

# Use list comprehension to dynamically select and alias columns
df_final = df_final.select(*[col(c[0]).alias(c[1]) for c in columns_with_aliases])
