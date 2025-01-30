
"""
Given an input DataFrame with columns:
- credit_limit (string/int)
- product_type (string)
- account_id (unique identifier)

Returns a DataFrame with:
- account_id
- account_type

Logic:
1) If product_type == "private_label_partnership" & credit_limit == "NPSL" → None
2) If credit_limit == "NPSL" → "0G"
3) If product_type == "small business" & credit_limit is numeric → "8A"
4) If product_type == "Private Label Partnership" & credit_limit is numeric → "07"
5) Else → "1B"
"""
