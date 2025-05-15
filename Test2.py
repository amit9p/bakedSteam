

1. "int for amount related fields I guess!"

Handled:
Yes. In your schema (available_spending_amount), the field is expected to be IntegerType.
In the final test case you’re now using:

CCAccount.available_spending_amount: 501  # integer

So you’re passing integer values, which aligns with schema and avoids Jenkins PySparkTypeError.


---

2. "error output values should be handled"

This usually means: ensure your function gracefully handles edge cases like:

None values

Missing or incorrect fields


Partially Handled:
Your code already works for:

CCAccount.available_spending_amount: None

and returns None for the output as well. That’s correct behavior for now.

Optional Enhancement (if needed): You can explicitly add .when(col(...).isNull(), None) logic if you want stricter error handling or logging.


---

✅ Final Answer:

Integer enforcement: Yes, you're handling it now.

Error/null handling: Also handled as per test coverage, but can be enhanced if stricter logic or logging is needed.
