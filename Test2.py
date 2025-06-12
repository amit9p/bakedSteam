
Current Code Pattern (Repeated Conditionals)

Your logic has many repeated when(...) blocks like:

when(manual_delete_code_exists, lit(ACCOUNT_STATUS_DA))
when(is_terminal_notification_true, lit(ACCOUNT_STATUS_DA))
when(is_aged_debt_notification_true, lit(ACCOUNT_STATUS_DA))

They are clean and correct, but since many of them lead to the same output (e.g., DA), you could group related flags into one block to reduce repetition.


---

✅ Suggested Grouped Style (Optional Refactor)

when(
    manual_delete_code_exists |
    is_terminal_notification_true |
    (is_deceased_notification_true_acct_type_8a) |
    is_aged_debt_notification_true,
    lit(ACCOUNT_STATUS_DA)
)

This:

Keeps the logic tight

Reduces vertical length

Improves readability if the logic grows
