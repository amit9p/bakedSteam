
Good catch, Mohan – you’re right.
In the current Lucidchart the arrow that feeds “Joint Account Indicator = true” comes after the “Bankruptcy File Date < Charge-Off Date” check, so the pure “Bankruptcy Chapter + Joint Account” shortcut from Step 4 isn’t represented.

We really need two parallel branches under “Bankruptcy Chapter [exists]”:

1. Step 4 Bankruptcy Chapter + Joint Account → Account Status = DA (no File-Date test).


2. Step 6 Bankruptcy Status = OPEN + File Date < CO Date (+ other qualifiers) → Pre-CO Status.



I’ll add a separate arrow from the “Bankruptcy Chapter” diamond straight to the “Joint Account” diamond (label it “4”) and leave the existing File-Date/Status-OPEN branch as “6”. I’ll push the updated Lucidchart shortly so we’re fully aligned with the function table.  Thanks for flagging this!





“The Bankruptcy File Date check is in Step 6 of the function table (the ‘Bankruptcy Status = OPEN’ rule); Step 4 handles only ‘Bankruptcy Chapter + Joint Account’. Lucidchart shows those two diamonds in that same order, so the logic still lines up.”




# Step 1: Checkout your feature branch
git checkout feature-branch

# Step 2: Fetch latest main
git fetch origin

# Step 3: Merge main into your branch (like "update with merge commit")
git merge origin/main

# Step 4: Push changes (if needed)
git push origin feature-branch



expected_rows = [
    {ABSegment.account_id: "A11",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "A13_AU", ABSegment.account_status_1: 16},
    {ABSegment.account_id: "A13_X",  ABSegment.account_status_1: 30},
    {ABSegment.account_id: "A64_AU", ABSegment.account_status_1: 16},
    {ABSegment.account_id: "A64_X",  ABSegment.account_status_1: 9},
    {ABSegment.account_id: "A97_AH", ABSegment.account_status_1: 2},
    {ABSegment.account_id: "A97_X",  ABSegment.account_status_1: 11},   # ← fixed
    {ABSegment.account_id: "A71",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "A78",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "A80",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "A82",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "A83",    ABSegment.account_status_1: 11},
    {ABSegment.account_id: "ADA",    ABSegment.account_status_1: 5},
    {
        ABSegment.account_id: "A84",
        ABSegment.account_status_1: c.DEFAULT_ERROR_INTEGER,  # else / error path
    },
]

expected_df = (
    create_partially_filled_dataset(spark, ABSegment, data=expected_rows)
    .select(ABSegment.account_id, ABSegment.account_status_1)
)

assert_df_equality(
    result_df,
    expected_df,
    ignore_row_order=True,
    ignore_column_order=True,
    ignore_nullable=True,     # ← makes chispa ignore nullable flag difference
)
