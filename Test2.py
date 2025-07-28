
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
