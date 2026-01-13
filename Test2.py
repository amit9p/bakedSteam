pipenv run behave -n "Process calculate_current_balance from parquet file" --no-capture --no-capture-stderr --show-timings



cond 6

.when(
    (
        lower(col(CustomerInformation.bankruptcy_court_case_status_code.str))
        == constants.BankruptcyStatus.OPEN.value
    )
    & (
        col(CustomerInformation.bankruptcy_chapter_number.str).isin(
            constants.BankruptcyChapter.TWELVE.value,
            constants.BankruptcyChapter.THIRTEEN.value,
        )
    ),
    value=0,
)

---

cond 8

.when(
    lower(col(CCAccount.reactivation_status.str))
    == constants.ReactivationNotification.REACTIVATED.value.lower(),
    value=0,
)

  cond 5

  .when(
    col(CCAccount.posted_balance.str) < 0,
    value=0,
  )


shakuntala.padmanabhuni valid point — the logic does exist today in the calculator.
The reason for moving it to the consolidator is that this is really an account-level classification (closed_by_consumer vs closed_by_grantor), not something specific to special comment code.
Centralizing it in consolidator lets us derive it once and reuse it across calculators, instead of duplicating the same logic in multiple places. Calculators can then just consume the derived output.
If you want it even shorter, use this:
This logic exists today, but we’re moving it to consolidator since it’s an account-level classification meant to be derived once and reused, rather than recalculated inside each calculator.
Tell me if you want a more assertive or more neutral tone.



Hi @KabanerTeamLead,
As part of the SCC calculation, we identified an upstream requirement to roll up Account Closure Reason into a derived status (closed_by_consumer / closed_by_grantor).
Per the spec, this roll-up is expected to be handled by the Card consolidator, with the calculator consuming only the derived value.
Can you confirm if this is owned by the Kabaner team and when we can expect availability? Happy to sync if needed.
CC: @ProductManager
