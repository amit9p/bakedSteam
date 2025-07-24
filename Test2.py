
# metro2_codes.py

# Two-char Metro2 status codes
CURRENT             = "11"  # 0–29 days past due
PAID_ZERO_BALANCE   = "13"  # Paid or closed account
PAST_DUE_30_59      = "71"  # 30–59 days past due
PAST_DUE_60_89      = "78"  # 60–89 days past due
PAST_DUE_90_119     = "80"  # 90–119 days past due
PAST_DUE_120_149    = "82"  # 120–149 days past due
PAST_DUE_150_179    = "83"  # 150–179 days past due
PAST_DUE_180_PLUS   = "84"  # 180+ days past due
CHARGE_OFF_LOSS     = "97"  # Unpaid balance reported as a loss
PAID_IN_FULL_CHGOFF = "64"  # PIF, was a charge-off
DELETE_ACCOUNT      = "DA"  # Delete Account

# Optional reverse lookup:
METRO2_NAME_BY_CODE = {
    CURRENT:             "CURRENT",
    PAID_ZERO_BALANCE:   "PAID_ZERO_BALANCE",
    PAST_DUE_30_59:      "PAST_DUE_30_59",
    PAST_DUE_60_89:      "PAST_DUE_60_89",
    PAST_DUE_90_119:     "PAST_DUE_90_119",
    PAST_DUE_120_149:    "PAST_DUE_120_149",
    PAST_DUE_150_179:    "PAST_DUE_150_179",
    PAST_DUE_180_PLUS:   "PAST_DUE_180_PLUS",
    CHARGE_OFF_LOSS:     "CHARGE_OFF_LOSS",
    PAID_IN_FULL_CHGOFF: "PAID_IN_FULL_CHGOFF",
    DELETE_ACCOUNT:      "DELETE_ACCOUNT",
}
