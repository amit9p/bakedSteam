
# metro2_disposition_codes.py

# Two- or three-digit disposition codes (as integers)
ACCOUNT_SOLD_TRANSFERRED                  = 2    # “2” – Account Sold/Transferred
CLOSED                                    = 5    # “5” – Closed
PAID_CHARGE_OFF                           = 9    # “9” – Paid Charge Off
CHARGE_OFF_WHOLE_BALANCE                  = 11   # “11” – Charge Off – Whole Balance
SETTLED_FOR_LESS_THAN_AMOUNT_DUE_AGREEMENT = 16   # “16” – Settled for Less than Amount Due (Agreement)
ACCOUNT_PAID_IN_FULL_AS_AGREED            = 30   # “30” – Account Paid in Full – As Agreed

# Optional reverse-lookup mapping if you need to get the name from a code:
DISPOSITION_NAME_BY_CODE = {
    ACCOUNT_SOLD_TRANSFERRED:                  "ACCOUNT_SOLD_TRANSFERRED",
    CLOSED:                                    "CLOSED",
    PAID_CHARGE_OFF:                           "PAID_CHARGE_OFF",
    CHARGE_OFF_WHOLE_BALANCE:                  "CHARGE_OFF_WHOLE_BALANCE",
    SETTLED_FOR_LESS_THAN_AMOUNT_DUE_AGREEMENT: "SETTLED_FOR_LESS_THAN_AMOUNT_DUE_AGREEMENT",
    ACCOUNT_PAID_IN_FULL_AS_AGREED:            "ACCOUNT_PAID_IN_FULL_AS_AGREED",
}

# metro2_special_condition_codes.py

# Two-character special condition codes (positions 124–125)
PURCHASED_BY_ANOTHER_COMPANY        = "AH"  # Purchased by another company
PIF_LESS_THAN_FULL_BALANCE          = "AU"  # Account PIF for less than full balance
CLOSED_DUE_TO_TRANSFER              = "AT"  # Account closed due to transfer
CLOSED_AT_CREDIT_GRANTOR_REQUEST    = "M"   # Account closed at credit grantor’s request
NO_SPECIAL_CONDITION                = None  # Code = NULL

# Optional reverse lookup if you need the constant name from a code:
SPECIAL_COND_NAME_BY_CODE = {
    PURCHASED_BY_ANOTHER_COMPANY:     "PURCHASED_BY_ANOTHER_COMPANY",
    PIF_LESS_THAN_FULL_BALANCE:       "PIF_LESS_THAN_FULL_BALANCE",
    CLOSED_DUE_TO_TRANSFER:           "CLOSED_DUE_TO_TRANSFER",
    CLOSED_AT_CREDIT_GRANTOR_REQUEST: "CLOSED_AT_CREDIT_GRANTOR_REQUEST",
    NO_SPECIAL_CONDITION:             "NO_SPECIAL_CONDITION",
}



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
