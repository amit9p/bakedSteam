

Objective: Align ECOA output field with the Intent Page logic and updated boolean fields (is_account_holder_deceased, is_customer_incorrectly_marked_deceased).


---

✔ Schema updates

Add/confirm output field ecoa_code (string)

Remove dependency on customer_restriction_status

Ensure schema uses:

is_account_holder_deceased

is_customer_incorrectly_marked_deceased




---

✔ Logic implementation (priority order)

Implement ECOA assignment in following priority (highest to lowest):

1. If joint_account_indicator = true → ECOA = "2"


2. If is_account_holder_deceased = true → ECOA = "X"


3. If reactivation_notification = 'Reactivated' → ECOA = "T"


4. If is_customer_incorrectly_marked_deceased = true → ECOA = "1"


5. Else → ECOA = "1"



(This matches the intent page exactly and preserves original fallback behavior.)


---

✔ Replace old logic references

Remove usage of customer_restriction_status

Replace with new bool fields per updated consolidator output



---

✔ Output mapping

Map calculated ECOA value into Field 37 in the output dataset

Verify that downstream consumers read from this field



---

✔ Testing

Add unit tests for:

case: joint

case: deceased confirmed

case: reactivated

case: deceased-in-error

case: default path


Ensure mutually exclusive priority ordering is tested



---

✔ Validation

Validate schema change against the O/P schema definition

Confirm output matches “intent page” with Betsy



---

✔ Deliverable outcome

After this change:

ECOA code in output dataset matches documented intent logic

No reference to customer_restriction_status

Uses new booleans as agreed in Slack discussion
