
.when(bankruptcy_status_open | bankruptcy_status_discharged & 
      to_date(bankruptcy_file_date, "MMddyyyy") < to_date(charge_off_date, "MMddyyyy"), lit("pre-CO Account Status"))

This condition applies to both OPEN and DISCHARGED bankruptcy statuses.

If an account satisfies both conditions (e.g., status = discharged but file_date < charge_off_date), it might unintentionally assign pre-CO when another rule should apply.


Solution:

Break them into explicit conditions:

.when(bankruptcy_status_open & to_date(bankruptcy_file_date, "MMddyyyy") < to_date(charge_off_date, "MMddyyyy"), lit("pre-CO"))
.when(bankruptcy_status_discharged & to_date(bankruptcy_file_date, "MMddyyyy") < to_date(charge_off_date, "MMddyyyy"), lit("pre-CO"))

This ensures that each status is evaluated separately and avoids unintended assignment.
