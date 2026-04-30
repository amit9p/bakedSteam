
Just sharing this as a reference, not saying this is the confirmed root cause.

If a value like "04292026" is parsed using "DDMMYYYY", Python reproduces the same error pattern:

from datetime import datetime
datetime.strptime("04292026", "%d%m%Y")
# ValueError: unconverted data remains: 6

So this may still be useful as a clue if we come back to the date-handling path later.


from datetime import datetime
datetime.strptime("04292026", "%d%m%Y")


I checked the platform code. This does not look like a joiner output issue. It looks like "reporting_date" format mismatch.

Platform expects "DDMMYYYY", but the run is passing "04292026" ("MMDDYYYY" style). That likely explains the "ValueError: unconverted data remains: 6".


Dedupe before each join looks fine for debugging, but I’m a bit concerned about changing all joins to "inner". If any helper function misses a key, we may drop valid records from the final output. Earlier logic was using "left", so this changes behavior.

Also, the "count()" calls inside the loop are okay for debugging, but they will add extra Spark actions and shouldn’t stay in the final version.
