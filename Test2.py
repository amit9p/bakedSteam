
I checked the platform code. This does not look like a joiner output issue. It looks like "reporting_date" format mismatch.

Platform expects "DDMMYYYY", but the run is passing "04292026" ("MMDDYYYY" style). That likely explains the "ValueError: unconverted data remains: 6".


Dedupe before each join looks fine for debugging, but I’m a bit concerned about changing all joins to "inner". If any helper function misses a key, we may drop valid records from the final output. Earlier logic was using "left", so this changes behavior.

Also, the "count()" calls inside the loop are okay for debugging, but they will add extra Spark actions and shouldn’t stay in the final version.
