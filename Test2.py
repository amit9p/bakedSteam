Dedupe before each join looks fine for debugging, but I’m a bit concerned about changing all joins to "inner". If any helper function misses a key, we may drop valid records from the final output. Earlier logic was using "left", so this changes behavior.

Also, the "count()" calls inside the loop are okay for debugging, but they will add extra Spark actions and shouldn’t stay in the final version.
