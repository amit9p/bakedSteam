
Everything else should come after that check. This is exactly why we usually place that (PLP + NPSL) => NULL check above the universal “NPSL ⇒ 0G” condition. If we don’t do it first, Spark will match the NPSL rule prematurely and never reach the (PLP + NPSL) rule.

In short:

1. First: (private_label_partnership & NPSL) => NULL


2. Then: NPSL => 0G


3. Then: anything else (like numeric small_business => 8A, numeric PLP => 07, fallback => 18, etc.)



That ensures the “PLP + NPSL => NULL” scenario is always applied before the simpler “NPSL => 0G.”

