
Thanks for checking! 50 sec per job adds up though , the more we split, the more install time we pay. Fine for a few splits, just don't want the setup cost to cancel out the speedup.

Nice work, approving! Splitting the data-driven tests further sounds good to me. One thing to keep in mind , each job re-runs pipenv install --dev, so too many small jobs might not actually save much time. Might be worth checking how long each job takes first, and making sure we have enough executors to run them in parallel.

Nice work, approving! Good with splitting data-driven further in principle , just worth checking per-job timings first, since each sub-job re-runs pipenv install --dev (though useCache should help). Only pays off if splits are balanced + we have executors to run them in parallel. Maybe aim for 2-3?

Nice work, approving! On splitting data-driven further , good with it in principle, but each sub-job re-runs pipenv install --dev, so too many tiny splits can cancel out the gain. Only helps if splits are balanced + we have enough executors to run them in parallel. Maybe check per-job timings and aim for 2-3 balanced splits?


This repo is failing the SonarQube quality gate on test coverage. Two conditions
fail: coverage on new code is 0% (needs ≥80%) and overall coverage is 77.8%
(needs ≥80%).

The entire new-code delta is one untested file:
  ecbr_tenant_card_dfs_l1/edq/common/scripts/edq_rule_engine.py
(122 uncovered lines, 26 uncovered branches/conditions, 0% coverage).

Do the following:
1. Read edq_rule_engine.py and summarize what each function/rule does before
   writing anything.
2. Write pytest unit tests that exercise every EDQ rule and hit both branches
   of each condition (target ≥80% line AND branch coverage on this file).
   Include edge cases: null / empty-string ah_previous_account_number,
   the ^\s*$ whitespace case, and the ^[\x20-\x7E]* printable-ASCII rule
   for apostrophes.
3. Also check sonar-project.properties: the Behave scaffolding files
   (features/environment.py, *_steps.py, component_test.py) appear to be
   counted as production code. Verify they're declared under sonar.tests /
   sonar.test.inclusions and not in the coverage denominator. Propose the fix
   if they aren't.
4. Run the tests with coverage (pytest --cov) and show me the resulting
   coverage % for edq_rule_engine.py before we commit.

Don't change production logic — only add tests and, if needed, Sonar config.
