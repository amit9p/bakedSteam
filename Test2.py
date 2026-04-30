
Hi Olivia, my understanding is that "context" is runtime metadata from the platform, not something defined in "config.yaml".

In this PR I did not assume a new payload contract; I only followed the existing generator pattern where "context" may contain values like "product_type" and "reporting_date". The compatibility fix was added because the framework was calling the method positionally, so "context" could shift into the "edq_suppressions_df" slot when that DF was not passed.

I can’t guarantee platform will always pass "context" unless platform confirms that contract. The code is safe when "context" is absent, but if Canada needs to depend on specific context fields, it would be good to confirm the payload with platform.


df -h /
du -sh ~/* 2>/dev/null | sort -h
du -sh ~/Library/* 2>/dev/null | sort -h | tail -20
rm -rf ~/Library/Caches/*
rm -rf ~/Library/Logs/*



Title
Generator O/P readiness – testing strategy for report / do not report scenarios
Description
As part of Generator Output Readiness, this story is to define and execute a testing strategy for generator report / do not report behavior based on the Generator O/P Readiness sheet. This includes reviewing listed scenarios, validating expected outcomes, identifying which cases are already covered in feature tests, finding coverage gaps, executing testable scenarios, and updating the readiness sheet with status, results, and follow-ups.
Acceptance Criteria
Test strategy is defined using the Generator O/P Readiness sheet.
Scenarios are reviewed against expected report / do not report outcome.
Existing feature-test coverage is identified.
Missing coverage / blockers are documented.
Testable scenarios are executed.
Readiness sheet is updated with testing status and results.
References
Generator O/P Readiness sheet: [paste Google Sheet link]
If you want it even shorter, use this:
Description
Define and execute testing strategy for generator output report / do not report scenarios using the Generator O/P Readiness sheet. Review scenarios, validate expected outcomes, map them to existing feature tests, identify gaps, execute testable cases, and update the sheet with status, results, and blockers.
