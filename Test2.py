Summary

This PR introduces a declarative generator configuration for DFS pipelines so we can define inputs, keys, joins, and filters in one place (no more hard-coded rules in code).
It enables the generator to:

read calculator, reporting-override, and previously-reported datasets,

normalize/validate join keys,

apply the business rule “Reporting (‘R’/’r’) and not previously reported”, and

emit only Calculator fields for the final reportable accounts feed.


This is groundwork for future generators and makes changes safer/auditable.

Relates to: CT4019T-454 (Generator Config)

Focus

Centralize rules that were scattered across utils into a single config artifact.

Keep existing behavior: case-insensitive reporting_status == R and left-anti against previously-reported.

Improve observability (row counts by step) for easier debugging.


Implementation strategy

Added config: dfs/generator/generator_config_CT4019T_454.yaml (name example) with:

dataset aliases (Calc, Ovr, Prev)

column bindings using schemas (EcbrCalculatorDfOutput.account_id, EcbrDFSOverride.reporting_status, …)

join types/conditions and filters


Added light-weight loader + validation (required keys/columns, types, allowed join types).

Updated ecbr_generator/.../reportable_accounts.py to consume config instead of hardcoding:

Normalize join keys (cast -> string, trim)

Status normalization: lower(substring(status,1,1)) == 'r'

inner join Calc ↔ Ovr; left_anti with Prev

Return columns in Calculator order


Kept schemas in schemas/ as the single source for field names.

Added unit tests under tests/ecbr_generator/unit_tests/test_reportable_accounts.py using create_partially_filled_dataset with happy-path and edge cases.

Added defensive logs for step counts (eligible, pruned) to aid triage (log level DEBUG).


Readiness checklist

[x] All tests pass locally

[x] Relevant unit tests added/updated

[x] Backwards compatible (no breaking schema changes)

[x] Config validated at load time; descriptive errors on missing/typo’d columns

[x] Docs: short README in dfs/generator/ describing config fields and examples
