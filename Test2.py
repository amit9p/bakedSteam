
import sys
import types
import io
import os
import yaml
import pytest

from pyspark.sql import SparkSession

# 1) Before importing any of your code, install a fake edq_lib into sys.modules
fake_engine = types.SimpleNamespace()
fake_edq_lib = types.SimpleNamespace(engine=fake_engine)
sys.modules['edq_lib'] = fake_edq_lib


def test_runEDQ_local(monkeypatch, tmp_path):
    # 2) Create a tiny CSV that our "LOCAL" branch will read
    csv = tmp_path / "data.csv"
    csv.write_text("foo,bar\n1,2\n")
    csv_path = str(csv)

    # 3) Stub out yaml.safe_load so that config.yaml & secrets.yaml yield exactly what we want
    real_safe_load = yaml.safe_load

    def fake_safe_load(stream):
        # stream.name exists on real file handles
        name = getattr(stream, "name", "")
        if name.endswith("config.yaml"):
            return {
                "JOB_ID": "myJob123",
                "DATA_SOURCE": "LOCAL",
                "LOCAL_DATA_PATH": csv_path,
            }
        if name.endswith("secrets.yaml"):
            return {
                "CLIENT_ID": "theClient",
                "CLIENT_SECRET": "theSecret",
            }
        # fallback (shouldn't really happen)
        return real_safe_load(stream)

    monkeypatch.setattr(yaml, "safe_load", fake_safe_load)

    # 4) Patch our fake engine.execute_rules to capture its inputs
    called = {}
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        called["args"] = (job_id, client_id, client_secret, environment)
        # return the shape your code expects
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 0,
                "row_level_results": []
            }
        }

    fake_engine.execute_rules = fake_execute_rules

    # 5) Now import the module under test (it will pick up our fake edq_lib & yaml.safe_load)
    from ecbr_card_self_service.edq.local_run.runEDQ import main

    # 6) Run it!
    main()

    # 7) Assert that we invoked engine.execute_rules with the values from our fake config/secrets
    assert called["args"] == (
        "myJob123",       # from config.yaml → JOB_ID
        "theClient",      # from secrets.yaml → CLIENT_ID
        "theSecret",      # from secrets.yaml → CLIENT_SECRET
        "NonProd",        # hard-coded in your runEDQ call
    )
