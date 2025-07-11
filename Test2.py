
# tests/edq/test_runEDQ.py
import sys, types, yaml, pytest
from pathlib import Path

# ─── 1) PRE‐INJECT ALL EXTERNAL MODULES ────────────────────────────────────────

# 1a) stub out edq_lib.engine
fake_engine = types.SimpleNamespace()
sys.modules['edq_lib'] = types.SimpleNamespace(engine=fake_engine)

# 1b) stub out boto3.Session so no real AWS SDK is required
fake_boto3 = types.ModuleType('boto3')
class FakeBoto3Session:
    def __init__(self, profile_name=None): pass
    def get_credentials(self):
        # must have .access_key, .secret_key, .token and get_frozen_credentials()
        creds = types.SimpleNamespace(
            access_key="AKIAFAKE",
            secret_key="SECRETFAKE",
            token=None,
            get_frozen_credentials=lambda: types.SimpleNamespace(
                access_key="AKIAFAKE",
                secret_key="SECRETFAKE",
                token=None
            )
        )
        return creds
fake_boto3.Session = FakeBoto3Session
sys.modules['boto3'] = fake_boto3

# 1c) stub out oneLake_mini.OneLakeSession
fake_ol = types.ModuleType('oneLake_mini')
def FakeOneLakeSession(**kwargs):
    # returns an object with get_credentials() and get_dataset(id)
    creds = types.SimpleNamespace(
        access_key="FAKE",
        secret_key="FAKE",
        token=None,
        get_frozen_credentials=lambda: types.SimpleNamespace(
            access_key="FAKE",
            secret_key="FAKE",
            token=None
        )
    )
    # dataset.get_S3fs() and dataset.location are only used to list partitions,
    # but since LOCAL branch never calls them, we just stub minimally.
    dataset = types.SimpleNamespace(get_S3fs=lambda: None, location="")
    return types.SimpleNamespace(
        get_credentials=lambda: creds,
        get_dataset=lambda dsid: dataset
    )
fake_ol.OneLakeSession = FakeOneLakeSession
sys.modules['oneLake_mini'] = fake_ol

# ─── 2) NOW IMPORT YOUR CODE UNDER TEST ───────────────────────────────────────

# At this point `import runEDQ` will see our fake `edq_lib`, `boto3` and `oneLake_mini`.
from pyspark.sql import SparkSession
from ecbr_card_self_service.edq.local_run.runEDQ import main

# ─── 3) THE ACTUAL TEST ──────────────────────────────────────────────────────

def test_runEDQ_local_only(monkeypatch, tmp_path):
    # 3a) write a tiny CSV that the LOCAL branch will read
    data_file = tmp_path / "tiny.csv"
    data_file.write_text("x,y\n100,fox\n200,cat\n")

    # 3b) stub yaml.safe_load to return just enough config/secrets
    real_load = yaml.safe_load
    def fake_safe_load(fp):
        fn = getattr(fp, "name", "")
        if fn.endswith("config.yaml"):
            return {
                "JOB_ID": "job-123",
                "DATA_SOURCE": "LOCAL",
                "LOCAL_DATA_PATH": str(data_file),
            }
        if fn.endswith("secrets.yaml"):
            return {
                "CLIENT_ID": "cid-xyz",
                "CLIENT_SECRET": "csecret-abc",
            }
        return real_load(fp)
    monkeypatch.setattr(yaml, "safe_load", fake_safe_load)

    # 3c) patch our fake_engine.execute_rules to capture the call
    called = {}
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        called["args"] = (job_id, client_id, client_secret, environment)
        # return the shape runEDQ expects so it can finish cleanly
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": df.count(),
                "row_level_results": df   # it .show()s at end
            }
        }
    fake_engine.execute_rules = fake_execute_rules

    # 3d) create a real SparkSession for the test
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("test_runEDQ")
             .getOrCreate())

    # 3e) actually invoke your main() (it will read our tiny CSV, stub configs, call fake_engine...)
    main()

    # 3f) assert that we called execute_rules with exactly our fake values
    assert called["args"] == (
        "job-123",      # from config.yaml
        "cid-xyz",      # from secrets.yaml
        "csecret-abc",  # from secrets.yaml
        "NonProd"       # hard-coded inside runEDQ.py
    )
