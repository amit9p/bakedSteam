
# tests/edq/conftest.py
import sys
import types
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(autouse=True)
def stub_all_dependencies(monkeypatch, tmp_path):
    """
    1) Stub edq_lib.engine.execute_rules
    2) Stub boto3.Session
    3) Stub oneLake_mini.OneLakeSession
    4) Override SparkSession.builder but capture the *real* builder
       so that we can actually spawn a local session under getOrCreate().
    """
    # ─────────────────────────────────────────────────────────────────────────────
    # 1) edq_lib.engine
    fake_engine = types.SimpleNamespace()
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        fake_engine.called = (job_id, client_id, client_secret, environment)
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": df.count(),
                "row_level_results": df,
            }
        }
    fake_engine.execute_rules = fake_execute_rules

    fake_edq = types.ModuleType("edq_lib")
    fake_edq.engine = fake_engine
    monkeypatch.setitem(sys.modules, "edq_lib", fake_edq)

    # ─────────────────────────────────────────────────────────────────────────────
    # 2) boto3.Session
    fake_boto3 = types.ModuleType("boto3")
    class FakeCreds:
        access_key = "AK"
        secret_key = "SK"
        token = None
    class FakeSession:
        def __init__(self, profile_name=None): pass
        def get_credentials(self):
            return types.SimpleNamespace(get_frozen_credentials=lambda: FakeCreds())
    fake_boto3.Session = FakeSession
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    # ─────────────────────────────────────────────────────────────────────────────
    # 3) oneLake_mini.OneLakeSession
    fake_one = types.ModuleType("oneLake_mini")
    class FakeOneLake:
        def __init__(self, **kw): self.auth_token = "TOK"
        def get_dataset(self, dsid):
            return types.SimpleNamespace(get_s3fs=lambda: None, location="")
    fake_one.OneLakeSession = FakeOneLake
    monkeypatch.setitem(sys.modules, "oneLake_mini", fake_one)

    # ─────────────────────────────────────────────────────────────────────────────
    # 4) SparkSession.builder
    #    capture the real builder before we overwrite it
    real_builder = SparkSession.builder

    class FakeBuilder:
        def appName(self, _): return self
        def config(self, *a, **k): return self
        def getOrCreate(self):
            # now call the real one to spin up a local[*] spark
            return real_builder.master("local[1]") \
                               .appName("pytest") \
                               .getOrCreate()

    monkeypatch.setattr(SparkSession, "builder", FakeBuilder())

    yield

    # tear down any real SparkSession we created
    try:
        SparkSession.builder.getOrCreate().stop()
    except:
        pass

_____
import sys
import types
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(autouse=True)
def stub_all_dependencies(monkeypatch, tmp_path):
    """
    Automatically stub out onelake_mini, boto3, edq_lib.engine, and SparkSession.builder
    for every test in this folder.
    """

    # 1) stub edq_lib.engine
    fake_engine = types.SimpleNamespace()
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        # record last‐called args, return the minimal structure your code expects
        fake_engine.called = (job_id, client_id, client_secret, environment)
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": df.count(),
                "row_level_results": df  # your code .show()s or .count()s on this
            }
        }
    fake_engine.execute_rules = fake_execute_rules

    fake_edq_lib = types.ModuleType("edq_lib")
    fake_edq_lib.engine = fake_engine
    monkeypatch.setitem(sys.modules, "edq_lib", fake_edq_lib)

    # 2) stub boto3.Session(...) → returns object with get_credentials().get_frozen_credentials()
    fake_boto3 = types.ModuleType("boto3")
    class FakeCreds:
        access_key = "AK"
        secret_key = "SK"
        token = None
    class FakeSession:
        def __init__(self, profile_name=None): pass
        def get_credentials(self):
            return types.SimpleNamespace(get_frozen_credentials=lambda: FakeCreds())
    fake_boto3.Session = FakeSession
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)

    # 3) stub onelake_mini.OneLakeSession
    fake_onelake = types.ModuleType("oneLake_mini")
    class FakeOneLakeSession:
        def __init__(self, **kw): 
            self.auth_token = "TOK"
        def get_dataset(self, dataset_id):
            # mimic object with .get_s3fs() & .location
            return types.SimpleNamespace(
                get_s3fs=lambda: None,
                location=""  # not used in LOCAL branch
            )
    fake_onelake.OneLakeSession = FakeOneLakeSession
    monkeypatch.setitem(sys.modules, "oneLake_mini", fake_onelake)

    # 4) stub SparkSession.builder → return a local SparkSession
    class FakeBuilder:
        def appName(self, name): return self
        def config(self, *a, **kw): return self
        def getOrCreate(self):
            return SparkSession.builder \
                .master("local[1]") \
                .appName("pytest") \
                .getOrCreate()
    monkeypatch.setattr(SparkSession, "builder", FakeBuilder())

    yield  # done stubbing

    # SparkSession teardown (stop any lingering local session)
    try:
        SparkSession.builder.getOrCreate().stop()
    except Exception:
        pass


-----'
import os
import yaml
import pytest
from pyspark.sql import SparkSession

def test_runEDQ_local_only(tmp_path, monkeypatch):
    # 1) Create a tiny CSV under tmp_path/data/data.csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv = data_dir / "data.csv"
    csv.write_text("x,y\n1,a\n")

    # 2) Write config.yaml & secrets.yaml in tmp_path
    config = {
        "LOCAL_DATA_PATH": str(data_dir),
        "DATA_SOURCE": "LOCAL",
        "JOB_ID": "JID"
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))

    secrets = {
        "CLIENT_ID": "CID",
        "CLIENT_SECRET": "CSEC"
    }
    (tmp_path / "secrets.yaml").write_text(yaml.dump(secrets))

    # 3) chdir so runEDQ.py will pick up config.yaml & secrets.yaml
    monkeypatch.chdir(tmp_path)

    # 4) import & run your code under test
    #    (import after monkeypatch so our stub modules are used)
    from ecbr_card_self_service.edq.local_run.runEDQ import main

    # Should not raise
    main()

    # 5) verify that our fake edq_lib.engine was called with the right secrets
    from edq_lib import engine as stub_engine
    job_id, client_id, client_secret, env = stub_engine.called
    assert job_id == "JID"
    assert client_id == "CID"
    assert client_secret == "CSEC"
    assert env == "LOCAL"

    # 6) also verify that the "row_level_results" had exactly 1 row
    #    (engine returned .row_level_results == the DataFrame)
    result_df = stub_engine.execute_rules.__self__.row_level_results
    assert result_df.count() == 1
