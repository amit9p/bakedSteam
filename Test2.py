# tests/edq/conftest.py
import builtins
import yaml
import os
import io
import pytest

_original_safe_load = yaml.safe_load

@pytest.fixture(autouse=True)
def fake_yaml_load(monkeypatch, tmp_path):
    """
    Intercept yaml.safe_load to swap out config.yaml contents for test values.
    """
    def _fake_safe_load(stream):
        path = getattr(stream, "name", None)
        if path and path.endswith("config.yaml"):
            # only for config.yaml
            return {
                "JOB_ID": "JID",
                "DATA_SOURCE": "LOCAL",
                "LOCAL_DATA_PATH": str(tmp_path / "data" / "data.csv"),
                # any other keys your code expects...
            }
        elif path and path.endswith("secrets.yaml"):
            # for secrets.yaml, you can return either real or test creds
            return {
                "CLIENT_ID": "CID",
                "CLIENT_SECRET": "CSEC",
            }
        else:
            # fall back for anything else
            return _original_safe_load(stream)

    monkeypatch.setattr(yaml, "safe_load", _fake_safe_load)
    yield
    # No teardown needed; monkeypatch undo automatically


<><><>
# tests/edq/test_runEDQ_local_only.py
import os
import yaml
from pathlib import Path
import pytest

def make_small_csv(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv = data_dir / "data.csv"
    # simple two-column CSV with header
    csv.write_text("x,y\n1,a\n2,b\n")
    return data_dir

class DummyEngine:
    def __init__(self):
        self.called_args = None

    def execute_rules(self, df, job_id, client_id, client_secret, environment):
        # just record the args and return a minimal successful shape
        self.called_args = (df, job_id, client_id, client_secret, environment)
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {"results": [], "total_DF_rows": 0, "row_level_results": []},
        }

@pytest.fixture(autouse=True)
def stub_engine(monkeypatch):
    import edq_lib
    dummy = DummyEngine()
    monkeypatch.setattr(edq_lib, "engine", dummy)
    return dummy

def test_runEDQ_local_only(tmp_path, stub_engine, monkeypatch):
    # 1) drop our tiny CSV
    data_dir = make_small_csv(tmp_path)

    # 2) write (empty) config & secrets files on disk
    #    main() won’t actually read them because we fake yaml.safe_load,
    #    but just in case something else expects them
    (tmp_path / "config.yaml").write_text(yaml.dump({
        "LOCAL_DATA_PATH": str(data_dir),
        "DATA_SOURCE": "LOCAL",
        "JOB_ID": "SHOULD_BE_IGNORED",
    }))
    (tmp_path / "secrets.yaml").write_text(yaml.dump({
        "CLIENT_ID": "SHOULD_BE_IGNORED",
        "CLIENT_SECRET": "SHOULD_BE_IGNORED",
    }))

    # 3) cd into tmp_path so that main()’s os.path.dirname(__file__) logic
    #    or similar will find files here if it tries
    monkeypatch.chdir(tmp_path)

    # 4) now import & run your code under test
    #    (import after monkeypatch so config.yaml loads are intercepted)
    from ecbr_card_self_service.edq.local_run.runEDQ import main
    main()   # should not raise

    # 5) inspect what our dummy engine saw
    df_arg, seen_job_id, seen_cid, seen_secret, seen_env = stub_engine.called_args

    assert seen_job_id == "JID"
    assert seen_cid == "CID"
    assert seen_secret == "CSEC"
    assert seen_env == "Local" or seen_env == "LOCAL"

_$$$$$$$$$
def test_runEDQ_local_only(tmp_path, monkeypatch):
    # 1) Create data.csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv = data_dir / "data.csv"
    csv.write_text("x,y\n1,a\n")

    # 2) Create config.yml & secrets.yml
    config = {
        "LOCAL_DATA_PATH": str(data_dir),
        "DATA_SOURCE": "LOCAL",
        "JOB_ID": "JID",
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))
    (tmp_path / "secrets.yaml").write_text(yaml.dump({
        "CLIENT_ID": "CID",
        "CLIENT_SECRET": "CSEC",
    }))

    # 3) Create an empty base_segment.csv so Spark can “show()” it
    #    (it only needs a header to satisfy the schema scanner)
    (tmp_path / "base_segment.csv").write_text("account_id,delinquency_status\n")

    # 4) cd into tmp_path
    monkeypatch.chdir(tmp_path)

    # 5) Import *after* all our autouse stubs
    from ecbr_card_self_service.edq.local_run.runEDQ import main

    # 6) Should no longer raise
    main()

_______
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
