
# tests/edq/test_runEDQ_local_only.py
import sys
import types
import yaml
import pytest
from pathlib import Path
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest") \
        .getOrCreate()

@pytest.fixture(autouse=True)
def stub_everything(monkeypatch):
    """
    Before any imports, inject dummy edq_lib, boto3, and oneLake_mini modules
    so that runEDQ.py can import them without error.
    Also give us a hook to assert that engine.execute_rules() was called.
    """
    # 1) stub out edq_lib.engine.execute_rules
    dummy_engine = types.SimpleNamespace()
    called = {}
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        called['args'] = (job_id, client_id, client_secret, environment)
        # return the shape your code expects:
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 1,
                "row_level_results": []
            }
        }
    dummy_engine.execute_rules = fake_execute_rules

    edq_lib_mod = types.SimpleNamespace(engine=dummy_engine)
    monkeypatch.setitem(sys.modules, "edq_lib", edq_lib_mod)

    # 2) stub boto3.Session(...)
    fake_creds = types.SimpleNamespace(
        get_frozen_credentials=lambda: types.SimpleNamespace(access_key="AK", secret_key="SK", token=None)
    )
    fake_session = types.SimpleNamespace(get_credentials=lambda: fake_creds)
    boto3_mod = types.SimpleNamespace(Session=lambda **kw: fake_session)
    monkeypatch.setitem(sys.modules, "boto3", boto3_mod)

    # 3) stub oneLake_mini.OneLakeSession
    class FakeOneLakeSession:
        def __init__(self, **kw): 
            self.auth_token = "T"
        def get_dataset(self, dataset_id):
            # we only exercise LOCAL path in this test
            return types.SimpleNamespace(
                get_s3fs=lambda: None,
                location=str(Path.cwd())
            )
    onelake_mod = types.SimpleNamespace(OneLakeSession=FakeOneLakeSession)
    monkeypatch.setitem(sys.modules, "oneLake_mini", onelake_mod)

    return called

def test_runEDQ_local_only(tmp_path: Path, spark, stub_everything, monkeypatch):
    # 1) write out a tiny one-row CSV that runEDQ will pick up
    data_file = tmp_path / "data.csv"
    spark.createDataFrame([(1, "foo")], ["x", "y"]) \
         .coalesce(1) \
         .write \
         .option("header", "true") \
         .csv(str(data_file))
    
    # 2) write config.yaml & secrets.yaml
    (tmp_path / "config.yaml").write_text(yaml.dump({"LOCAL_DATA_PATH": str(data_file)}))
    (tmp_path / "secrets.yaml").write_text(yaml.dump({"CLIENT_ID": "CID", "CLIENT_SECRET": "CSEC"}))
    
    # 3) chdir into tmp_path so runEDQ finds those two files
    monkeypatch.chdir(tmp_path)
    
    # 4) now import & run your code under test
    from ecbr_card_self_service.edq.local_run.runEDQ import main
    main()
    
    # 5) assert that our fake engine.execute_rules was called
    job_id, cid, csec, env = stub_everything['args']
    assert cid == "CID"
    assert csec == "CSEC"
    assert env == "local"
    # you can also assert job_id came from your config–> config["JOB_ID"] if set

    # done!


-----
# tests/edq/test_runEDQ_local_only.py
import os
import csv
import yaml
import pytest
from pathlib import Path
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------
# 1) dummy EDQ engine to capture execute_rules() calls
# -----------------------------------------------------------------------------
class DummyEngine:
    def __init__(self):
        self.called = {}

    def execute_rules(self, df, job_id, client_id, client_secret, environment):
        # record exactly what we got for the four parameters after df
        self.called["args"] = (job_id, client_id, client_secret, environment)
        # return shape your code expects
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 0,
                "row_level_results": []
            }
        }

# -----------------------------------------------------------------------------
# 2) fake SparkSession.builder → FakeBuilder → FakeSession/Reader,
#    so we never launch a real Spark cluster in tests
# -----------------------------------------------------------------------------
class FakeReader:
    def format(self, fmt):
        return self
    def option(self, key, val):
        return self
    def load(self, path):
        # load the tiny CSV we wrote
        rows = []
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

class FakeSparkSession:
    def __init__(self):
        self.read = FakeReader()

class FakeBuilder:
    def __init__(self):
        pass
    def appName(self, _):
        return self
    def config(self, _, __):
        return self
    def getOrCreate(self):
        return FakeSparkSession()

# -----------------------------------------------------------------------------
# 3) fixtures
# -----------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def stub_engine(monkeypatch):
    """
    Swap out edq_lib.engine for our DummyEngine so that
    when runEDQ calls engine.execute_rules() we capture the call.
    """
    import edq_lib
    dummy = DummyEngine()
    monkeypatch.setattr(edq_lib, "engine", dummy)
    return dummy

@pytest.fixture(autouse=True)
def stub_spark_builder(monkeypatch):
    """
    Swap out SparkSession.builder → FakeBuilder so no real Spark context fires.
    """
    monkeypatch.setattr(SparkSession, "builder", FakeBuilder())
    return

# -----------------------------------------------------------------------------
# 4) helper to write our tiny CSV and YAML files
# -----------------------------------------------------------------------------
def write_csv(path: Path):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["x","y"])
        writer.writerow([1,"a"])
        writer.writerow([2,"b"])
        writer.writerow([3,"c"])

# -----------------------------------------------------------------------------
# 5) the actual test
# -----------------------------------------------------------------------------
def test_runEDQ_local_only(tmp_path: Path,
                           stub_engine,
                           stub_spark_builder,
                           monkeypatch):
    # a) write data.csv
    data_csv = tmp_path / "data.csv"
    write_csv(data_csv)

    # b) write config.yaml
    #    - JOB_ID must match what runEDQ pulls
    #    - DATA_SOURCE anything other than "s3"/"onelake" → goes local branch
    #    - LOCAL_DATA_PATH points at our data.csv
    config = {
        "JOB_ID": "test_job_123",
        "DATA_SOURCE": "local",
        "LOCAL_DATA_PATH": str(data_csv)
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))

    # c) write secrets.yaml (only CLIENT_ID/CLIENT_SECRET used in local path)
    secrets = {
        "CLIENT_ID": "my-client",
        "CLIENT_SECRET": "my-secret"
    }
    (tmp_path / "secrets.yaml").write_text(yaml.dump(secrets))

    # d) run inside tmp_path so runEDQ finds config.yaml + secrets.yaml there
    monkeypatch.chdir(tmp_path)

    # e) import & invoke
    from ecbr_card_self_service.edq.local_run.runEDQ import main
    main()

    # f) assert that our dummy engine saw exactly the args we expect
    #    (job_id, client_id, client_secret, environment)
    assert stub_engine.called["args"] == (
        "test_job_123",
        "my-client",
        "my-secret",
        "NonProd"
    )
