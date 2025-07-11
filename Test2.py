
# tests/conftest.py
import sys, types

# ─── fake edq_lib.engine ─────────────────────────────────────────────────────
fake_engine = types.SimpleNamespace(execute_rules=lambda *args, **kwargs: None)
sys.modules['edq_lib']       = types.ModuleType('edq_lib')
sys.modules['edq_lib.engine']= fake_engine

# ─── fake boto3.Session ──────────────────────────────────────────────────────
fake_boto3 = types.ModuleType('boto3')
class FakeBoto3Session:
    def __init__(self, profile_name=None): pass
    def get_credentials(self):
        # return a simple object with access_key/secret_key/token
        creds = types.SimpleNamespace(
            access_key="FAKE_AK",
            secret_key="FAKE_SK",
            token=None,
            get_frozen_credentials=lambda: types.SimpleNamespace(
                access_key="FAKE_AK", secret_key="FAKE_SK", token=None
            )
        )
        return creds
fake_boto3.Session = FakeBoto3Session
sys.modules['boto3'] = fake_boto3

# ─── fake oneLake_mini.OneLakeSession ────────────────────────────────────────
fake_ol = types.ModuleType('oneLake_mini')
def FakeOneLakeSession(**kwargs):
    creds = types.SimpleNamespace(
        access_key="FAKE_AK",
        secret_key="FAKE_SK",
        token=None,
        get_frozen_credentials=lambda: types.SimpleNamespace(
            access_key="FAKE_AK", secret_key="FAKE_SK", token=None
        )
    )
    # we only need get_credentials() and get_dataset()
    ds = types.SimpleNamespace(get_S3fs=lambda: None, location="")
    return types.SimpleNamespace(
        get_credentials=lambda: creds,
        get_dataset=lambda dsid: ds
    )
fake_ol.OneLakeSession = FakeOneLakeSession
sys.modules['oneLake_mini'] = fake_ol



------
import os
import yaml
import pandas as pd
import pytest
from pyspark.sql import SparkSession

# import the very function we want to test
from ecbr_card_self_service.edq.local_run.runEDQ import main

class DummyEngine(types.SimpleNamespace):
    def __init__(self):
        super().__init__()
        self.called = None

    def execute_rules(self, df, job_id, client_id, client_secret, environment):
        # record the arguments we were called with
        self.called = (df, job_id, client_id, client_secret, environment)
        # return the shape your code expects
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 0,
                "row_level_results": []
            }
        }

class FakeReader:
    def format(self, fmt): return self
    def option(self, k, v): return self
    def load(self, path): 
        # for our test, path should match LOCAL_DATA_PATH
        assert os.path.isfile(path)
        return "FAKE_DF"

class FakeSpark:
    def __init__(self):
        self.read = FakeReader()

class FakeBuilder:
    def __init__(self): pass
    def appName(self, name): return self
    def config(self, k, v): return self
    def getOrCreate(self): return FakeSpark()

@pytest.fixture(autouse=True)
def stub_engine(monkeypatch):
    """
    Automatically run before every test in this folder:
     - swap out edq_lib.engine for our DummyEngine
     - swap out SparkSession.builder
    """
    import edq_lib
    dummy = DummyEngine()
    edq_lib.engine = dummy
    monkeypatch.setattr(SparkSession, "builder", FakeBuilder())
    return dummy

def test_runEDQ_local_only(tmp_path, stub_engine, monkeypatch):
    # 1) create a tiny CSV for LOCAL_DATA_PATH
    data_file = tmp_path / "data.csv"
    pd.DataFrame({"x":[1,2,3], "y":["a","b","c"]}).to_csv(data_file, index=False)
    
    # 2) write config.yaml and secrets.yaml into tmp_path
    config = {
        "JOB_ID": "myJob",
        "DATA_SOURCE": "LOCAL",
        "LOCAL_DATA_PATH": str(data_file),
        # those two aren’t used in LOCAL path but main() will still load them
        "ONE_LAKE_CATALOG_ID": "unused",
        "ONE_LAKE_LOAD_PARTITION_DATE": "unused"
    }
    secrets = {
        "CLIENT_ID":     "cid123",
        "CLIENT_SECRET": "csecret456"
    }
    (tmp_path/"config.yaml").write_text(yaml.safe_dump(config))
    (tmp_path/"secrets.yaml").write_text(yaml.safe_dump(secrets))
    
    # 3) force runEDQ to look in tmp_path for its two YAML files
    import ecbr_card_self_service.edq.local_run.runEDQ as mod
    monkeypatch.setattr(mod, "project_root", str(tmp_path))

    # 4) actually call main()
    #    (it will read our config, read the CSV via FakeSpark, then call our stub_engine)
    main()

    # 5) assert that execute_rules was invoked exactly once
    df_arg, job_id, client_id, client_secret, env = stub_engine.called
    assert df_arg == "FAKE_DF"
    assert job_id   == config["JOB_ID"]
    assert client_id     == secrets["CLIENT_ID"]
    assert client_secret == secrets["CLIENT_SECRET"]
    assert env == "NonProd"
