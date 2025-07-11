
# tests/edq/test_runEDQ_local_only.py
import sys
import types
import pytest
from pathlib import Path
from pyspark.sql import SparkSession
import yaml

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest") \
        .getOrCreate()

@pytest.fixture(autouse=True)
def stub_everything(monkeypatch):
    # 1) stub edq_lib.engine.execute_rules
    dummy_engine = types.SimpleNamespace()
    called = {}
    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        called['args'] = (job_id, client_id, client_secret, environment)
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 1,
                "row_level_results": []
            }
        }
    dummy_engine.execute_rules = fake_execute_rules
    # inject edq_lib
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
            return types.SimpleNamespace(
                get_s3fs=lambda: None,
                location=str(Path.cwd())
            )
    onelake_mod = types.SimpleNamespace(OneLakeSession=FakeOneLakeSession)
    monkeypatch.setitem(sys.modules, "oneLake_mini", onelake_mod)

    return called

def test_runEDQ_local_only(tmp_path: Path, spark, stub_everything, monkeypatch):
    # 1) write one‐row CSV
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "part-00000.csv").write_text("x,y\n1,foo\n")

    # 2) write config.yaml + secrets.yaml
    (tmp_path / "config.yaml").write_text(
        yaml.dump({"LOCAL_DATA_PATH": str(data_dir)})
    )
    (tmp_path / "secrets.yaml").write_text(
        yaml.dump({"CLIENT_ID":"CID","CLIENT_SECRET":"CSEC"})
    )

    # 3) cd so runEDQ picks up those files
    monkeypatch.chdir(tmp_path)

    # 4) now import *after* our stubs exist
    from ecbr_card_self_service.edq.local_run.runEDQ import main
    main()

    # 5) assert our fake engine was called correctly
    job_id, cid, csec, env = stub_everything['args']
    assert cid == "CID"
    assert csec == "CSEC"
    assert env == "local"
