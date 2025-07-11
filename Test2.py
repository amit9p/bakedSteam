
import sys
import types
import pytest
from pathlib import Path
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """A real local SparkSession for any tests that need one."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("pytest-spark") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(autouse=True)
def stub_external_libs(monkeypatch):
    """
    Before ANY test or import runs:
      1) Inject a fake edq_lib.engine.execute_rules
      2) Inject a fake boto3.Session
      3) Inject a fake oneLake_mini.OneLakeSession
    """

    # 1) stub edq_lib.engine
    fake_engine = types.SimpleNamespace()
    fake_engine.calls = []

    def fake_execute_rules(df, job_id, client_id, client_secret, environment):
        fake_engine.calls.append({
            "df": df,
            "job_id": job_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "environment": environment,
        })
        return {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": df.count() if hasattr(df, "count") else 0,
                "row_level_results": [],
            }
        }

    fake_engine.execute_rules = fake_execute_rules
    monkeypatch.setitem(sys.modules, "edq_lib", types.SimpleNamespace(engine=fake_engine))

    # 2) stub boto3
    class FakeCredentials:
        def __init__(self): 
            self.access_key = "AK"; self.secret_key = "SK"; self.token = None
    class FakeSession:
        def __init__(self, **kwargs): pass
        def get_credentials(self): return self
        def get_frozen_credentials(self): return FakeCredentials()
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=lambda **kw: FakeSession()))

    # 3) stub oneLake_mini
    class FakeOneLakeSession:
        def __init__(self, **kwargs): pass
        def get_dataset(self, dataset_id):
            # pretend the dataset lives in cwd
            return types.SimpleNamespace(get_s3fs=lambda: None, location=str(Path.cwd()))
    monkeypatch.setitem(sys.modules, "oneLake_mini", types.SimpleNamespace(OneLakeSession=FakeOneLakeSession))

    return fake_engine

------

import os
import yaml
import pytest
from pathlib import Path

def test_runEDQ_local_only(tmp_path, spark, stub_external_libs, monkeypatch):
    # 1) Create a tiny CSV under tmp_path/data/data.csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    csv = data_dir / "data.csv"
    csv.write_text("x,y\n1,a\n2,b\n3,c\n")

    # 2) Write config.yaml & secrets.yaml so runEDQ.main() will pick them up
    config = {
        "LOCAL_DATA_PATH": str(data_dir),
        "JOB_ID": "JOB123",
        "DATA_SOURCE": "LOCAL",
    }
    (tmp_path / "config.yaml").write_text(yaml.dump(config))

    secrets = {
        "CLIENT_ID": "CID",
        "CLIENT_SECRET": "CSECRET",
    }
    (tmp_path / "secrets.yaml").write_text(yaml.dump(secrets))

    # 3) cd into tmp_path so runEDQ finds config.yaml & secrets.yaml
    monkeypatch.chdir(tmp_path)

    # 4) now import & run your code under test
    #    this import happens *after* our autouse stub fixture primed sys.modules
    from ecbr_card_self_service.edq.local_run.runEDQ import main
    main()

    # 5) Assert that our fake engine got called exactly once
    calls = stub_external_libs.calls
    assert len(calls) == 1

    call = calls[0]
    # at minimum we know it used our JOB_ID and ran in "NonProd"
    assert call["job_id"] == "JOB123"
    assert call["environment"] == "NonProd"

    # and that the dataframe passed had the right number of rows
    # (we wrote 3 lines)
    assert call["df"].count() == 3
