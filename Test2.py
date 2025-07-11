
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
