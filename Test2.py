
import builtins
import io
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
import runEDQ  # your module

@pytest.fixture(scope="session")
def spark():
    # if you have pytest-spark, use its spark fixture;
    # otherwise spin up a local SparkSession once for all tests:
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test-runEDQ") \
        .getOrCreate()


def make_dummy_df(spark):
    # build a tiny two-column DataFrame matching your engine’s expectations:
    return spark.createDataFrame(
        [("x1", 42), ("x2", 99)],
        ["colA", "colB"]
    )


def test_main_local_branch(monkeypatch, spark, tmp_path):
    # 1) fake out the YAML loads
    fake_config = {
        "DATA_SOURCE":    "LOCAL",
        "LOCAL_DATA_PATH": str(tmp_path / "data.csv"),
        "JOB_ID":          "job-123",
    }
    fake_secrets = {
        "CLIENT_ID":     "abc-id",
        "CLIENT_SECRET": "shh-secret",
    }
    # write a dummy CSV file
    csv = tmp_path / "data.csv"
    csv.write_text("foo,bar\n1,2\n3,4\n")

    monkeypatch.setattr(runEDQ, "config_path", "ignored")
    monkeypatch.setattr(runEDQ, "secrets_path", "ignored")
    monkeypatch.setattr(runEDQ.yaml, "safe_load", lambda f: fake_config if f.name == "ignored" else fake_secrets)

    # 2) stub SparkSession.builder.getOrCreate()
    real_builder = SparkSession.builder
    class FakeBuilder:
        def __init__(self): pass
        def appName(self, _): return self
        def config(self, *_, **__): return self
        def getOrCreate(self): return spark
    monkeypatch.setattr(SparkSession, "builder", FakeBuilder())

    # 3) patch engine.execute_rules so it doesn’t run for real
    fake_result = {"result_type":"ExecutionCompleted", "job_execution_result": {"results": [], "total_DF_rows": 2}}
    mock_execute = MagicMock(return_value=fake_result)
    monkeypatch.setattr(runEDQ.engine, "execute_rules", mock_execute)

    # 4) now call main()
    runEDQ.main()

    # 5) verify we read the file you wrote, and engine was invoked
    #    with the DataFrame you expect, plus your job/creds
    mock_execute.assert_called_once()
    called_df, called_job, called_cid, called_secret, called_env = mock_execute.call_args[0]
    assert called_job == "job-123"
    assert called_cid == "abc-id"
    assert called_secret == "shh-secret"
    assert called_env == "NonProd"

    # You can also check that `called_df.count() == 2` if you like:
    assert called_df.count() == 2
