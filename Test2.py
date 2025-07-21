
import io
import sys
import unittest
from unittest.mock import patch, mock_open, MagicMock
import yaml

# prevent hard imports inside runEDQ
sys.modules["edq_lib"] = MagicMock()
sys.modules["boto3"]    = MagicMock()
sys.modules["oneLake_minij"] = MagicMock()      # <- typo? keep actual name

# import after stubbing heavy deps
from ecbr_card_self_service.edq.local_run import runEDQ
main, engine, SparkSession, logger = (
    runEDQ.main, runEDQ.engine, runEDQ.SparkSession, runEDQ.logger
)

class TestRunEDQ(unittest.TestCase):

    def build_open_side_effect(self, cfg_dict, sec_dict):
        """
        Returns a side-effect function that yields two
        in-memory file-handles whose read() returns the
        dumped YAML strings (config first, secrets second).
        """
        cfg_str  = yaml.dump(cfg_dict)
        sec_str  = yaml.dump(sec_dict)

        def _open_side_effect(*args, **kwargs):
            path = args[0]
            if path.endswith("config.yml"):
                return io.StringIO(cfg_str)
            elif path.endswith("secrets.yml"):
                return io.StringIO(sec_str)
            else:                                 # anything else → real open
                return open_original(*args, **kwargs)

        return _open_side_effect


    @patch("ecbr_card_self_service.edq.local_run.runEDQ.yaml.safe_load")
    @patch("builtins.open", new_callable=mock_open)
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.boto3.Session")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.SparkSession")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.engine.execute_rules")
    @patch("ecbr_card_self_service.edq.local_run.runEDQ.logger")
    def test_main_s3(
        self,
        mock_logger,
        mock_exec_rules,
        mock_spark_cls,
        mock_boto_sess,
        mock_open_file,
        mock_yaml_load,
    ):
        # ----------------- A) config & secrets we expect -----------------
        cfg = {
            "DATA_SOURCE": "s3",
            "S3_DATA_PATH": "s3://bucket/base_segment.csv",
            "JOB_ID": "JOB_S3",
            "AWS_PROFILE": "my-profile",
        }
        sec = {
            "CLIENT_ID": "CID_S3",
            "CLIENT_SECRET": "CSEC_S3",
        }

        # feed the YAML loader: first call → config, second → secrets
        mock_yaml_load.side_effect = [cfg, sec]

        # still patch open so we never hit the real filesystem
        global open_original      # keep reference for fallback use
        open_original = open      # noqa: E402
        mock_open_file.side_effect = self.build_open_side_effect(cfg, sec)

        # ----------------- B) boto3 stub -----------------
        fake_creds = MagicMock(access_key="AK", secret_key="SK", token=None)
        mock_boto_sess.return_value.get_credentials.return_value\
            .get_frozen_credentials.return_value = fake_creds

        # ----------------- C) Spark stub -----------------
        fake_df = MagicMock(name="df_s3")
        mock_spark = MagicMock(name="spark_s3")
        mock_spark.read.format.return_value.option.return_value.load.return_value = fake_df
        mock_spark_cls.builder.appName.return_value.config.return_value\
            .config.return_value.getOrCreate.return_value = mock_spark

        # ----------------- D) engine.execute_rules stub -----------------
        mock_exec_rules.return_value = {
            "result_type": "ExecutionCompleted",
            "job_execution_result": {
                "results": [],
                "total_DF_rows": 0,
                "row_level_results": MagicMock(),
            },
        }

        # ----------------- E) RUN -----------------
        main()

        # ----------------- F) ASSERT -----------------
        # correct branch?
        mock_logger.info.assert_any_call("Loading data from S3 URI")
        mock_spark.read.format.assert_called_once_with("csv")
        mock_exec_rules.assert_called_once_with(
            fake_df, "JOB_S3", "CID_S3", "CSEC_S3", "S3"
        )

if __name__ == "__main__":
    unittest.main()
