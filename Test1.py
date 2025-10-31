# utils/logging_setup.py
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

def setup_logging(level: int = logging.INFO, log_file: str = "logs/app.log"):
    # Create root logger once
    if logging.getLogger().handlers:
        return

    logging.Formatter.converter = time.gmtime  # UTC timestamps
    fmt = "%(asctime)sZ | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%S"

    # Console
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    sh.setLevel(level)

    # Daily rotating file (keep 14 days)
    fh = TimedRotatingFileHandler(log_file, when="midnight", backupCount=14, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    fh.setLevel(level)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(sh)
    root.addHandler(fh)

import time  # keep at bottom so it's available above


----
# main.py
import logging
from utils.logging_setup import setup_logging
from utils.config_loader import load_config, require_keys
from token_generator.get_credentials import load_secrets
from publisher.onelake_upload import publish_file
from validator.file_validator import validate_submission
from datetime import datetime

logger = logging.getLogger(__name__)

def run() -> list[dict]:
    cfg = load_config()
    require_keys(cfg, "config.json", ["s3_path", "file_list"])
    secrets = load_secrets()  # loads, but don't log secrets!

    s3_path   = cfg["s3_path"]
    file_list = cfg["file_list"]
    results: list[dict] = []

    for file_name in file_list:
        try:
            logger.info("Publishing started", extra={"file": file_name})
            resp = publish_file(s3_path, file_name)  # may raise
            file_id = resp.get("fileSubmissionId")
            if not file_id:
                raise RuntimeError("Missing fileSubmissionId in response")

            logger.info("Published OK", extra={"file": file_name, "file_id": file_id})

            status = validate_submission(file_id)  # may raise
            logger.info("Validation result", extra={"file": file_name, "file_id": file_id, "status": status})

            results.append({
                "file_name": file_name,
                "file_submission_id": file_id,
                "validation_status": status,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            })

        except Exception:
            # logs message + full stacktrace
            logger.exception("Failed processing file", extra={"file": file_name})
            results.append({
                "file_name": file_name,
                "file_submission_id": None,
                "validation_status": "FAILED",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            })
    return results

if __name__ == "__main__":
    setup_logging()  # once, at app start
    try:
        logger.info("Job start")
        summary = run()
        logger.info("Job finished", extra={"success": sum(1 for r in summary if r["validation_status"] != "FAILED"),
                                           "total": len(summary)})
    except Exception:
        logger.exception("Uncaught error in main")
        raise  # let orchestrator/CI see the failure exit code


______

# publisher/onelake_upload.py
import logging, requests

logger = logging.getLogger(__name__)

class PublishError(RuntimeError): pass

def publish_file(s3_path: str, file_name: str) -> dict:
    url = "https://example/publish"
    payload = {"s3Path": f"{s3_path}{file_name}"}
    headers = {"Content-Type": "application/json"}

    try:
        logger.debug("Publish request", extra={"file": file_name, "url": url})
        r = requests.post(url, json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        return data
    except requests.RequestException as e:
        logger.exception("HTTP error on publish", extra={"file": file_name})
        raise PublishError(str(e)) from e
    except ValueError as e:
        logger.exception("Invalid JSON in publish response", extra={"file": file_name})
        raise PublishError("Bad JSON response") from e

_____













