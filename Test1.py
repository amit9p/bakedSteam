
import json
from datetime import datetime

def generate_report(responses, file_prefix="validation_report"):
    """
    Takes a list of JSON/dict responses and writes them to a text file.
    Each response will be formatted as JSON and separated by blank lines.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{file_prefix}_{timestamp}.txt"

    with open(file_name, "w") as f:
        for response in responses:
            # convert dict to nicely formatted JSON string
            f.write(json.dumps(response, indent=2))
            f.write("\n\n")  # separate entries with blank lines

    print(f"✅ Report generated: {file_name}")
    return file_name


import json

data = json.loads(response.content.decode('utf-8'))
submission_id = data.get("fileSubmissionId")
print(submission_id)



submission_id = response_json.get("fileSubmissionId")
print(submission_id)

import json
import logging
from typing import Any, Dict, Optional

log = logging.getLogger(__name__)

def file_submission_id_from(resp: Any) -> Optional[str]:
    """
    Works with:
      - requests.Response
      - dict (already parsed)
      - JSON string
  collected_ids = []

resp = publishFile(payload)     # your call
fid = file_submission_id_from(resp)
if fid:
    collected_ids.append(fid)
else:
    # Inspect raw to see what came back
    body = resp.text if hasattr(resp, "text") else (resp[:300] if isinstance(resp, str) else str(resp))
    logging.error("No fileSubmissionId in response. Status=%s Body(head)=%r",
                  getattr(resp, "status_code", "?"), body)  Returns the 'fileSubmissionId' or None.
    """
    try:
        # 1) requests.Response
        if hasattr(resp, "json") and callable(getattr(resp, "json")):
            try:
                data: Dict[str, Any] = resp.json()
            except ValueError:
                # Body not JSON (could be 404 HTML) -> fall back to text
                log.error("Response body is not JSON. Status=%s Body(head)=%r",
                          getattr(resp, "status_code", "?"), resp.text[:200])
                return None

        # 2) dict
        elif isinstance(resp, dict):
            data = resp

        # 3) JSON string
        elif isinstance(resp, str):
            data = json.loads(resp)

        else:
            log.error("Unsupported response type: %s", type(resp))
            return None

        fid = data.get("fileSubmissionId")
        if not fid:
            # Some APIs nest IDs; adapt here if needed (e.g., data['result']['fileSubmissionId'])
            log.error("fileSubmissionId key not found. Keys present: %s", list(data.keys()))
        return fid

    except Exception:
        log.exception("Failed to extract fileSubmissionId")
        return None

____

collected_ids = []

resp = publishFile(payload)     # your call
fid = file_submission_id_from(resp)
if fid:
    collected_ids.append(fid)
else:
    # Inspect raw to see what came back
    body = resp.text if hasattr(resp, "text") else (resp[:300] if isinstance(resp, str) else str(resp))
    logging.error("No fileSubmissionId in response. Status=%s Body(head)=%r",
                  getattr(resp, "status_code", "?"), body)
