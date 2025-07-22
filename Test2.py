
# tests/edq/test_get_partition.py
import os
import pytest
from unittest.mock import MagicMock

# import just the helper
from ecbr_card_self_service.edq.local_run.runEDQ import get_partition


# ────────────────────────────────────────────────────────────────
# helpers ─ make a fake dataset-like object with a nested s3fs stub
# ────────────────────────────────────────────────────────────────
def _make_dataset(base: str, partition: str, with_file: bool = True):
    """
    Constructs

        dataset.location == base
        dataset.get_s3fs().ls(base)       ->  [f"{base}{partition}/"]
        dataset.get_s3fs().ls(f"{base}{partition}/")
                                       ->  [f"{base}{partition}/part-00000.parquet"]

    so that get_partition(dataset, partition) succeeds.
    """
    s3fs_mock = MagicMock()
    dataset    = MagicMock()
    dataset.location          = base
    dataset.get_s3fs.return_value = s3fs_mock

    # 1️⃣ first ls(): list the partition directory
    s3fs_mock.ls.side_effect = [
        [f"{base}{partition}/"],                         # ls(base)
        [f"{base}{partition}/part-00000.parquet"]        # ls(base/partition)
        if with_file else []                             # second call may be empty
    ]
    return dataset


# ────────────────────────────────────────────────────────────────
# happy-path
# ────────────────────────────────────────────────────────────────
def test_get_partition_success():
    base       = "s3://bucket/path/"
    partition  = "2025-04-10"
    dataset    = _make_dataset(base, partition)

    result = get_partition(dataset, partition)

    expected = os.path.dirname(                           # <- mirrors helper’s logic
        dataset.get_s3fs().ls.side_effect[1][-1]
    )
    assert result == expected


# ────────────────────────────────────────────────────────────────
# error-path
# ────────────────────────────────────────────────────────────────
def test_get_partition_not_found():
    base       = "s3://bucket/path/"
    partition  = "2025-04-10"
    dataset    = _make_dataset(base, partition, with_file=False)

    with pytest.raises(ValueError, match=partition):
        get_partition(dataset, partition)
