
# tests/edq/test_get_partition.py
import os
import pytest
from unittest.mock import MagicMock
from ecbr_card_self_service.edq.local_run.runEDQ import get_partition


def _make_dataset(base: str, partition: str, with_file: bool = True):
    s3fs_mock = MagicMock()
    dataset    = MagicMock()
    dataset.location = base
    dataset.get_s3fs.return_value = s3fs_mock
    s3fs_mock.ls.side_effect = [
        [f"{base}{partition}/"],
        [f"{base}{partition}/part-00000.parquet"] if with_file else []
    ]
    return dataset


def test_get_partition_success():
    base      = "s3://bucket/path/"
    partition = "2025-04-10"
    dataset   = _make_dataset(base, partition)

    result    = get_partition(dataset, partition)
    expected  = f"{base}{partition}"        # ← simpler & iterator-safe
    assert result == expected


def test_get_partition_not_found():
    base      = "s3://bucket/path/"
    partition = "2025-04-10"
    dataset   = _make_dataset(base, partition, with_file=False)

    with pytest.raises(ValueError, match=partition):
        get_partition(dataset, partition)
