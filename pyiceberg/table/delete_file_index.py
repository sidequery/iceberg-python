# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from bisect import bisect_left
from typing import TYPE_CHECKING

from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import EqualTo
from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
from pyiceberg.manifest import INITIAL_SEQUENCE_NUMBER, POSITIONAL_DELETE_SCHEMA, DataFile, DataFileContent, ManifestEntry
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField

if TYPE_CHECKING:
    from pyiceberg.schema import Schema

PATH_FIELD_ID = 2147483546


class PositionDeletes:
    """Collects position delete files and indexes them by sequence number."""

    __slots__ = ("_buffer", "_seqs", "_files")

    def __init__(self) -> None:
        self._buffer: list[tuple[DataFile, int]] | None = []
        self._seqs: list[int] = []
        self._files: list[tuple[DataFile, int]] = []

    def add(self, delete_file: DataFile, seq_num: int) -> None:
        if self._buffer is None:
            raise ValueError("Cannot add files after indexing")
        self._buffer.append((delete_file, seq_num))

    def _ensure_indexed(self) -> None:
        if self._buffer is not None:
            self._files = sorted(self._buffer, key=lambda file: file[1])
            self._seqs = [seq for _, seq in self._files]
            self._buffer = None

    def filter_by_seq(self, seq: int) -> list[DataFile]:
        self._ensure_indexed()
        if not self._files:
            return []
        start_idx = bisect_left(self._seqs, seq)
        return [delete_file for delete_file, _ in self._files[start_idx:]]

    def referenced_delete_files(self) -> list[DataFile]:
        self._ensure_indexed()
        return [data_file for data_file, _ in self._files]


class EqualityDeletes(PositionDeletes):
    """Collect equality delete files indexed by the newest data sequence they may affect."""

    def add(self, delete_file: DataFile, seq_num: int) -> None:
        # Equality deletes apply only to data whose sequence is strictly less than
        # the delete sequence. Indexing at seq - 1 lets the shared >= lookup encode
        # that rule without special cases at lookup time.
        super().add(delete_file, seq_num - 1)


def _has_path_bounds(delete_file: DataFile) -> bool:
    lower = delete_file.lower_bounds
    upper = delete_file.upper_bounds
    if not lower or not upper:
        return False

    return PATH_FIELD_ID in lower and PATH_FIELD_ID in upper


def _applies_to_data_file(delete_file: DataFile, data_file: DataFile) -> bool:
    if not _has_path_bounds(delete_file):
        return True

    evaluator = _InclusiveMetricsEvaluator(POSITIONAL_DELETE_SCHEMA, EqualTo("file_path", data_file.file_path))
    return evaluator.eval(delete_file)


def _is_all_null(data_file: DataFile, field_id: int) -> bool:
    null_counts = data_file.null_value_counts
    value_counts = data_file.value_counts
    if not null_counts or not value_counts:
        return False
    null_count = null_counts.get(field_id)
    value_count = value_counts.get(field_id)
    return null_count is not None and value_count is not None and null_count == value_count


def _has_no_nulls(data_file: DataFile, field_id: int) -> bool:
    null_counts = data_file.null_value_counts
    return bool(null_counts) and null_counts.get(field_id) == 0


def _contains_null(data_file: DataFile, field: NestedField) -> bool:
    if field.required:
        return False
    null_counts = data_file.null_value_counts
    if not null_counts:
        return True
    null_count = null_counts.get(field.field_id)
    return null_count is None or null_count > 0


def _equality_delete_applies_to_data_file(delete_file: DataFile, data_file: DataFile, schema: Schema) -> bool:
    """Conservatively prune equality deletes whose metrics cannot match a data file."""
    if not delete_file.equality_ids:
        return True

    for field_id in delete_file.equality_ids:
        try:
            field = schema.find_field(field_id)
        except ValueError:
            # A dropped field can still exist in older data and delete files.
            return True
        if not field.field_type.is_primitive:
            continue

        if _contains_null(data_file, field) and _contains_null(delete_file, field):
            continue
        if _is_all_null(data_file, field_id) and _has_no_nulls(delete_file, field_id):
            return False
        if _is_all_null(delete_file, field_id) and _has_no_nulls(data_file, field_id):
            return False

        delete_lower = delete_file.lower_bounds
        delete_upper = delete_file.upper_bounds
        data_lower = data_file.lower_bounds
        data_upper = data_file.upper_bounds
        if (
            delete_lower
            and delete_upper
            and data_lower
            and data_upper
            and field_id in delete_lower
            and field_id in delete_upper
            and field_id in data_lower
            and field_id in data_upper
        ):
            field_type = field.field_type
            if (
                from_bytes(field_type, delete_upper[field_id]) < from_bytes(field_type, data_lower[field_id])
                or from_bytes(field_type, delete_lower[field_id]) > from_bytes(field_type, data_upper[field_id])
            ):
                return False

    return True


def _referenced_data_file_path(delete_file: DataFile) -> str | None:
    """Return the path, if the path bounds evaluate to the same location."""
    lower_bounds = delete_file.lower_bounds
    upper_bounds = delete_file.upper_bounds

    if not lower_bounds or not upper_bounds:
        return None

    lower = lower_bounds.get(PATH_FIELD_ID)
    upper = upper_bounds.get(PATH_FIELD_ID)

    if lower and upper and lower == upper:
        try:
            return lower.decode("utf-8")
        except (UnicodeDecodeError, AttributeError):
            pass

    return None


def _partition_key(spec_id: int, partition: Record | None) -> tuple[int, Record]:
    if partition:
        return spec_id, partition
    return spec_id, Record()  # unpartitioned handling


class DeleteFileIndex:
    """Index position and equality delete files by their Iceberg applicability rules."""

    def __init__(self, schema: Schema | None = None) -> None:
        self._schema = schema
        self._by_partition: dict[tuple[int, Record], PositionDeletes] = {}
        self._by_path: dict[str, PositionDeletes] = {}
        self._equality_deletes: dict[tuple[int, Record] | None, EqualityDeletes] = {}

    def is_empty(self) -> bool:
        return not self._by_partition and not self._by_path and not self._equality_deletes

    def add_delete_file(self, manifest_entry: ManifestEntry, partition_key: Record | None = None) -> None:
        delete_file = manifest_entry.data_file
        seq = manifest_entry.sequence_number or INITIAL_SEQUENCE_NUMBER
        if delete_file.content == DataFileContent.EQUALITY_DELETES:
            # An unpartitioned equality delete is global, including for partitioned
            # data files. Partitioned deletes remain scoped to their exact spec/key.
            key = _partition_key(delete_file.spec_id or 0, partition_key) if partition_key else None
            self._equality_deletes.setdefault(key, EqualityDeletes()).add(delete_file, seq)
        elif target_path := _referenced_data_file_path(delete_file):
            self._by_path.setdefault(target_path, PositionDeletes()).add(delete_file, seq)
        else:
            key = _partition_key(delete_file.spec_id or 0, partition_key)
            self._by_partition.setdefault(key, PositionDeletes()).add(delete_file, seq)

    def for_data_file(self, seq_num: int, data_file: DataFile, partition_key: Record | None = None) -> set[DataFile]:
        if self.is_empty():
            return set()

        deletes: set[DataFile] = set()
        spec_id = data_file.spec_id or 0

        key = _partition_key(spec_id, partition_key)
        if partition_deletes := self._by_partition.get(key):
            for delete_file in partition_deletes.filter_by_seq(seq_num):
                if _applies_to_data_file(delete_file, data_file):
                    deletes.add(delete_file)

        if path_deletes := self._by_path.get(data_file.file_path):
            deletes.update(path_deletes.filter_by_seq(seq_num))

        candidates: list[DataFile] = []
        if partition_equality_deletes := self._equality_deletes.get(key):
            candidates.extend(partition_equality_deletes.filter_by_seq(seq_num))
        if global_equality_deletes := self._equality_deletes.get(None):
            candidates.extend(global_equality_deletes.filter_by_seq(seq_num))

        for delete_file in candidates:
            if self._schema and not _equality_delete_applies_to_data_file(delete_file, data_file, self._schema):
                continue
            deletes.add(delete_file)

        return deletes

    def referenced_delete_files(self) -> list[DataFile]:
        data_files: list[DataFile] = []

        for deletes in self._by_partition.values():
            data_files.extend(deletes.referenced_delete_files())

        for deletes in self._by_path.values():
            data_files.extend(deletes.referenced_delete_files())

        for deletes in self._equality_deletes.values():
            data_files.extend(deletes.referenced_delete_files())

        return data_files
