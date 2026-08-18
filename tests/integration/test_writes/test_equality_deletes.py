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

import pyarrow as pa
import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.manifest import ManifestContent


@pytest.mark.integration
def test_large_composite_equality_upsert_is_visible_to_spark(session_catalog: Catalog, spark: SparkSession) -> None:
    identifier = "default.large_composite_equality_upsert"
    if session_catalog.table_exists(identifier):
        session_catalog.drop_table(identifier)

    initial_count = 200_000
    source_start = 100_000
    source_end = 300_000
    initial = pa.table(
        {
            "account_id": pa.array((row // 10 for row in range(initial_count)), type=pa.int64()),
            "item_id": pa.array((row % 10 for row in range(initial_count)), type=pa.int64()),
            "value": pa.array(range(initial_count), type=pa.int64()),
        }
    )
    source = pa.table(
        {
            "account_id": pa.array((row // 10 for row in range(source_start, source_end)), type=pa.int64()),
            "item_id": pa.array((row % 10 for row in range(source_start, source_end)), type=pa.int64()),
            "value": pa.array((-row for row in range(source_start, source_end)), type=pa.int64()),
        }
    )

    try:
        table = session_catalog.create_table(identifier, initial.schema, properties={"format-version": "2"})
        table.append(initial)
        table.upsert_by_equality_delete(source, join_cols=["account_id", "item_id"])

        result = table.scan().to_arrow()
        assert result.num_rows == source_end
        result_by_key = result.sort_by([("account_id", "ascending"), ("item_id", "ascending")])
        assert result_by_key.column("value")[source_start - 1].as_py() == source_start - 1
        assert result_by_key.column("value")[source_start].as_py() == -source_start
        assert result_by_key.column("value")[-1].as_py() == -(source_end - 1)

        snapshot = table.current_snapshot()
        assert snapshot is not None
        assert {manifest.content for manifest in snapshot.manifests(table.io)} == {
            ManifestContent.DATA,
            ManifestContent.DELETES,
        }

        spark_table = spark.table(identifier)
        assert spark_table.count() == source_end
        sample = spark_table.where(
            (spark_table.account_id == source_start // 10) & (spark_table.item_id == source_start % 10)
        ).collect()
        assert len(sample) == 1
        assert sample[0].value == -source_start
    finally:
        if session_catalog.table_exists(identifier):
            session_catalog.drop_table(identifier)
