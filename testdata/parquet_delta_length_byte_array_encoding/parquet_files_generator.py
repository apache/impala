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

# Generates Parquet test files that use DELTA_LENGTH_BYTE_ARRAY encoding for
# BYTE_ARRAY columns. Run from IMPALA_HOME:
#   python3 testdata/parquet_delta_length_byte_array_encoding/parquet_files_generator.py

import os

import pyarrow as pa
import pyarrow.parquet as pq

OUT_DIR = "testdata/data"

# ---- mixed_types_delta_length_byte_array.parquet --------------------------------
# A table with:
#   id        INT32     (PLAIN)
#   str_col   STRING    (DELTA_LENGTH_BYTE_ARRAY)
#   bin_col   BINARY    (DELTA_LENGTH_BYTE_ARRAY)
#
# Mix of short strings (<=11 bytes, fit in Impala's small-string optimisation)
# and long strings (>11 bytes, require keeping the page buffer alive).
# 20 non-null rows followed by a NULL row. The run of 20 identical non-null
# definition levels is long enough to trigger RLE encoding (threshold ~8/16),
# ensuring that Impala's batch decode path (DecodeValues) is exercised.

ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, None]
str_vals = [
    "hello", "longer_string!", "", "impala", "another_long_str", "world",
    "hi", "long_string_007", "abc", "long_string_010",
    "test", "long_string_012", "ok", "long_string_014",
    "short", "long_string_016", "x", "long_string_018",
    "end", "long_string_020", None,
]
bin_vals = [
    b"foo", b"longer_binary!", b"", b"baz", b"more_bin_data", b"bar",
    b"hi", b"long_binary_007", b"abc", b"long_binary_010",
    b"tst", b"long_binary_012", b"ok", b"long_binary_014",
    b"bin", b"long_binary_016", b"x", b"long_binary_018",
    b"end", b"long_binary_020", None,
]

schema = pa.schema([
    pa.field("id", pa.int32(), nullable=True),
    pa.field("str_col", pa.string(), nullable=True),
    pa.field("bin_col", pa.binary(), nullable=True),
])

table = pa.table(
    {
        "id": pa.array(ids, type=pa.int32()),
        "str_col": pa.array(str_vals, type=pa.string()),
        "bin_col": pa.array(bin_vals, type=pa.binary()),
    },
    schema=schema,
)

out_path = os.path.join(OUT_DIR, "mixed_types_delta_length_byte_array.parquet")
pq.write_table(
    table,
    out_path,
    use_dictionary=False,
    column_encoding={
        "str_col": "DELTA_LENGTH_BYTE_ARRAY",
        "bin_col": "DELTA_LENGTH_BYTE_ARRAY",
    },
    write_statistics=True,
)
print("Written: {}".format(out_path))

# ---- boundary_smallify_delta_length_byte_array.parquet --------------------------
# Two rows that straddle the smallify boundary (SMALL_LIMIT = 11):
#   id=1  str_col='11chars_str'  (11 bytes, last length that is smallified)
#   id=2  str_col='12chars_str!' (12 bytes, first length that is NOT smallified)
boundary_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("str_col", pa.string(), nullable=False),
    pa.field("bin_col", pa.binary(), nullable=False),
])
boundary_table = pa.table(
    {
        "id": pa.array([1, 2], type=pa.int32()),
        "str_col": pa.array(["11chars_str", "12chars_str!"], type=pa.string()),
        "bin_col": pa.array([b"11chars_bin", b"12chars_bin!"], type=pa.binary()),
    },
    schema=boundary_schema,
)
boundary_path = os.path.join(OUT_DIR, "boundary_smallify_delta_length_byte_array.parquet")
pq.write_table(
    boundary_table,
    boundary_path,
    use_dictionary=False,
    column_encoding={
        "str_col": "DELTA_LENGTH_BYTE_ARRAY",
        "bin_col": "DELTA_LENGTH_BYTE_ARRAY",
    },
    write_statistics=True,
)
print("Written: {}".format(boundary_path))

# ---- large_delta_length_byte_array.parquet --------------------------------------
# 2000 rows of short strings (10 bytes each, all smallified).
# With default PyArrow page size all 2000 lengths land in one data page, requiring
# FillLengthsBuffer() to refill twice (1024 + 976), exercising the streaming path.
N = 2000
large_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("str_col", pa.string(), nullable=False),
])
large_table = pa.table(
    {
        "id": pa.array(list(range(1, N + 1)), type=pa.int32()),
        "str_col": pa.array(["value_{:04d}".format(i) for i in range(N)],
                            type=pa.string()),
    },
    schema=large_schema,
)
large_path = os.path.join(OUT_DIR, "large_delta_length_byte_array.parquet")
pq.write_table(
    large_table,
    large_path,
    use_dictionary=False,
    column_encoding={"str_col": "DELTA_LENGTH_BYTE_ARRAY"},
    write_statistics=True,
)
print("Written: {}".format(large_path))

# ---- multipage_delta_length_byte_array.parquet ----------------------------------
# 2400 rows split across 2 data pages (max_rows_per_page=1200).
# Page 1: 1200 rows -> FillLengthsBuffer refills at row 1024, then continues.
# Page 2: 1200 rows -> NewPage() resets decoder state; verifies cross-page decoding.
N = 2400
multipage_schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("str_col", pa.string(), nullable=False),
])
multipage_table = pa.table(
    {
        "id": pa.array(list(range(1, N + 1)), type=pa.int32()),
        "str_col": pa.array(["value_{:04d}".format(i) for i in range(N)],
                            type=pa.string()),
    },
    schema=multipage_schema,
)
multipage_path = os.path.join(OUT_DIR, "multipage_delta_length_byte_array.parquet")
pq.write_table(
    multipage_table,
    multipage_path,
    use_dictionary=False,
    column_encoding={"str_col": "DELTA_LENGTH_BYTE_ARRAY"},
    max_rows_per_page=1200,
    write_statistics=True,
)
print("Written: {}".format(multipage_path))
