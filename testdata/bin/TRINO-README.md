<!---
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
-->
# Setting up Trino in the development environment

Impala has a couple of scripts that make it easy to run Trino in the dev environment.

First we need to build our custom Trino docker image via:
`testdata/bin/build-trino-docker-image.sh`

Then we can run the Trino container via:
`testdata/bin/run-trino.sh`

We can connect to the Trino CLI by:
`testdata/bin/trino-cli.sh`

Trino will be configured to access our HMS and HDFS. We add the 'hive' and
'iceberg' catalogs for Trino. Legacy Hive tables can be accessed via the 'hive'
catalog, while Iceberg tables (only the ones that reside in HiveCatalog) can be
accessed via the 'iceberg' catalog. E.g.:
```
trino> use iceberg.functional_parquet;
trino:functional_parquet> select count(*) from iceberg_mixed_file_format;
 _col0
-------
     3
(1 row)
```

# Running the Impala <-> Trino interop tests

There are two custom cluster interop suites, both driving Trino through the
containerized Trino CLI (so no extra Python dependency is required):

* `tests/custom_cluster/test_iceberg_trino_interop.py` exercises interop over
  Iceberg V3 tables (INSERT, deletion-vector DELETE/UPDATE/MERGE, and column
  default values) via Trino's `iceberg` catalog.
* `tests/custom_cluster/test_trino_interop.py` is a minimal suite over legacy
  (non-Iceberg) Hive tables via Trino's `hive` catalog: Trino reads tables Impala
  exposes (including the standard `functional` database), and Impala reads a
  database + ORC table that Trino creates and writes.

Both are skipped only on non-HDFS filesystems. They build the image (if missing)
and start the container automatically; if the Trino container or Docker is
unavailable the tests fail (rather than skipping silently) so the problem is
visible. Run them with:
```
impala-py.test tests/custom_cluster/test_iceberg_trino_interop.py
impala-py.test tests/custom_cluster/test_trino_interop.py
```
You can still build/start manually (e.g. to iterate on the image):
```
testdata/bin/build-trino-docker-image.sh
testdata/bin/run-trino.sh
```

Both suites request the Trino container by decorating the class with
`@CustomClusterTestSuite.with_args(run_trino=True)`, which starts the container
before the Impala cluster (leaving an already-running container untouched) and
stops it on teardown only if it started it. Impala reads Trino's writes after an
`INVALIDATE METADATA`/`REFRESH`; Trino reads Impala's writes without an explicit
refresh.

Note on the `hive` catalog: in this minicluster the HMS metadata transformer converts
the MANAGED tables Trino requests into EXTERNAL ones (Trino does not advertise Hive ACID
write capabilities), tagging them `TRANSLATED_TO_EXTERNAL=true`. The image sets
`hive.non-managed-table-writes-enabled=true` so Trino can still `INSERT` into them;
otherwise writes fail with "Cannot write to non-managed Hive table". Also prefer ORC (or
TEXTFILE) over Parquet for Trino-written tables Impala reads back: Trino's Parquet writer
uses `DELTA_LENGTH_BYTE_ARRAY` for string columns, which Impala cannot read yet, and the
`hive` connector (unlike `iceberg`) exposes no session property to disable it.

## Authoring interop tests: TRINO_QUERY / RESULTS

`.test` files may contain `TRINO_QUERY` sections (statements run against Trino in
the `iceberg` catalog, using the test's database as the schema), analogous to
`HIVE_QUERY`. A `TRINO_QUERY` that returns rows can be verified against a
`RESULTS` section (which accepts an optional `VERIFY_*` modifier), exactly as
`HIVE_QUERY` and `QUERY` do. Result checking is type-aware: the framework uses
Trino's `DESCRIBE OUTPUT` metadata, and an optional `TYPES` section checks the
normalized output types. Common Trino names are mapped to the existing `.test`
vocabulary, such as `integer` to `INT`, `real` to `FLOAT`, unbounded `varchar`
to `STRING`, and `varbinary` to `BINARY`. Type parameters are omitted, as they
are for Impala results, so `decimal(10,2)` is `DECIMAL` and `varchar(10)` is
`VARCHAR`.

Rows use the same textual convention as `RESULTS`: strings are single-quoted,
numbers/booleans are bare, and SQL NULL is written as bare `NULL`. Note that
Trino values represented as JSON strings, including DECIMAL and temporal
values, are quoted. Non-finite FLOAT/DOUBLE values are normalized to bare
`NaN`, `Infinity`, and `-Infinity`, matching Impala's `RESULTS` convention.
