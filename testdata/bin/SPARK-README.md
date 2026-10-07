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
# Setting up Spark in the development environment

These scripts are the Spark counterpart of the Trino ones described in
[TRINO-README.md](./TRINO-README.md): they run Apache Spark in a Docker container
wired to the same HMS and HDFS as the Impala minicluster, so a table written by
one engine is visible to the other.

We can run the Spark container via:
`testdata/bin/run-spark.sh`

We can connect to a SQL shell by:
`testdata/bin/spark-sql.sh`

There is no image to build: the official Spark image is used unmodified, and
`run-spark.sh` bind mounts the Iceberg runtime jar and a conf directory it
assembles from the minicluster configs, passing everything else as environment
variables. The jar is downloaded from Maven Central on the first run and reused
afterwards; the conf directory is reassembled on every run, so a changed
minicluster configuration is picked up by restarting the container. Both live
under `testdata/bin/minicluster_spark` and are gitignored. The jar is mounted as a
single file, since mounting a directory over `/opt/spark/jars` would hide the jars
that ship in the image.

Unlike Trino, Spark has no always-on server of its own, so the container runs the
Spark Thrift Server (a HiveServer2 endpoint on port 9092, with the driver UI on
9093) and `spark-sql.sh` is Beeline connecting to it from inside the container.
That keeps one `SparkSession` alive across statements instead of paying JVM
startup per query. Starting the session takes a few tens of seconds; follow it
with `docker logs -f impala-minicluster-spark`.

Because the Thrift Server speaks HiveServer2, any HiveServer2 client works, e.g.
Beeline from the host, or the Python HS2 clients the tests use, against
`jdbc:hive2://localhost:9092`. The container runs with `--network=host`, like the
Trino one, so the port is reachable from the host.

## Catalogs

`spark_catalog`, Spark's built-in catalog, is configured against our HMS and wrapped
in Iceberg's `SparkSessionCatalog`, so a table keeps the name Impala knows it by,
whether it is an Iceberg table or a legacy Hive one, with no catalog prefix:
```
select count(*) from functional.alltypes;
```

The Iceberg catalogs are named like their Trino equivalents: `iceberg` for tables
in our HMS HiveCatalog, `iceberg_rest` for the REST catalog started by
`tests/common/iceberg_rest_server.py`, and `iceberg_lakekeeper` for the
Lakekeeper catalog started by `testdata/bin/run-lakekeeper.sh`. The last two only
resolve while those servers are up; otherwise any statement against them fails
with an Iceberg `RESTException`. There is also `iceberg_hadoop`, a HadoopCatalog
in HDFS with no HMS involvement.


## Picking the Spark version

The Spark image tag and the Iceberg jar are pinned to specific releases so
behaviour is reproducible. Overrides, all read by `run-spark.sh`:

* `IMPALA_SPARK_VERSION` (default `4.1.3`) - the Spark release, which also selects
  the Iceberg runtime (`iceberg-spark-runtime-<spark minor>_<scala>`).
* `IMPALA_SPARK_ICEBERG_VERSION` (default `1.12.0`) - the Iceberg version to fetch
  from Maven Central. Deliberately decoupled from the Iceberg version Impala builds
  against: `$IMPALA_ICEBERG_VERSION`.
* `IMPALA_SPARK_WITH_ICEBERG=false` - run without Iceberg: no jar is mounted and
  the Iceberg catalogs are left unconfigured, leaving only `spark_catalog`. Needed
  to run a Spark release that has no Iceberg runtime yet.
* `IMPALA_SPARK_SCALA_VERSION`, `IMPALA_SPARK_JAVA_VERSION`, `IMPALA_SPARK_IMAGE`
  - for images off the default naming scheme, e.g. Spark 3.5, which is built with
  Scala 2.12.

## Notes on interop

* Impala sees Spark's writes after some delay due to HMS event processor, similarly to
  Hive and Trino writes.
* The image uses Spark's bundled Hive 2.3 metastore client against our Hive 3 HMS.
* Spark runs with `HADOOP_USER_NAME=$USER` so that it can write to the minicluster
  HDFS. Unlike the Trino image this is passed to `docker run` rather than baked in.
* Since the image is unmodified, the `spark` user's home directory is
  `/nonexistent`, so Beeline prints a harmless `Failed to create directory:
  /nonexistent/.beeline` warning; it only means history is not saved.

## Interop tests

There is no automated Impala <-> Spark interop suite yet; these scripts are for
manual use and for generating test data.
