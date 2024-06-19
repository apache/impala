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
