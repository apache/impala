#!/bin/bash
#
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

# Runs compute table stats over a curated set of Impala test tables.
#
set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# TODO: We need a better way of managing how these get set. See IMPALA-4346
IMPALAD=${IMPALAD:-localhost:21000}

COMPUTE_STATS_SCRIPT="${IMPALA_HOME}/tests/util/compute_table_stats.py --impalad=${IMPALAD}"

# Run compute stats over as many of the tables used in the Planner tests as possible.
${COMPUTE_STATS_SCRIPT} --db_names=functional\
    --table_names="alltypes,alltypesagg,alltypesaggmultifilesnopart,alltypesaggnonulls,
    alltypessmall,alltypestiny,jointbl,dimtbl,stringpartitionkey,nulltable,nullrows,
    date_tbl,chars_medium,part_strings_with_quotes,alltypes_date_partition,
    alltypes_date_partition_2,mv1_alltypes_jointbl,binary_tbl,binary_tbl_big"
${COMPUTE_STATS_SCRIPT} --db_names=functional_parquet \
    --table_names="unique_with_nulls"

# We cannot load HBase on s3 and isilon yet.
if [ "${TARGET_FILESYSTEM}" = "hdfs" ]; then
  ${COMPUTE_STATS_SCRIPT} --db_name=functional_hbase\
    --table_names="alltypessmall,stringids"
fi
${COMPUTE_STATS_SCRIPT} --db_names=tpch,tpch_parquet,tpch_orc_def \
    --table_names=customer,lineitem,nation,orders,part,partsupp,region,supplier
${COMPUTE_STATS_SCRIPT} --db_names=tpch_nested_parquet,tpcds,tpcds_parquet
${COMPUTE_STATS_SCRIPT} --db_names=functional_kudu,tpch_kudu

# Compute tables of tpcds_partitioned_parquet_snap serially
# due to large number of partitions in some of the fact tables.
${COMPUTE_STATS_SCRIPT} --db_names=tpcds_partitioned_parquet_snap \
    --parallelism=1
