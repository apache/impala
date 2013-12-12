#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Runs compute table stats over the Impala test tables.
#
set -e
set -u

# Run compute stats over as many of the tables used in the Planner tests as possible.
python ${IMPALA_HOME}/tests/util/compute_table_stats.py --db_names=functional\
    --table_names="alltypes,alltypesagg,alltypesaggmultifilesnopart,alltypesaggnonulls,
    alltypessmall,alltypestiny,jointbl,dimtbl,emptytable"
python ${IMPALA_HOME}/tests/util/compute_table_stats.py --db_name=functional_hbase\
    --table_names="alltypessmall,stringids"
python ${IMPALA_HOME}/tests/util/compute_table_stats.py --db_names=tpch \
    --table_names=customer,lineitem,nation,orders,part,partsupp,region,supplier
python ${IMPALA_HOME}/tests/util/compute_table_stats.py --db_names=tpcds
