#!/usr/bin/env bash
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

# This script load 150k partitions into /test-warehouse/1k_col_tbl
# with 3 long partition keys, each 250 chars.

mkdir batch-1
cd batch-1

# Create data for "p1=0000....0001" in local file system
p1=$(printf '%*s\n' 250 "1" | tr ' ' '0')
for j in {1..1000}; do
    z=$(printf '%*s\n' 250 "$j" | tr ' ' '0')
    ppath="p1=$p1/p2=$z/p3=$z"
    delims=$(printf '%*s\n' 1000 "" | tr ' ' '|')
    mkdir -p "$ppath"; echo "${j}${delims}" > "$ppath/data.txt";
done

# Put data for "p1=0000....0001" to HDFS.
hdfs dfs -put -f "p1=$p1" /test-warehouse/1k_col_tbl

# Create the other partitions by copying from "p1=0000....0001"
for i in {2..150}; do
    pn=$(printf '%*s\n' 250 "$i" | tr ' ' '0')
    echo "loading p1=$pn"
    hdfs dfs -cp "/test-warehouse/1k_col_tbl/p1=$p1" "/test-warehouse/1k_col_tbl/p1=$pn"
done

