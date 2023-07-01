#!/bin/bash
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
#
# add_thrift_python_namespace.sh IN_THRIFT_FILE OUT_THRIFT_FILE
#
# This script reads $IN_THRIFT_FILE and adds a python namespace
# (replacing any existing python namespace) with
# impala_thrift_gen.${BASE_NAME} where the BASE_NAME is the
# thrift filename without the ".thrift". i.e. Foo.thrift uses
# impala_thrift_gen.Foo python namespace. It writes the resulting
# thrift file to $OUT_THRIFT_FILE.
#
# This logic is taken from Impyla's impala/thrift/process_thrift.sh
# script with minor changes. This requires that the source thrift
# file have at least one preexisting non-python namespace. That is
# true for all of the Thrift files that we care about.

set -eou pipefail

THRIFT_FILE_IN=$1
THRIFT_FILE_OUT=$2

FILE_NAME=$(basename $THRIFT_FILE_IN)
BASE_NAME=${FILE_NAME%.*}
# Awk script to add the python namespace before the first namespace
# in the thrift file.
ADD_NAMESPACE_PY="
    BEGIN {
        n = 0
    }
    {
        if (\$0 ~ /^namespace/ && n == 0) {
            print \"namespace py impala_thrift_gen.$BASE_NAME\";
            n += 1;
        }
        print \$0;
    }"

# Remove any existing python namespace, then add our namespace
cat $THRIFT_FILE_IN | grep -v "^namespace py" | awk "$ADD_NAMESPACE_PY" > $THRIFT_FILE_OUT
