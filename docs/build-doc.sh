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

set -euo pipefail

function usage() {
  echo "$0 <file_format> <output_file> <filter_file> <log_file> [--pretty]"
  exit 1
}

[[ $# -eq 4 || $# -eq 5 ]] || usage

FILE_FORMAT=$1
OUTPUT_FILE=$2
FILTER_FILE=$3
LOG_FILE=$4

if [[ $# -eq 4 || $5 != "--pretty" ]]; then
  dita -i impala.ditamap -f ${FILE_FORMAT} -o ${OUTPUT_FILE} -filter ${FILTER_FILE} 2>&1 \
    | tee ${LOG_FILE}
else
  dita -i impala.ditamap -f ${FILE_FORMAT} -o ${OUTPUT_FILE} \
    -Dnav-toc=partial \
    -Dargs.copycss=yes \
    -Dargs.csspath=css \
    -Dargs.cssroot=$PWD/css \
    -Dargs.css=dita-ot-doc.css \
    -Dargs.hdr=$PWD/shared/header.xml \
    -filter ${FILTER_FILE} 2>&1 \
    | tee ${LOG_FILE}
fi

[[ -z $(grep "\[ERROR\]" ${LOG_FILE}) ]] || exit 1
