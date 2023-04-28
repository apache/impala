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

# Utility script that invokes the passed in thrift command but deduplicates
# and filters the output if the command is successful.

COMMAND=("$@")

TMP_FILE=$(mktemp -q)

"${COMMAND[@]}" > ${TMP_FILE} 2>&1
COMMAND_RET_CODE=${PIPESTATUS[0]}

if [[ ${COMMAND_RET_CODE} -ne 0 ]]; then
    # If the command failed, print all the output without sorting or filtering.
    cat ${TMP_FILE}
else
    # If the command succeeded, then it is safe to deduplicate the output and
    # filter out warnings that are not useful.
    #
    # Thrift can print some warnings from included Thrift files, not just
    # the top-level Thrift file. Some commonly used Thrift files can be
    # included several times by several other included Thrift files, leading
    # to many copies of the same warning. Sorting and deduplicating reduces
    # the output considerably (i.e. '| sort | uniq' below)
    #
    # Ignored output:
    # 1. '64-bit constant "34359738368" may not work in all languages.'
    # 2. 'The "byte" type is a compatibility alias for "i8". Use "i8" to
    #    emphasize the signedness of this type.'
    # 3. Empty lines ("^$")
    cat ${TMP_FILE} \
        | sort | uniq \
        | grep -v "64-bit constant.* may not work in all language." \
        | grep -v 'The "byte" type is a compatibility alias for "i8".' \
        | grep -v "^$"
fi

rm -f ${TMP_FILE}
exit "${COMMAND_RET_CODE}"
