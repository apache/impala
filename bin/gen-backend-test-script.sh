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

# This script generates a bash script to run a certain set of backend tests
# using the unified backend test executable. It take two positional arguments:
# 1: The file location to write the generated script
# 2: The test filter pattern that describes which tests to run (following gtest's
#    --gtest_filter)
# This script is used by the build system and is not intended to be run directly.
TEST_EXEC_LOCATION=${1}
GTEST_FILTER=${2}
TEST_EXEC_NAME=$(basename "${TEST_EXEC_LOCATION}")

cat > "${TEST_EXEC_LOCATION}" << EOF
#!/bin/bash
${IMPALA_HOME}/be/build/latest/service/unifiedbetests \
  --gtest_filter=${GTEST_FILTER} \
  --gtest_output=xml:${IMPALA_BE_TEST_LOGS_DIR}/${TEST_EXEC_NAME}.xml \
  -log_filename="${TEST_EXEC_NAME}" \
  "\$@"
# Running a gtest executable using the -gtest_filter flag produces a JUnitXML that
# contains all the tests whether they ran or not. The tests that ran are populated
# normally. The tests that didn't run have status="notrun". This is a problem for the
# unified binary, because every JUnitXML would contain every test, with a large portion
# of them marked as notrun. Various consumers of JUnitXML (such as Jenkins) may not
# understand this distinction, so it is useful to prune the notrun entries. The
# junitxml_prune_notrun.py walks through the JUnitXML file and drops the entries that
# did not run, producing a new JUnitXML file of the same name.
${IMPALA_HOME}/bin/junitxml_prune_notrun.py \
  -f "${IMPALA_BE_TEST_LOGS_DIR}/${TEST_EXEC_NAME}.xml"
EOF

chmod +x "${TEST_EXEC_LOCATION}"
