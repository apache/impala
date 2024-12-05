#!/usr/bin/env ambari-python-wrap
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

# This script generates a bash script to run a certain set of backend tests. It is
# used by the build system (see be/CMakeLists.txt for usage) and is not intended to be
# run directly.

from __future__ import absolute_import, division, print_function
import argparse
import os
import stat
import textwrap

# The script header template requires substitutions to set the variables used by the body.
script_header_template = textwrap.dedent("""\
    #!/bin/bash
    GTEST_FILTER="{gtest_filter}"
    TEST_EXEC_NAME="{test_exec_name}"
""")

# The script body uses shell variables for all of its key components. It requires
# no substitutions.
script_body = textwrap.dedent(r"""
    ${IMPALA_HOME}/bin/run-jvm-binary.sh \
    ${IMPALA_HOME}/be/build/latest/service/unifiedbetests \
      --gtest_filter=${GTEST_FILTER} \
      --gtest_output=xml:${IMPALA_BE_TEST_LOGS_DIR}/${TEST_EXEC_NAME}.xml \
      -log_filename="${TEST_EXEC_NAME}" \
      "$@"
    RETURN_CODE=$?
    # Running a gtest executable using the -gtest_filter flag produces a JUnitXML that
    # contains all the tests whether they ran or not. The tests that ran are populated
    # normally. The tests that didn't run have status="notrun". This is a problem for the
    # unified binary, because every JUnitXML would contain every test, with a large
    # portion of them marked as notrun. Various consumers of JUnitXML (such as Jenkins)
    # may not understand this distinction, so it is useful to prune the notrun entries.
    # The junitxml_prune_notrun.py walks through the JUnitXML file and drops the entries
    # that did not run, producing a new JUnitXML file of the same name.
    ${IMPALA_HOME}/bin/junitxml_prune_notrun.py  \
      -f "${IMPALA_BE_TEST_LOGS_DIR}/${TEST_EXEC_NAME}.xml"
    exit ${RETURN_CODE}
""")


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--gtest_filter", required=True,
    help="The location to write the generated script")
  parser.add_argument("--test_script_output", required=True,
    help="The test filter pattern to pass to gtest to limit the set of tests run")
  args = parser.parse_args()
  with open(args.test_script_output, "w") as f:
    test_exec_name = os.path.basename(args.test_script_output)
    script_header = script_header_template.format(gtest_filter=args.gtest_filter,
      test_exec_name=test_exec_name)
    f.write(script_header)
    f.write(script_body)
  # Make the script executable by user and group.
  st = os.stat(args.test_script_output)
  os.chmod(args.test_script_output, st.st_mode | stat.S_IXUSR | stat.S_IXGRP)


if __name__ == "__main__":
  main()
