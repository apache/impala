#!/usr/bin/env ambari-python-wrap
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

from __future__ import absolute_import, division, print_function
import os
import subprocess
import sys
from optparse import OptionParser

IMPALA_HOME = os.environ['IMPALA_HOME']

def get_set_of_tests(unified_binary, filters):
    # Run the unified_binary with the specified filters. If filters is None, run
    # without filters. Process the output to get fully qualified tests.
    command = [os.path.join(IMPALA_HOME, "bin/run-binary.sh"), unified_binary,
               "--gtest_list_tests"]
    if filters is not None:
        command.append("--gtest_filter={0}".format(filters))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, universal_newlines=True)
    out, err = p.communicate()
    if p.returncode != 0:
        print("FAILED: Unified backend test executable returned an error when trying\n"
              "        to list tests.")
        print("Command: {0}".format(" ".join(command)))
        print("Return Code: {0}".format(p.returncode))
        print("stdout:\n{0}\nstderr:\n{1}".format(out, err))
        sys.exit(1)

    test_list = set()
    cur_test_suite = None
    for line in out.split("\n"):
        if line.find("seed = ") != -1: continue
        if len(line) == 0: continue
        if line[-1] == ".":
            cur_test_suite = line
        else:
            testcase = line.strip()
            test_list.add("{0}{1}".format(cur_test_suite, testcase))
    return test_list


def main():
    parser = OptionParser()
    parser.add_option("-f", "--filters", dest="filters",
                      help="Aggregation of all gtest filters")
    parser.add_option("-b", "--unified_binary", dest="unified_binary",
                      help="Filename for the unified test binary")
    options, args = parser.parse_args()
    without_filter = get_set_of_tests(options.unified_binary, None)
    with_filter = get_set_of_tests(options.unified_binary, options.filters)

    assert with_filter.issubset(without_filter)
    if without_filter != with_filter:
        print("FAILED: The unified backend test executable contains tests that are\n"
              "        missing from the CMake test filters specified via the\n"
              "        ADD_UNIFIED_BE_TEST/ADD_UNIFIED_BE_LSAN_TEST macros in\n"
              "        CMakeLists.txt. To fix this, add a pattern in the appropriate\n"
              "        CMakeLists.txt to match the following tests:")
        for tests in without_filter - with_filter:
            print(tests)
        print("Unified test executable: {0}\nFilters: {1}".format(
            options.unified_binary, options.filters))
        sys.exit(1)

    # Check to see if there are any filters that do not match tests in the unified
    # test executable. This can indicate that a test file is not included appropriately
    # in the executable. It can also indicate a bogus filter.
    filters_without_tests = []
    for test_filter in options.filters.split(":"):
        if len(test_filter) == 0: continue
        tests = get_set_of_tests(options.unified_binary, test_filter)
        if len(tests) == 0:
            filters_without_tests.append(test_filter)
    if len(filters_without_tests) > 0:
        print("FAILED: Some test filters specified by\n"
              "        ADD_UNIFIED_BE_TEST/ADD_UNIFIED_BE_LSAN_TEST macros in\n"
              "        CMakeLists.txt do not match any tests in the unified backend\n"
              "        test executable. This can happen if there is a bogus filter or\n"
              "        if some tests are not being included in the test executable\n"
              "        (e.g. if a new test file is missing from the test library).\n"
              "        The invalid test filters are:")
        for test_filter in filters_without_tests:
            print(test_filter)
        sys.exit(1)

if __name__ == "__main__": main()
