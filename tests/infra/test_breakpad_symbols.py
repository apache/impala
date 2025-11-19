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
import tempfile

from tests.common.base_test_suite import BaseTestSuite


class TestDumpBreakpadSymbols(BaseTestSuite):

  def __positive_test(self, binary_location_args, stdin_input=None):
    impala_home = os.getenv("IMPALA_HOME")
    with tempfile.TemporaryDirectory() as temp_dir:
      subprocess.check_call(
        ["{0}/bin/dump_breakpad_symbols.py".format(impala_home),
         "-d", temp_dir] + binary_location_args, stdin=stdin_input)

  def __negative_test(self, binary_location_args, expected_error_messages):
    impala_home = os.getenv("IMPALA_HOME")
    with tempfile.TemporaryDirectory() as temp_dir:
      p = subprocess.Popen(
        ["{0}/bin/dump_breakpad_symbols.py".format(impala_home),
         "-d", temp_dir] + binary_location_args, stderr=subprocess.PIPE,
        universal_newlines=True)
      _, stderr_output = p.communicate()
      # Verify that it failed with the expected error messages
      assert p.returncode != 0
      for expected_error in expected_error_messages:
        assert expected_error in stderr_output

  def test_binary_locations_positive(self):
    """Test different ways to specify binaries for dump_breakpad_symbols.py to catch
       obvious issues"""
    impala_build_dir = os.path.join(os.getenv("IMPALA_HOME"), "be/build/latest")

    # Test specifying an exact executable file (-f)
    self.__positive_test(["-f", os.path.join(impala_build_dir, "service/impalad")])

    # Test specifying a build directory with executables (-b)
    self.__positive_test(["-b", os.path.join(impala_build_dir, "service")])

    # Test specifying a list of executables via stdin (-i)
    with tempfile.TemporaryDirectory() as temp_dir:
      with open(os.path.join(temp_dir, "input_file"), "w") as input_list:
        input_list.write(os.path.join(impala_build_dir, "service/impalad"))
        input_list.write("\n")
        input_list.write(os.path.join(impala_build_dir, "util/impala-profile-tool"))

      with open(os.path.join(temp_dir, "input_file")) as input_list:
        self.__positive_test(["-i"], stdin_input=input_list)

  def test_binary_locations_negative(self):
    """Test a couple error cases for dump_breakpad_symbols.py to verify
       reasonable error messages"""
    impala_build_dir = os.path.join(os.getenv("IMPALA_HOME"), "be/build/latest")
    nonexistant_path = os.path.join(impala_build_dir, "doesnt_exist")

    # Non-existant executable with -f option
    self.__negative_test(["-f", nonexistant_path],
      ["File does not exist", nonexistant_path])

    # Non-existant build directory with -b option
    self.__negative_test(["-b", nonexistant_path],
      ["Build directory (-b) does not exist", nonexistant_path])

    # Build directory that doesn't contain any ELF files (we'll use a temporary
    # directory for this)
    with tempfile.TemporaryDirectory() as temp_dir:
      self.__negative_test(["-b", temp_dir], ["No ELF files found in", temp_dir])
