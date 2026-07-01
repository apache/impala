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
# Test that impalad logs omit specific messages we shouldn't see.

import os
import subprocess

from tests.common.base_test_suite import BaseTestSuite


class TestBannedLogMessages(BaseTestSuite):
  """Verify that specific log messages are banned from Impala logs.

  This test suite should be run after all the tests have been run.
  """

  def assert_message_absent(self, message, log_dir=os.environ["IMPALA_LOGS_DIR"],
                             skip_subdirs=None):
    for root, dirs, files in os.walk(log_dir):
      if skip_subdirs:
        dirs[:] = [d for d in dirs if d not in skip_subdirs]
      for file in files:
        log_file_path = os.path.join(root, file)
        returncode = subprocess.call(['grep', message, log_file_path])
        assert returncode == 1, "%s contains '%s'" % (log_file_path, message)

  def test_no_inaccessible_objects(self):
    """Test that cluster logs do not contain InaccessibleObjectException"""
    self.assert_message_absent('InaccessibleObjectException')

  def test_no_unsupported_operations(self):
    """Test that cluster logs do not contain jamm.CannotAccessFieldException"""
    self.assert_message_absent('CannotAccessFieldException')

  def test_no_tuniqueid(self):
    """Test that cluster logs do not contain TUniqueId. They should instead print
    IDs with the format 8a4673c8fbe83a74:309751e900000000."""
    # Skip 'coverage' dirs (gcovr C++ HTML and Jacoco Java HTML reports render
    # source code containing TUniqueId) and 'results' dirs (pytest JUnit XML files
    # may capture TUniqueId.__repr__() from test failure tracebacks).
    self.assert_message_absent('[^a-zA-Z]TUniqueId(',
        skip_subdirs={'coverage', 'results'})
