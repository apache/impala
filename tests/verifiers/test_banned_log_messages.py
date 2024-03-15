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

from __future__ import absolute_import, division, print_function
import os
import subprocess


class TestBannedLogMessages:
  """Verify that specific log messages are banned from Impala logs.

  This test suite should be run after all the tests have been run.
  """

  def assert_message_absent(self, message, log_dir=os.environ["IMPALA_LOGS_DIR"]):
    for root, _, files in os.walk(log_dir):
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
