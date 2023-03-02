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
# Unit tests for collect_diagnostics.Command

from __future__ import absolute_import, division, print_function
import os
import pytest
import sys

# Update the sys.path to include the modules from bin/diagnostics.
sys.path.insert(0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../bin/diagnostics')))
from collect_diagnostics import Command

class TestCommand(object):
  """ Unit tests for the Command class"""

  def test_simple_commands(self):
    # Successful command
    c = Command(["echo", "foo"], 1000)
    assert c.run() == 0, "Command expected to succeed, but failed"
    assert c.stdout.strip("\n") == "foo"

    # Failed command, check return code
    c = Command(["false"], 1000)
    assert c.run() == 1

  def test_command_timer(self):
    # Try to run a command that sleeps for 1000s and set a
    # timer for 1 second. The command should timed out.
    c = Command(["sleep", "1000"], 1)
    assert c.run() != 0, "Command expected to timeout but succeeded."
    assert c.child_killed_by_timeout, "Command didn't timeout as expected."


