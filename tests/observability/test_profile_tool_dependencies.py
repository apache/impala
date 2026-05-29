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
import os.path
import sys
from subprocess import CalledProcessError, STDOUT, check_output

import pytest
from tests.common.base_test_suite import BaseTestSuite
from tests.common.environ import impalad_basedir


class TestProfileToolDependencies(BaseTestSuite):

  def test_runtime_dependencies(self):
    if not sys.platform.startswith('linux'):
      pytest.skip('runtime dependency check is Linux-only')

    profile_tool = os.path.join(impalad_basedir, 'util/impala-profile-tool')
    dynamic_section = check_output(
        ['readelf', '-d', profile_tool], universal_newlines=True)
    transitive_dependencies = check_output(
        ['ldd', profile_tool], universal_newlines=True)
    unused_deps = [
      'libhdfs',
      'libjvm',
      'libkudu_client',
      'libsasl2',
      'libkrb5',
      'libgssapi',
      'libssl',
      'libcrypto',
    ]
    for dep in unused_deps:
      assert dep not in dynamic_section
      assert dep not in transitive_dependencies

    try:
      help_output = check_output([profile_tool, '--help'], stderr=STDOUT,
          universal_newlines=True)
    except CalledProcessError as e:
      help_output = e.output
      assert e.returncode != 127
    assert 'impala-profile-tool' in help_output
