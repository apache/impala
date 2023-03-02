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

# Validates table stored on the LocalFileSystem.
#
from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfDockerizedCluster
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestLocalFileSystem(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLocalFileSystem, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and \
        v.get_value('table_format').compression_codec == 'none')

  @SkipIfDockerizedCluster.accesses_host_filesystem
  def test_local_filesystem(self, vector, unique_database):
    self.run_test_case('QueryTest/local-filesystem', vector, unique_database)
