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

# Functional tests for ACID integration with Hive.
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfHive2
from tests.common.test_dimensions import create_single_exec_option_dimension


class TestAcid(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAcid, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # TODO(todd) consider running on other formats
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['text'])

  @SkipIfHive2.acid
  def test_acid(self, vector, unique_database):
    self.run_test_case('QueryTest/acid', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_acid_compaction(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-compaction', vector, use_db=unique_database)

  @SkipIfHive2.acid
  def test_acid_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-negative', vector, use_db=unique_database)

# TODO(todd): further tests to write:
#  TRUNCATE, once HIVE-20137 is implemented.
#  INSERT OVERWRITE with empty result set, once HIVE-21750 is fixed.
#  Negative test for LOAD DATA INPATH and all other SQL that we don't support.
