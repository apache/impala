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
from tests.metadata.test_ddl_base import TestDdlBase


class TestResetMetadata(TestDdlBase):
  def test_reset_metadata_case_sensitivity(self, unique_database):
    # IMPALA-6719: Issue with database name case sensitivity in reset metadata.
    table = 'newtable'
    self.client.execute('create table %s.%s (i int)' % (unique_database, table))

    self.client.execute('refresh %s.%s' % (unique_database, table))
    self.client.execute('refresh %s.%s' % (unique_database.upper(), table.upper()))

    self.client.execute('invalidate metadata %s.%s' % (unique_database, table))
    self.client.execute('invalidate metadata %s.%s' % (unique_database.upper(),
                                                       table.upper()))

    self.client.execute('refresh functions %s' % unique_database)
    self.client.execute('refresh functions %s' % unique_database.upper())
