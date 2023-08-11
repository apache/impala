#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
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
from tests.common.impala_test_suite import ImpalaTestSuite


class TestBeeline(ImpalaTestSuite):
  def test_beeline_compatibility(self, unique_database):
    stmt = "show tables"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == "name\n"
    stmt = "create table test_beeline_compatibility(id int)"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == \
           "summary\nTable has been created.\n"
    stmt = "show tables"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == \
           "name\ntest_beeline_compatibility\n"
    stmt = "select count(*) as cnt from test_beeline_compatibility"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == "cnt\n0\n"
    stmt = "insert into test_beeline_compatibility values(1), (2), (3)"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == ""
    stmt = "select count(*) as cnt from test_beeline_compatibility"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == "cnt\n3\n"
    stmt = "drop table test_beeline_compatibility"
    assert self.run_impala_stmt_in_beeline(stmt, None, unique_database) == \
           "summary\nTable has been dropped.\n"
