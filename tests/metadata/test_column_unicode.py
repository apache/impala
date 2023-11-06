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

from __future__ import absolute_import, division, print_function, unicode_literals
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_client_protocol_dimension


class TestColumnUnicode(ImpalaTestSuite):

    @classmethod
    def get_workload(cls):
        return 'functional-query'

    @classmethod
    def add_test_dimensions(cls):
        super(TestColumnUnicode, cls).add_test_dimensions()
        cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
        # Since the test file already covers different file formats,
        # a constraint is added to avoid redundancy.
        cls.ImpalaTestMatrix.add_constraint(
            lambda v: v.get_value('table_format').file_format == 'parquet'
            and v.get_value('table_format').compression_codec == 'none')

    def test_create_unicode_table(self, vector, unique_database):
        self.run_test_case('QueryTest/unicode-column-name', vector,
                           use_db=unique_database)
