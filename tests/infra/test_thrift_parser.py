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

from __future__ import absolute_import
import os
from subprocess import check_output

from tests.common.base_test_suite import BaseTestSuite


class TestThriftParser(BaseTestSuite):

  def test_thrift_parser(self):
    """Make sure thrift_parser can parse all the thrift files"""
    impala_home = os.getenv("IMPALA_HOME")
    # Only supports python3
    output = check_output([os.path.join(impala_home, "bin/impala-python3"),
                           os.path.join(impala_home, "bin/jenkins/thrift_parser.py"),
                           os.path.join(impala_home, "common/thrift")],
                          universal_newlines=True)
    assert "TStatus" in output
    assert "Field1 required ErrorCodes.TErrorCode status_code" in output
    assert "Field2 optional list<string> error_msgs" in output
    assert "TErrorCode" in output
    assert "EnumItem OK" in output
    assert "EnumItem UNUSED" in output
    assert "EnumItem GENERAL" in output
    assert "EnumItem CANCELLED" in output
