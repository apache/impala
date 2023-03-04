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

# Unit tests for the test file parser

from __future__ import absolute_import, division, print_function
from tests.common.base_test_suite import BaseTestSuite
from tests.util.test_file_parser import parse_test_file_text

test_text = """
# Text before in the header (before the first ====) should be ignored
# so put this here to test it out.
====
---- QUERY
# comment
SELECT blah from Foo
s
---- RESULTS
'Hi'
---- TYPES
string
---- LINEAGE
test_lineage_str > 'foo' AND 'bar'
multi_line
====
---- QUERY
SELECT 2
---- RESULTS
'Hello'
---- TYPES
string
#====
# SHOULD PARSE COMMENTED OUT TEST PROPERLY
#---- QUERY: TEST_WORKLOAD_Q2
#SELECT int_col from Bar
#---- RESULTS
#231
#---- TYPES
#int
====
---- QUERY: TEST_WORKLOAD_Q2
SELECT int_col from Bar
---- RESULTS
231
---- TYPES
int
====
"""

VALID_SECTIONS = ['QUERY', 'RESULTS', 'TYPES', 'LINEAGE']

class TestTestFileParser(BaseTestSuite):
  def test_valid_parse(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS)
    assert len(results) == 3
    print(results[0])
    expected_results = {'QUERY': '# comment\nSELECT blah from Foo\ns\n',
                        'TYPES': 'string\n', 'RESULTS': "'Hi'\n",
                        'LINEAGE': "test_lineage_str > 'foo' AND 'bar'\nmulti_line\n"}
    assert results[0] == expected_results

  def test_invalid_section(self):
    # Restrict valid sections to exclude one of the section names.
    valid_sections = ['QUERY', 'RESULTS']
    results = parse_test_file_text(test_text, valid_sections, skip_unknown_sections=True)
    assert len(results) == 3
    expected_results = {'QUERY': '# comment\nSELECT blah from Foo\ns\n',
                        'RESULTS': "'Hi'\n"}
    assert results[0] == expected_results

    # In this case, instead of ignoring the invalid section we should get an error
    try:
      results = parse_test_file_text(test_text, valid_sections,
                                     skip_unknown_sections=False)
      assert 0, 'Expected error due to invalid section'
    except RuntimeError as re:
      assert str(re) == "Unknown subsection: TYPES"

  def test_parse_query_name(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS, False)
    assert len(results) == 3
    expected_results = {'QUERY': 'SELECT int_col from Bar\n',
                        'TYPES': 'int\n', 'RESULTS': '231\n',
                        'QUERY_NAME': 'TEST_WORKLOAD_Q2'}
    assert results[2] == expected_results

  def test_parse_commented_out_test_as_comment(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS)
    assert len(results) == 3
    expected_results = {'QUERY': 'SELECT 2\n', 'RESULTS': "'Hello'\n",
                        'TYPES': "string\n#====\n"\
                        "# SHOULD PARSE COMMENTED OUT TEST PROPERLY\n"
                        "#---- QUERY: TEST_WORKLOAD_Q2\n"
                        "#SELECT int_col from Bar\n"
                        "#---- RESULTS\n#231\n#---- TYPES\n#int\n"}
    print(expected_results)
    print(results[1])
    assert results[1] == expected_results
