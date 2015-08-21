#!/usr/bin/env impala-python
# encoding=utf-8
# Copyright 2016 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

def assert_var_substitution(result):
  assert_pattern(r'\bfoo_number=.*$', 'foo_number= 123123', result.stdout, \
    'Numeric values not replaced correctly')
  assert_pattern(r'\bfoo_string=.*$', 'foo_string=123', result.stdout, \
    'String values not replaced correctly')
  assert_pattern(r'\bVariables:[\s\n]*BAR:\s*[0-9]*\n\s*FOO:\s*[0-9]*', \
    'Variables:\n\tBAR: 456\n\tFOO: 123', result.stdout, \
    "Set variable not listed correctly by the first SET command")
  assert_pattern(r'\bError: Unknown variable FOO1$', \
    'Error: Unknown variable FOO1', result.stderr, \
    'Missing variable FOO1 not reported correctly')
  assert_pattern(r'\bmulti_test=.*$', 'multi_test=456_123_456_123', \
    result.stdout, 'Multiple replaces not working correctly')
  assert_pattern(r'\bError:\s*Unknown\s*substitution\s*syntax\s*' +
                 r'\(RANDOM_NAME\). Use \${VAR:var_name}', \
    'Error: Unknown substitution syntax (RANDOM_NAME). Use ${VAR:var_name}', \
    result.stderr, "Invalid variable reference")
  assert_pattern(r'"This should be not replaced: \${VAR:foo} \${HIVEVAR:bar}"',
    '"This should be not replaced: ${VAR:foo} ${HIVEVAR:bar}"', \
    result.stdout, "Variable escaping not working")
  assert_pattern(r'\bVariable MYVAR set to.*$', 'Variable MYVAR set to foo123',
    result.stderr, 'No evidence of MYVAR variable being set.')
  assert_pattern(r'\bVariables:[\s\n]*BAR:.*[\s\n]*FOO:.*[\s\n]*MYVAR:.*$',
    'Variables:\n\tBAR: 456\n\tFOO: 123\n\tMYVAR: foo123', result.stdout,
    'Set variables not listed correctly by the second SET command')
  assert_pattern(r'\bUnsetting variable FOO$', 'Unsetting variable FOO',
    result.stdout, 'No evidence of variable FOO being unset')
  assert_pattern(r'\bUnsetting variable BAR$', 'Unsetting variable BAR',
    result.stdout, 'No evidence of variable BAR being unset')
  assert_pattern(r'\bVariables:[\s\n]*No variables defined\.$', \
    'Variables:\n\tNo variables defined.', result.stdout, \
    'Unset variables incorrectly listed by third SET command.')
  assert_pattern(r'\bNo variable called NONEXISTENT is set', \
    'No variable called NONEXISTENT is set', result.stdout, \
    'Problem unsetting non-existent variable.')
  assert_pattern(r'\bVariable COMMENT_TYPE1 set to.*$',
    'Variable COMMENT_TYPE1 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE1 variable being set.')
  assert_pattern(r'\bVariable COMMENT_TYPE2 set to.*$',
    'Variable COMMENT_TYPE2 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE2 variable being set.')
  assert_pattern(r'\bVariable COMMENT_TYPE3 set to.*$',
    'Variable COMMENT_TYPE3 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE3 variable being set.')
  assert_pattern(r'\bVariables:[\s\n]*COMMENT_TYPE1:.*[\s\n]*' + \
    'COMMENT_TYPE2:.*[\s\n]*COMMENT_TYPE3:.*$',
    'Variables:\n\tCOMMENT_TYPE1: ok\n\tCOMMENT_TYPE2: ok\n\tCOMMENT_TYPE3: ok', \
    result.stdout, 'Set variables not listed correctly by the SET command')

def assert_pattern(pattern, result, text, message):
  """Asserts that the pattern, when applied to text, returns the expected result"""
  m = re.search(pattern, text, re.MULTILINE)
  assert m and m.group(0) == result, message

