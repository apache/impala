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
from tests.comparison.db_types import Boolean
from tests.comparison.funcs import And, Equals, Or
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import DefaultProfile


def test_func_tree_contains_funcs():
  """
  Tests the QueryGenerator.func_tree_contains_funcs() method
  """

  qgen = QueryGenerator(DefaultProfile())

  # Create a simple func_tree with only one function
  and_func = And.create_from_args(Boolean(True), Boolean(True))
  and_func.parent = None
  assert qgen._func_tree_contains_funcs(and_func, [And])
  assert not qgen._func_tree_contains_funcs(and_func, [Or])

  # Create a func_tree that contains one parent, and two children
  equals_func = Equals.create_from_args(Boolean(True), Boolean(True))
  and_func = And.create_from_args(equals_func, equals_func)
  equals_func.parent = and_func
  and_func.parent = None
  assert qgen._func_tree_contains_funcs(equals_func, [And])
  assert qgen._func_tree_contains_funcs(equals_func, [Equals])
  assert not qgen._func_tree_contains_funcs(equals_func, [Or])
