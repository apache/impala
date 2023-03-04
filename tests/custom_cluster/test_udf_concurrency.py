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
from builtins import range
import os
import pytest
import random
import threading
import time
from subprocess import check_call

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.util.filesystem_utils import get_fs_path

# This custom cluster test splits out concurrency tests to allow running with
# a higher fe_service_threads (and thus higher concurrency). This also avoids
# side-effects for other tests (see IMPALA-7639).


class TestUdfConcurrency(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestUdfConcurrency, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestUdfConcurrency, cls).add_test_dimensions()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--fe_service_threads=1000")
  def test_native_functions_race(self, vector, unique_database):
    """ IMPALA-6488: stress concurrent adds, uses, and deletes of native functions.
        Exposes a crash caused by use-after-free in lib-cache."""

    # Native function used by a query. Stresses lib-cache during analysis and
    # backend expressions.
    create_fn_to_use = \
      """create function {0}.use_it(string) returns string
         LOCATION '{1}'
         SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE'"""
    use_fn = """select * from (select max(int_col) from functional.alltypesagg
                where {0}.use_it(string_col) = 'blah' union all
                (select max(int_col) from functional.alltypesagg
                 where {0}.use_it(String_col) > '1' union all
                (select max(int_col) from functional.alltypesagg
                 where {0}.use_it(string_col) > '1'))) v"""
    # Reference to another native function from the same 'so' file. Creating/dropping
    # stresses lib-cache lookup, add, and refresh.
    create_another_fn = """create function if not exists {0}.other(float)
                           returns float location '{1}' symbol='Identity'"""
    drop_another_fn = """drop function if exists {0}.other(float)"""
    udf_path = get_fs_path('/test-warehouse/libTestUdfs.so')

    # Tracks number of impalads prior to tests to check that none have crashed.
    # All impalads are assumed to be coordinators.
    cluster = ImpalaCluster.get_e2e_test_cluster()
    exp_num_coordinators = cluster.num_responsive_coordinators()

    setup_client = self.create_impala_client()
    setup_query = create_fn_to_use.format(unique_database, udf_path)
    try:
      setup_client.execute(setup_query)
    except Exception as e:
      print("Unable to create initial function: {0}".format(setup_query))
      raise

    errors = []

    def use_fn_method():
      time.sleep(1 + random.random())
      client = self.create_impala_client()
      query = use_fn.format(unique_database)
      try:
        client.execute(query)
      except Exception as e:
        errors.append(e)

    def load_fn_method():
      time.sleep(1 + random.random())
      client = self.create_impala_client()
      drop = drop_another_fn.format(unique_database)
      create = create_another_fn.format(unique_database, udf_path)
      try:
        client.execute(drop)
        client.execute(create)
      except Exception as e:
        errors.append(e)

    # number of uses/loads needed to reliably reproduce the bug.
    num_uses = 200
    num_loads = 200

    # create threads to use native function.
    runner_threads = []
    for i in range(num_uses):
      runner_threads.append(threading.Thread(target=use_fn_method))

    # create threads to drop/create native functions.
    for i in range(num_loads):
      runner_threads.append(threading.Thread(target=load_fn_method))

    # launch all runner threads.
    for t in runner_threads: t.start()

    # join all threads.
    for t in runner_threads: t.join()

    for err in errors: print(err)

    # Checks that no impalad has crashed.
    assert cluster.num_responsive_coordinators() == exp_num_coordinators

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--fe_service_threads=1000")
  def test_concurrent_jar_drop_use(self, vector, unique_database):
    """IMPALA-6215: race between dropping/using java udf's defined in the same jar.
       This test runs concurrent drop/use threads that result in class not found
       exceptions when the race is present.
    """
    udf_src_path = os.path.join(
      os.environ['IMPALA_HOME'], "testdata/udfs/impala-hive-udfs.jar")
    udf_tgt_path = get_fs_path(
      '/test-warehouse/{0}.db/impala-hive-udfs.jar'.format(unique_database))

    create_fn_to_drop = """create function {0}.foo_{1}() returns string
                           LOCATION '{2}' SYMBOL='org.apache.impala.TestUpdateUdf'"""
    create_fn_to_use = """create function {0}.use_it(string) returns string
                          LOCATION '{1}' SYMBOL='org.apache.impala.TestUdf'"""
    drop_fn = "drop function if exists {0}.foo_{1}()"
    use_fn = """select * from (select max(int_col) from functional.alltypesagg
                where {0}.use_it(string_col) = 'blah' union all
                (select max(int_col) from functional.alltypesagg
                 where {0}.use_it(String_col) > '1' union all
                (select max(int_col) from functional.alltypesagg
                 where {0}.use_it(string_col) > '1'))) v"""
    num_drops = 100
    num_uses = 100

    # use a unique jar for this test to avoid interactions with other tests
    # that use the same jar
    self.filesystem_client.copy_from_local(udf_src_path, udf_tgt_path)

    # create all the functions.
    setup_client = self.create_impala_client()
    try:
      s = create_fn_to_use.format(unique_database, udf_tgt_path)
      setup_client.execute(s)
    except Exception as e:
      print(e)
      assert False
    for i in range(0, num_drops):
      try:
        setup_client.execute(create_fn_to_drop.format(unique_database, i, udf_tgt_path))
      except Exception as e:
        print(e)
        assert False

    errors = []

    def use_fn_method():
      time.sleep(5 + random.random())
      client = self.create_impala_client()
      try:
        client.execute(use_fn.format(unique_database))
      except Exception as e: errors.append(e)

    def drop_fn_method(i):
      time.sleep(1 + random.random())
      client = self.create_impala_client()
      try:
        client.execute(drop_fn.format(unique_database, i))
      except Exception as e: errors.append(e)

    # create threads to use functions.
    runner_threads = []
    for i in range(0, num_uses):
      runner_threads.append(threading.Thread(target=use_fn_method))

    # create threads to drop functions.
    for i in range(0, num_drops):
      runner_threads.append(threading.Thread(target=drop_fn_method, args=(i, )))

    # launch all runner threads.
    for t in runner_threads: t.start()

    # join all threads.
    for t in runner_threads: t.join()

    # Check for any errors.
    for err in errors: print(err)
    assert len(errors) == 0
