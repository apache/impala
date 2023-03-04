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
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS
from tests.util.hive_utils import HiveDbWrapper


@SkipIfFS.hive
class TestMetadataReplicas(CustomClusterTestSuite):
  """ Validates metadata content across catalogd and impalad coordinators."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestMetadataReplicas, cls).setup_class()

  @pytest.mark.execute_serially
  def test_start(self):
    """ Baseline to verify that the initial state is identical. No DDL/DML
        is processed, so no objects are fully loaded."""
    self.__validate_metadata()

  @pytest.mark.execute_serially
  def test_catalog_restart(self, testid_checksum):
    """ IMPALA-6948: reproduces the issue by deleting a table from Hive while the catalogd
        is down. When catalogd is restarted, if the regression is present, the deleted
        table will still be present at the impalads."""
    db_name = "test_catalog_restart_%s" % testid_checksum
    try:
      with HiveDbWrapper(self, db_name):
        # Issue several invalidates to boost the version for the current incarnation of the
        # catalog. As a result, the table we'll add to Hive will get a version that's easier
        # to see is higher than the highest version of the restarted catalogd incarnation.
        for i in range(0, 50):
          self.client.execute("invalidate metadata functional.alltypes")
        assert self.cluster.catalogd.service.get_catalog_version() >= 50
        # Creates a database and table with Hive and makes it visible to Impala.
        self.run_stmt_in_hive("create table %s.x (a string)" % db_name)
        self.client.execute("invalidate metadata %s.x" % db_name)
        assert "x" in self.client.execute("show tables in %s" % db_name).data
        # Stops the catalog
        self.cluster.catalogd.kill()
        # Drops the table from the catalog using Hive.
        self.run_stmt_in_hive("drop table %s.x" % db_name)
        # Restarts the catalog
        self.cluster.catalogd.start()
        # Refreshes the state of the catalogd process.
        self.cluster.refresh()
        # Wait until the impalad catalog versions agree with the catalogd's version.
        catalogd_version = self.cluster.catalogd.service.get_catalog_version()
        for impalad in self.cluster.impalads:
          impalad.service.wait_for_metric_value("catalog.curr-version", catalogd_version)

        self.__validate_metadata()
    except Exception as e:
      assert False, "Unexpected exception: " + str(e)
    finally:
      # Hack to work-around IMPALA-5695.
      self.cluster.catalogd.kill()

  def __validate_metadata(self):
    """ Computes the pair-wise object version difference between the catalog contents
        in catalogd and each impalad. Asserts that there are no differences."""
    c_objects = self.cluster.catalogd.service.get_catalog_objects()
    i_objects = [proc.service.get_catalog_objects() for proc in self.cluster.impalads]

    for idx in range(0, len(i_objects)):
      i_obj = i_objects[idx]
      diff = self.__diff_catalog_objects(c_objects, i_obj)
      assert diff[0] == {},\
          'catalogd has objects not in impalad(%d): %s ' % (idx, diff[0])
      assert diff[1] == {}, 'impalad(%d) has objects not in catalogd: %s' % (idx, diff[1])
      assert diff[2] is None,\
          'impalad(%d) and catalogd version for objects differs: %s' % (idx, diff[2])

  def __diff_catalog_objects(self, a, b):
    """ Computes the diff between the input 'a' and 'b' dictionaries. The result is a
        list of length 3 where position 0 holds those entries that are in a, but not b,
        position 1 those entries that are in b, but not a, and position 2 holds entries
        where the key is in both a and b, but whose value differs."""
    # diff[0] : a - b
    # diff[1] : b - a
    # diff[2] : a[k] != b[k]
    diff = [None, None, None]
    diff[0] = dict((k, a[k]) for k in set(a) - set(b))
    diff[1] = dict((k, b[k]) for k in set(b) - set(a))
    for k, v_a in a.items():
      v_b = b[k]
      if v_b is not None:
        if v_b != v_a:
          diff[2][k] = (v_a, v_b)
    return diff
