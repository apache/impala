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
import json
import requests
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension


class TestReusePartitions(ImpalaTestSuite):
  """Tests for catalogd reusing unchanged partition instances for DDL/DMLs"""

  JSON_TABLE_OBJECT_URL = "http://localhost:25020/catalog_object?" \
                          "json&object_type=TABLE&object_name={0}.{1}"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestReusePartitions, cls).add_test_dimensions()

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def __get_partition_id_set(self, db_name, tbl_name):
    obj_url = self.JSON_TABLE_OBJECT_URL.format(db_name, tbl_name)
    response = requests.get(obj_url)
    assert response.status_code == requests.codes.ok
    json_response = json.loads(response.text)
    assert "json_string" in json_response, json_response
    catalog_obj = json.loads(json_response["json_string"])
    assert "table" in catalog_obj, catalog_obj
    assert "hdfs_table" in catalog_obj["table"], catalog_obj["table"]
    tbl_obj = catalog_obj["table"]["hdfs_table"]
    assert "partitions" in tbl_obj, tbl_obj
    return set(tbl_obj["partitions"].keys())

  def test_reuse_partitions_nontransactional(self, unique_database):
    self.__test_reuse_partitions_helper(unique_database, transactional=False)

  def test_reuse_partitions_transactional(self, unique_database):
    self.__test_reuse_partitions_helper(unique_database, transactional=True)

  def __test_reuse_partitions_helper(self, unique_database, transactional=False):
    """Test catalogd reuses partition instances by verifying the partition ids
    are unchanged"""
    tbl_name = "tbl"
    create_tbl_ddl =\
        "create table %s.%s (id int) partitioned by (p int) stored as textfile"\
        % (unique_database, tbl_name)
    if transactional:
      create_tbl_ddl += " tblproperties('transactional'='true'," \
                        " 'transactional_properties'='insert_only')"
    # Creates a partitioned table with 3 partitions.
    self.client.execute(create_tbl_ddl)
    self.client.execute("insert into %s.%s partition (p) values (1, 1), (2, 2), (3, 3)"
                        % (unique_database, tbl_name))
    part_ids = self.__get_partition_id_set(unique_database, tbl_name)
    assert len(part_ids) == 3

    # REFRESH can reuse the existing partition instances.
    self.client.execute("refresh %s.%s" % (unique_database, tbl_name))
    assert self.__get_partition_id_set(unique_database, tbl_name) == part_ids
    # INSERT query that only touches one partition will reuse the other partitions.
    self.client.execute("insert into %s.%s partition (p) values (1, 1)"
                        % (unique_database, tbl_name))
    new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
    assert len(part_ids.intersection(new_part_ids)) == 2
    part_ids = new_part_ids
    # INSERT query that adds a new partition will reuse the existing partitions.
    self.client.execute("insert into %s.%s partition(p) values (4, 4)"
                        % (unique_database, tbl_name))
    new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
    assert len(part_ids.intersection(new_part_ids)) == 3
    part_ids = new_part_ids

    # ALTER TABLE not supported on transactional tables (IMPALA-8831).
    if not transactional:
      # ALTER statements that don't touch data will reuse the existing partitions.
      self.client.execute("alter table %s.%s set tblproperties('numRows'='4')"
                          % (unique_database, tbl_name))
      assert self.__get_partition_id_set(unique_database, tbl_name) == part_ids
      self.client.execute("alter table %s.%s add column name string"
                          % (unique_database, tbl_name))
      assert self.__get_partition_id_set(unique_database, tbl_name) == part_ids
      self.client.execute("alter table %s.%s drop column name"
                          % (unique_database, tbl_name))
      assert self.__get_partition_id_set(unique_database, tbl_name) == part_ids
      # ALTER statements that modify a partition will reuse other partitions.
      self.client.execute("alter table %s.%s add partition (p=5)"
                          % (unique_database, tbl_name))
      new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
      assert len(new_part_ids) == 5
      assert len(part_ids.intersection(new_part_ids)) == 4
      self.client.execute("alter table %s.%s drop partition (p=5)"
                          % (unique_database, tbl_name))
      new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
      assert part_ids == new_part_ids

    # Updating stats will also update partition stats so no instances can be reused.
    self.client.execute("compute stats %s.%s" % (unique_database, tbl_name))
    new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
    assert len(new_part_ids) == 4
    assert len(part_ids.intersection(new_part_ids)) == 0
    self.client.execute("compute incremental stats %s.%s" % (unique_database, tbl_name))
    new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
    assert len(new_part_ids) == 4
    assert len(part_ids.intersection(new_part_ids)) == 0
    part_ids = new_part_ids
    # DROP STATS not supported on transactional tables (HIVE-22104).
    if not transactional:
      # Drop incremental stats of one partition can reuse the other 3 partitions.
      self.client.execute("drop incremental stats %s.%s partition (p=1)"
                          % (unique_database, tbl_name))
      new_part_ids = self.__get_partition_id_set(unique_database, tbl_name)
      assert len(part_ids.intersection(new_part_ids)) == 3
