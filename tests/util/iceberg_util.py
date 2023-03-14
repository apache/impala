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
import datetime

from tests.util.filesystem_utils import get_fs_path


class Snapshot(object):
  """Encapsulate an Iceberg Snapshot"""

  def __init__(self, raw_string):
    """Construct a snapshot from a row of output from DESCRIBE HISTORY.
    The columns, separated by tabs are:
    creation_time, snapshot_id, parent_id, is_current_ancestor
    for example (using a space not a tab):
    2020-08-30 22:58:08.440000000 8270633197658268308 NULL TRUE
    """
    parts = raw_string.split("\t")
    self._creation_time = parse_timestamp(parts[0])
    self._snapshot_id = int(parts[1])
    parent_id_str = parts[2]
    if parent_id_str == "NULL":
      self._parent_id = None
    else:
      self._parent_id = int(parent_id_str)
    is_ancestor_str = parts[3]
    if is_ancestor_str == "TRUE":
      self._is_current_ancestor = True
    elif is_ancestor_str == "FALSE":
      self._is_current_ancestor = False
    else:
      raise ValueError("unexpected is_current_ancestor value {0}".format(is_ancestor_str))

  def __str__(self):
    return "<Snapshot>: id={0} creation={1} parent={2} is_current_ancestor={3}".format(
      self._snapshot_id, self._creation_time, self._parent_id,
      self._is_current_ancestor)

  def get_creation_time(self):
    return self._creation_time

  def get_snapshot_id(self):
    return self._snapshot_id

  def get_parent_id(self):
    return self._parent_id

  def is_current_ancestor(self):
    return self._is_current_ancestor


def parse_timestamp(ts_string):
  """The client can receive the timestamp in two formats, if the timestamp has
  fractional seconds "yyyy-MM-dd HH:mm:ss.SSSSSSSSS" pattern is used, otherwise
  "yyyy-MM-dd HH:mm:ss". Additionally, Python's datetime library cannot handle
  nanoseconds, therefore in that case the timestamp has to be trimmed."""
  if len(ts_string.split('.')) > 1:
    return datetime.datetime.strptime(ts_string[:-3], '%Y-%m-%d %H:%M:%S.%f')
  else:
    return datetime.datetime.strptime(ts_string, '%Y-%m-%d %H:%M:%S')


def quote(s):
  return "'{0}'".format(s)


def cast_ts(ts):
  return "CAST({0} as timestamp)".format(quote(ts))


def get_snapshots(impalad_client, tbl_name, ts_start=None, ts_end=None,
                  expected_result_size=None, user=None):
  """Executes DESCRIBE HISTORY <tbl> through the given client.
  If ts_start is set, and ts_end is not, then start_ts is used as the FROM parameter of
  DESCRIBE HISTORY.
  If both ts_start and ts_end are set, then they are used as parameters to
  DESCRIBE HISTORY BETWEEN.
  If expected_result_size is set then, this is used to check the number of results.
  The output is converted to a list of Snapshot objects, which are returned."""
  if ts_start and ts_end:
    query = "DESCRIBE HISTORY {0} BETWEEN {1} AND {2};".format(
      tbl_name, cast_ts(ts_start), cast_ts(ts_end))
  elif ts_start:
    query = "DESCRIBE HISTORY {0} FROM {1};".format(tbl_name, cast_ts(ts_start))
  else:
    query = "DESCRIBE HISTORY {0};".format(tbl_name)
  rows = impalad_client.execute(query, user)
  if expected_result_size:
    assert len(rows.data) == expected_result_size, \
      "got unexpected number of snapshots {0} when expected {1}".format(
        len(rows.data), expected_result_size)
  results = []
  for snapshot_str in rows.data:
    results.append(Snapshot(snapshot_str))
  return results


class IcebergCatalogs:
  """Utility class to generate TBLPROPERTIES corresponding to various iceberg catalogs."""

  def __init__(self, database):
    """Create a IcebergCatalogs object parameterized by database name."""
    self.database = database

  hive_catalog = "'iceberg.catalog'='hive.catalog'"
  hive_catalogs = "'iceberg.catalog'='ice_hive_cat'"
  hadoop_tables = "'iceberg.catalog'='hadoop.tables'"
  hadoop_catalogs = "'iceberg.catalog'='ice_hadoop_cat'"

  def get_iceberg_catalog_properties(self):
    """Return a list containing TBLPROPERTIES corresponding to various iceberg catalogs.
     The TBLPROPERTIES can be used to create tables."""
    ice_cat_location = "/test-warehouse/{0}/hadoop_catalog_test/".format(self.database)
    ice_cat_location_fs = get_fs_path(ice_cat_location)
    hadoop_catalog = ("'iceberg.catalog'='hadoop.catalog', "
                      + "'iceberg.catalog_location'='{0}'".format(ice_cat_location_fs))
    return [
      self.hadoop_tables,
      self.hive_catalog,
      self.hive_catalogs,
      self.hadoop_catalogs,
      hadoop_catalog
    ]

  def is_a_hive_catalog(self, catalog):
    """Return true if the catalog property is for a Hive catalog."""
    return catalog == self.hive_catalog or catalog == self.hive_catalogs
