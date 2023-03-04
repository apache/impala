#!/usr/bin/env impala-python
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
#
# Utility functions used by the stress test to save and load runtime info
# about queries to and from JSON files.

from __future__ import absolute_import, division, print_function
from collections import defaultdict
import json
import logging
import os
import sys

from tests.stress.queries import Query

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

# The version of the file format containing the collected query runtime info.
RUNTIME_INFO_FILE_VERSION = 3


def save_runtime_info(path, query, impala):
  """Updates the file at 'path' with the given query information."""
  store = None
  if os.path.exists(path):
    with open(path) as file:
      store = json.load(file)
    _check_store_version(store)
  if not store:
    store = {
        "host_names": list(), "db_names": dict(), "version": RUNTIME_INFO_FILE_VERSION}
  with open(path, "w+") as file:
    store["host_names"] = sorted([i.host_name for i in impala.impalads])
    queries = store["db_names"].get(query.db_name, dict())
    query_by_options = queries.get(query.sql, dict())
    query_by_options[str(sorted(query.options.items()))] = query
    queries[query.sql] = query_by_options
    store["db_names"][query.db_name] = queries

    class JsonEncoder(json.JSONEncoder):
      def default(self, obj):
        data = dict(obj.__dict__)
        # Queries are stored by sql, so remove the duplicate data. Also don't store
        # profiles as JSON values, but instead separately.
        for k in ("sql", "solo_runtime_profile_with_spilling",
                  "solo_runtime_profile_without_spilling"):
          if k in data:
            del data[k]
        return data
    json.dump(
        store, file, cls=JsonEncoder, sort_keys=True, indent=2, separators=(',', ': '))


def load_runtime_info(path, impala=None):
  """Reads the query runtime information at 'path' and returns a
  dict<db_name, dict<sql, Query>>. Returns an empty dict if the hosts in the 'impala'
  instance do not match the data in 'path'.
  """
  queries_by_db_and_sql = defaultdict(lambda: defaultdict(dict))
  if not os.path.exists(path):
    return queries_by_db_and_sql
  with open(path) as file:
    store = json.load(file)
    _check_store_version(store)
    if (
        impala and
        store.get("host_names") != sorted([i.host_name for i in impala.impalads])
    ):
      return queries_by_db_and_sql
    for db_name, queries_by_sql in store["db_names"].items():
      for sql, queries_by_options in queries_by_sql.items():
        for options, json_query in queries_by_options.items():
          query = Query()
          query.__dict__.update(json_query)
          query.sql = sql
          queries_by_db_and_sql[db_name][sql][options] = query
  return queries_by_db_and_sql


def _check_store_version(store):
  """Clears 'store' if the version is too old or raises an error if the version is too
  new.
  """
  if store["version"] < RUNTIME_INFO_FILE_VERSION:
    LOG.warn("Runtime file info version is old and will be ignored")
    store.clear()
  elif store["version"] > RUNTIME_INFO_FILE_VERSION:
    raise Exception(
        "Unexpected runtime file info version %s expected %s"
        % (store["version"], RUNTIME_INFO_FILE_VERSION))


def print_runtime_info_comparison(old_runtime_info, new_runtime_info):
  # TODO: Provide a way to call this from the CLI. This was hard coded to run from main()
  #       when it was used.
  print(",".join([
      "Database", "Query",
      "Old Mem MB w/Spilling",
      "New Mem MB w/Spilling",
      "Diff %",
      "Old Runtime w/Spilling",
      "New Runtime w/Spilling",
      "Diff %",
      "Old Mem MB wout/Spilling",
      "New Mem MB wout/Spilling",
      "Diff %",
      "Old Runtime wout/Spilling",
      "New Runtime wout/Spilling",
      "Diff %"]))
  for db_name, old_queries in old_runtime_info.items():
    new_queries = new_runtime_info.get(db_name)
    if not new_queries:
      continue
    for sql, old_query in old_queries.items():
      new_query = new_queries.get(sql)
      if not new_query:
        continue
      sys.stdout.write(old_query["db_name"])
      sys.stdout.write(",")
      sys.stdout.write(old_query["name"])
      sys.stdout.write(",")
      for attr in [
          "required_mem_mb_with_spilling", "solo_runtime_secs_with_spilling",
          "required_mem_mb_without_spilling", "solo_runtime_secs_without_spilling"
      ]:
        old_value = old_query[attr]
        sys.stdout.write(str(old_value))
        sys.stdout.write(",")
        new_value = new_query[attr]
        sys.stdout.write(str(new_value))
        sys.stdout.write(",")
        if old_value and new_value is not None:
          sys.stdout.write("%0.2f%%" % (100 * float(new_value - old_value) / old_value))
        else:
          sys.stdout.write("N/A")
        sys.stdout.write(",")
      print()
