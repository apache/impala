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
from builtins import map
from functools import partial
import glob
import json
import os

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def rewrite_metadata(prefix, unique_database, metadata_dir):
  # Update metadata.json
  for mfile in glob.glob(os.path.join(metadata_dir, '*.metadata.json')):
    with open(mfile, 'r') as f:
      metadata = json.load(f)

    if 'format-version' not in metadata:
      print("WARN: skipping {}, missing format-version".format(f))
      continue

    version = metadata['format-version']
    if version < 1 or version > 2:
      print("WARN: skipping {}, unknown version {}".format(f, version))
      continue

    metadata_parent_dir = os.path.split(metadata_dir.rstrip("/"))[0]
    table_name = os.path.basename(metadata_parent_dir)

    table_params = (prefix, unique_database, table_name)

    # metadata: required
    metadata['location'] = generate_new_path(
        table_params, metadata['location'])

    # snapshots: optional
    if 'snapshots' in metadata:
      metadata['snapshots'] = \
          list(map(partial(add_prefix_to_snapshot, table_params), metadata['snapshots']))

    # metadata-log: optional
    if 'metadata-log' in metadata:
      metadata['metadata-log'] = \
          list(map(partial(add_prefix_to_mlog, table_params), metadata['metadata-log']))

    if 'statistics' in metadata:
      metadata['statistics'] = list(map(
          partial(add_prefix_to_statistics, table_params), metadata['statistics']))

    with open(mfile + '.tmp', 'w') as f:
      json.dump(metadata, f, indent=2)
    os.rename(mfile + '.tmp', mfile)

  # Easier to cache it instead of trying to resolve the manifest files paths
  file_size_cache = {}

  for afile in glob.glob(os.path.join(metadata_dir, '*.avro')):
    with open(afile, 'rb') as f:
      with DataFileReader(f, DatumReader()) as reader:
        schema = reader.datum_reader.writers_schema
        lines = list(map(
            partial(add_prefix_to_snapshot_entry, table_params),
            reader))

    with open(afile + '.tmp', 'wb') as f:
      with DataFileWriter(f, DatumWriter(), schema) as writer:
        for line in lines:
          writer.append(line)
    os.rename(afile + '.tmp', afile)
    filename = afile.split('/')[-1]
    file_size_cache[filename] = os.path.getsize(afile)

  for snapfile in glob.glob(os.path.join(metadata_dir, 'snap*.avro')):
    with open(snapfile, 'rb') as f:
      with DataFileReader(f, DatumReader()) as reader:
        schema = reader.datum_reader.writers_schema
        lines = list(map(partial(fix_manifest_length, file_size_cache), reader))

    with open(snapfile + '.tmp', 'wb') as f:
      with DataFileWriter(f, DatumWriter(), schema) as writer:
        for line in lines:
          writer.append(line)
    os.rename(snapfile + '.tmp', snapfile)


def generate_new_path(table_params, file_path):
  """ Hive generates metadata with absolute paths.
  This method relativizes the path and applies a new prefix."""
  prefix, unique_database, table_name = table_params
  start_directory = "/test-warehouse"
  start = file_path.find(start_directory)
  if start == -1:
    raise RuntimeError("{} is not found in file path:{}".format(
      start_directory, file_path))

  result = file_path[start:]
  if prefix:
    result = prefix + result

  if not unique_database:
    return result

  def replace_last(s, old_expr, new_expr):
    maxsplit = 1
    li = s.rsplit(old_expr, maxsplit)
    assert len(li) == 2
    return new_expr.join(li)

  return replace_last(result, table_name, "{}/{}".format(unique_database, table_name))


def add_prefix_to_snapshot(table_params, snapshot):
  if 'manifest-list' in snapshot:
    snapshot['manifest-list'] = generate_new_path(table_params, snapshot['manifest-list'])
  if 'manifests' in snapshot:
    snapshot['manifests'] = [
        generate_new_path(table_params, m) for m in snapshot['manifests']]
  return snapshot


def add_prefix_to_mlog(table_params, metadata_log):
  metadata_log['metadata-file'] = generate_new_path(
      table_params, metadata_log['metadata-file'])
  return metadata_log


def add_prefix_to_statistics(table_params, statistics):
  statistics['statistics-path'] = generate_new_path(
      table_params, statistics['statistics-path'])
  return statistics


def add_prefix_to_snapshot_entry(table_params, entry):
  if 'manifest_path' in entry:
    entry['manifest_path'] = generate_new_path(table_params, entry['manifest_path'])
  if 'data_file' in entry:
    entry['data_file']['file_path'] = generate_new_path(
        table_params, entry['data_file']['file_path'])
  return entry


def fix_manifest_length(file_size_cache, entry):
  if 'manifest_path' in entry and 'manifest_length' in entry:
    filename = entry['manifest_path'].split('/')[-1]
    if filename in file_size_cache:
      entry['manifest_length'] = file_size_cache[filename]
  return entry
