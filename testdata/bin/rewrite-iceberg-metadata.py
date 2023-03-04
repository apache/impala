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

from __future__ import absolute_import, division, print_function
from builtins import map
import glob
import json
import os
import sys

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

args = sys.argv[1:]
if len(args) < 2:
  print("Usage: rewrite-iceberg-metadata.py PREFIX METADATA-dirs...")
  exit(1)

prefix = args[0]

# Easier to cache it instead of trying to resolve the manifest files paths
file_size_cache = {}


def generate_new_path(prefix, file_path):
  """ Hive generates metadata with absolute paths.
  This method relativizes the path and applies a new prefix."""
  start_directory = "/test-warehouse"
  start = file_path.find(start_directory)
  if start == -1:
    raise RuntimeError("{} is not found in file path:{}".format(
      start_directory, file_path))
  return prefix + file_path[start:]


def add_prefix_to_snapshot(snapshot):
  if 'manifest-list' in snapshot:
    snapshot['manifest-list'] = generate_new_path(prefix, snapshot['manifest-list'])
  if 'manifests' in snapshot:
    snapshot['manifests'] = [generate_new_path(prefix, m) for m in snapshot['manifests']]
  return snapshot


def add_prefix_to_mlog(metadata_log):
  metadata_log['metadata-file'] = generate_new_path(prefix, metadata_log['metadata-file'])
  return metadata_log


def add_prefix_to_snapshot_entry(entry):
  if 'manifest_path' in entry:
    entry['manifest_path'] = generate_new_path(prefix, entry['manifest_path'])
  if 'data_file' in entry:
    entry['data_file']['file_path'] = generate_new_path(prefix,
                                      entry['data_file']['file_path'])
  return entry


def fix_manifest_length(entry):
  if 'manifest_path' in entry and 'manifest_length' in entry:
    filename = entry['manifest_path'].split('/')[-1]
    if filename in file_size_cache:
      entry['manifest_length'] = file_size_cache[filename]
  return entry


for arg in args[1:]:
  # Update metadata.json
  for mfile in glob.glob(os.path.join(arg, '*.metadata.json')):
    with open(mfile, 'r') as f:
      metadata = json.load(f)

    if 'format-version' not in metadata:
      print("WARN: skipping {}, missing format-version".format(f))
      continue

    version = metadata['format-version']
    if version < 1 or version > 2:
      print("WARN: skipping {}, unknown version {}".format(f, version))
      continue

    # metadata: required
    metadata['location'] = generate_new_path(prefix, metadata['location'])

    # snapshots: optional
    if 'snapshots' in metadata:
      metadata['snapshots'] = list(map(add_prefix_to_snapshot, metadata['snapshots']))

    # metadata-log: optional
    if 'metadata-log' in metadata:
      metadata['metadata-log'] = list(map(add_prefix_to_mlog, metadata['metadata-log']))

    with open(mfile + '.tmp', 'w') as f:
      json.dump(metadata, f, indent=2)
    os.rename(mfile + '.tmp', mfile)

  for afile in glob.glob(os.path.join(arg, '*.avro')):
    with open(afile, 'rb') as f:
      with DataFileReader(f, DatumReader()) as reader:
        schema = reader.datum_reader.writers_schema
        lines = list(map(add_prefix_to_snapshot_entry, reader))

    with open(afile + '.tmp', 'wb') as f:
      with DataFileWriter(f, DatumWriter(), schema) as writer:
        for line in lines:
          writer.append(line)
    os.rename(afile + '.tmp', afile)
    filename = afile.split('/')[-1]
    file_size_cache[filename] = os.path.getsize(afile)

  for snapfile in glob.glob(os.path.join(arg, 'snap*.avro')):
    with open(snapfile, 'rb') as f:
      with DataFileReader(f, DatumReader()) as reader:
        schema = reader.datum_reader.writers_schema
        lines = list(map(fix_manifest_length, reader))

    with open(snapfile + '.tmp', 'wb') as f:
      with DataFileWriter(f, DatumWriter(), schema) as writer:
        for line in lines:
          writer.append(line)
    os.rename(snapfile + '.tmp', snapfile)
