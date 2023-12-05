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
# Utilities for supporting different filesystems.
from __future__ import absolute_import, division, print_function
import os

# FILESYSTEM_PREFIX is the path prefix that should be used in queries.  When running
# the tests against the default filesystem (fs.defaultFS), FILESYSTEM_PREFIX is the
# empty string.  When running against a secondary filesystem, it will be the scheme
# and authority portion of the qualified path.
FILESYSTEM_PREFIX = os.getenv("FILESYSTEM_PREFIX") or str()
SECONDARY_FILESYSTEM = os.getenv("SECONDARY_FILESYSTEM") or str()
FILESYSTEM = os.getenv("TARGET_FILESYSTEM")
IS_S3 = FILESYSTEM == "s3"
IS_ISILON = FILESYSTEM == "isilon"
IS_LOCAL = FILESYSTEM == "local"
IS_HDFS = FILESYSTEM == "hdfs"
IS_ADLS = FILESYSTEM == "adls"
IS_ABFS = FILESYSTEM == "abfs"
IS_GCS = FILESYSTEM == "gs"
IS_COS = FILESYSTEM == "cosn"
IS_OSS = FILESYSTEM == "oss"
IS_OBS = FILESYSTEM == "obs"
IS_OZONE = FILESYSTEM == "ozone"
IS_EC = os.getenv("ERASURE_CODING") == "true"
IS_ENCRYPTED = os.getenv("USE_OZONE_ENCRYPTION") == "true"
# This condition satisfies both the states where one can assume a default fs
#   - The environment variable is set to an empty string.
#   - Tne environment variables is unset ( None )
# When the local filesystem is used, it should always be the default filesystem.
IS_DEFAULT_FS = not FILESYSTEM_PREFIX or IS_LOCAL

# Isilon specific values.
ISILON_NAMENODE = os.getenv("ISILON_NAMENODE") or str()
ISILON_WEBHDFS_PORT = 8082

# S3 specific values
S3_BUCKET_NAME = os.getenv("S3_BUCKET")
S3GUARD_ENABLED = os.getenv("S3GUARD_ENABLED") == "true"

# ADLS / ABFS specific values
ABFS_ACCOUNT_NAME = os.getenv("azure_storage_account_name")
ABFS_CONTAINER_NAME = os.getenv("azure_storage_container_name")
ADLS_STORE_NAME = os.getenv("azure_data_lake_store_name")
ADLS_CLIENT_ID = os.getenv("azure_client_id")
ADLS_TENANT_ID = os.getenv("azure_tenant_id")
ADLS_CLIENT_SECRET = os.getenv("azure_client_secret")

# A map of FILESYSTEM values to their corresponding Scan Node types
fs_to_name = {'s3': 'S3', 'hdfs': 'HDFS', 'local': 'LOCAL', 'adls': 'ADLS',
              'abfs': 'ADLS', 'gs': 'GCS', 'cosn': 'COS', 'ozone': 'OZONE',
              'oss': 'OSS', 'obs': 'OBS'}


def get_fs_name(fs):
 """Given the target filesystem, return the name of the associated storage layer"""
 return fs_to_name[fs]


def prepend_with_fs(fs, path):
  """Prepend 'path' with 'fs' if it's not already the prefix."""
  return path if path.startswith(fs) else "%s%s" % (fs, path)


def get_fs_path(path):
  return prepend_with_fs(FILESYSTEM_PREFIX, path)


def get_secondary_fs_path(path):
  return prepend_with_fs(SECONDARY_FILESYSTEM, path)


def get_fs_uri_scheme():
  # Set default URI scheme as "hdfs".
  uri_scheme = "hdfs"
  # Extract URI scheme from DEFAULT_FS environment variable.
  default_fs = os.getenv("DEFAULT_FS")
  if default_fs and default_fs.find(':') != -1:
    uri_scheme = default_fs.split(':')[0]
  return uri_scheme


WAREHOUSE = get_fs_path('/test-warehouse')
FILESYSTEM_NAME = get_fs_name(FILESYSTEM)
WAREHOUSE_PREFIX = os.getenv("WAREHOUSE_LOCATION_PREFIX")
FILESYSTEM_URI_SCHEME = get_fs_uri_scheme()
