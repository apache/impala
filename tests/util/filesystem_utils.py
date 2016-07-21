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

def get_fs_path(path):
  return "%s%s" % (FILESYSTEM_PREFIX, path)

def get_secondary_fs_path(path):
  return "%s%s" % (SECONDARY_FILESYSTEM, path)

WAREHOUSE = get_fs_path('/test-warehouse')
