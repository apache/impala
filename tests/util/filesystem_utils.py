# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Utilities for supporting different filesystems.
import os

FILESYSTEM_PREFIX = os.getenv("FILESYSTEM_PREFIX") or str()
FILESYSTEM = os.getenv("TARGET_FILESYSTEM")
# This condition satisfies both the states where one can assume a default fs
#   - The environment variable is set to an empty string.
#   - Tne environment variables is unset ( None )
IS_DEFAULT_FS = not FILESYSTEM_PREFIX
IS_S3 = FILESYSTEM == "s3"
IS_ISILON = FILESYSTEM == "isilon"

# Isilon specific values.
ISILON_WEBHDFS_PORT = 8082

def get_fs_path(path):
  return "%s%s" % (FILESYSTEM_PREFIX, path)

WAREHOUSE = get_fs_path('/test-warehouse')

