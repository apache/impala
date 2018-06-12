#!/usr/bin/env bash
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

# $1 - gdb binary path
# $2 - pid of the Impala process
# $3 - Output directory to copy the sharedlibs to.

set -euxo pipefail

if [ "$#" -ne 3 ]; then
  echo "Incorrect usage. Expected: $0 <gdb executable path> <target PID> <output dir>"
  exit 1
fi

gdb=$1
target=$2
dest_dir=$3

if [ ! -d $dest_dir ]; then
  echo "Directory $dest_dir does not exist. This script expects the output directory to exist."
  exit 1
fi

# Generate the list of shared libs path to copy.
shared_libs_to_copy=$(mktemp)
$gdb --pid $target --batch -ex 'info shared' 2> /dev/null | sed '1,/Shared Object Library/d' |
    sed 's/\(.*\s\)\(\/.*\)/\2/' | grep \/ > $shared_libs_to_copy

echo "Generated shared library listing for the process."

# Copy the files to the target directory keeping the directory structure intact.
# We use rsync instead of 'cp --parents' since the latter has permission issues
# copying from system level directories. https://goo.gl/6yYNhw
rsync -LR --files-from=$shared_libs_to_copy / $dest_dir

echo "Copied the shared libraries to the target directory: $dest_dir"

rm -f $shared_libs_to_copy
# Make sure the impala user has write permissions on all the copied sharedlib paths.
chmod 755 -R $dest_dir
