#!/usr/bin/env bash
# Copyright 2014 Cloudera Inc.
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
# Downloads and extracts the raw data files for test datasets that are too large for the
# main Impala repo.
# TODO: Migrate this script to the aux test repo.

set -e
set -u

echo "Copying data files from the share.  If the file already exists locally, the files"\
     "will not be copied.  It's not check summing the files or anything like that, if"\
     "you need to force a copy, delete the local directory:"\
     "IMPALA_HOME/testdata/impala-data"

DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
DATADST=${IMPALA_HOME}/testdata/impala-data

mkdir -p ${DATADST}
pushd ${DATADST}

# Download all .tar.gz files from the source, excluding the hostname and directory name.
# If the file already exists locally, skip the download.
wget -q --cut-dirs=1 --no-clobber -r --no-parent -nH --accept="*.tar.gz" ${DATASRC}
for filename in *.tar.gz
do
  echo "Extracting: ${filename}"
  tar -xzf ${filename}
done
popd

echo "Test data download successful."
