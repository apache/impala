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
#
# This script creates a tarball of the ~/.m2 directory that is
# intended to be used to prepopulate the .m2 directory. Since
# we want CDH/CDP dependencies to come from impala.cdh.repo or
# impala.cdp.repo, it parses the maven log to detect artifacts
# that come from those repositories and does not include them
# in the tarball. The script does not make any changes to the
# ~/.m2 directory.
#
# The script takes two arguments: the maven log and the filename
# for the output tarball.
# archive_m2_directory.sh [maven_log] [output_tarball]
#
# There are two requirements for the maven log:
# 1. It needs to be produced by a recent maven version (such as 3.5.4 installed by
# bin/bootstrap_system.sh). This is required because recent maven outputs line like:
# [INFO] Downloading from {repo}: {url}
# [INFO] Downloaded from {repo}: {url}
# Older maven (e.g. 3.3.9) omits the "from {repo}" part.
# 2. Maven needs to run in batch mode (-B). This keeps the output from using special
# characters to format things on the console (e.g. carriage return ^M).
set -euo pipefail

MVN_LOG=$1
OUTFILE=$2

TMP_DIR=$(mktemp -d)
ARCHIVE_DIR=${TMP_DIR}/repository

function onexit {
  echo "$0: Cleaning up temporary directory"
  rm -rf ${TMP_DIR}
}
trap onexit EXIT

# Make our own copy of .m2 in a temp directory
mkdir -p ${ARCHIVE_DIR}
cp -R ~/.m2/repository/* ${ARCHIVE_DIR}

# We want to remove artifacts/directories that belong to impala.cdh.repo or
# impala.cdp.repo. This greps the maven log to get all the URLs that we
# downloaded. It knows the form of the URL for impala.cdh.repo and
# impala.cdp.repo (i.e. that the actual directory structure starts after
# '/maven/'. Extract out the directory/filename downloaded.
cat "${MVN_LOG}" | grep "Downloaded from" | sed 's|.* Downloaded from ||' \
    | grep -e 'impala.cdp.repo' -e 'impala.cdh.repo' | cut -d' ' -f2 \
    | sed 's|.*/maven/||' > ${TMP_DIR}/cdp_cdh_artifacts.txt

# Simplify it to a list of directories that contain artifacts from impala.cdp.repo
# and impala.cdh.repo. SNAPSHOT artifacts like maven-metadata.xml can result in
# multiple artifacts in the directory, so it is useful to get rid of the whole
# directory.
cat ${TMP_DIR}/cdp_cdh_artifacts.txt | xargs dirname | sort \
    | uniq > ${TMP_DIR}/cdp_cdh_directories.txt

# Remove the directories from our copy of m2
for dir in $(cat ${TMP_DIR}/cdp_cdh_directories.txt); do
    DIRECTORY=${ARCHIVE_DIR}/${dir}
    echo "Removing directory ${DIRECTORY}"
    rm -rf "${DIRECTORY}"
done

# Tar it up
tar -zcf ${OUTFILE} -C ${TMP_DIR} repository

# Note: The exit callback handles cleanup of the temp directory.
