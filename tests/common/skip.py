# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
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
# Impala py.test skipif markers.  When a test can't be run against S3,
# choose the appropriate reason (or add a new one if needed) and
# annotate the class or test routine with the marker.
#

import re
import os
import pytest
from functools import partial
from tests.util.filesystem_utils import IS_DEFAULT_FS, IS_S3, IS_ISILON, IS_LOCAL


class SkipIfS3:

  # These ones are skipped due to product limitations.
  insert = pytest.mark.skipif(IS_S3, reason="INSERT not implemented for S3")
  load_data = pytest.mark.skipif(IS_S3, reason="LOAD DATA not implemented for S3")
  caching = pytest.mark.skipif(IS_S3, reason="SET CACHED not implemented for S3")
  hive = pytest.mark.skipif(IS_S3, reason="Hive doesn't work with S3")
  hdfs_block_size = pytest.mark.skipif(IS_S3, reason="S3 uses it's own block size")
  jira = partial(pytest.mark.skipif, IS_S3)

  # These ones need test infra work to re-enable.
  udfs = pytest.mark.skipif(IS_S3, reason="udas/udfs not copied to S3")
  datasrc = pytest.mark.skipif(IS_S3, reason="data sources not copied to S3")
  hdfs_client = pytest.mark.skipif(IS_S3, reason="hdfs_client doesn't work with S3")
  hbase = pytest.mark.skipif(IS_S3, reason="HBase not started with S3")
  qualified_path = pytest.mark.skipif(IS_S3,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")

class SkipIf:
  # Some tests require a non-default filesystem to be present.
  default_fs = pytest.mark.skipif(IS_DEFAULT_FS, reason="Non-default filesystem needed")
  skip_hbase = pytest.mark.skipif(pytest.config.option.skip_hbase,
      reason="--skip_hbase argument specified")
  not_default_fs = pytest.mark.skipif(not IS_DEFAULT_FS,
      reason="Default filesystem needed")


class SkipIfIsilon:
  caching = pytest.mark.skipif(IS_ISILON, reason="SET CACHED not implemented for Isilon")
  hbase = pytest.mark.skipif(IS_ISILON, reason="HBase not tested with Isilon")
  hive = pytest.mark.skipif(IS_ISILON, reason="Hive not tested with Isilon")
  hdfs_acls = pytest.mark.skipif(IS_ISILON, reason="HDFS acls are not supported on Isilon")
  hdfs_block_size = pytest.mark.skipif(IS_ISILON,
      reason="Isilon uses its own block size")
  hdfs_encryption = pytest.mark.skipif(IS_ISILON,
      reason="HDFS encryption is not supported with Isilon")
  untriaged = pytest.mark.skipif(IS_ISILON,
      reason="This Isilon issue has yet to be triaged.")
  jira = partial(pytest.mark.skipif, IS_ISILON)

# TODO: looking at TEST_START_CLUSTER_ARGS is a hack. It would be better to add an option
# to pytest.
test_start_cluster_args = os.environ.get("TEST_START_CLUSTER_ARGS","")
old_agg_regex = "enable_partitioned_aggregation=false"
old_hash_join_regex = "enable_partitioned_hash_join=false"
using_old_aggs_joins = re.search(old_agg_regex, test_start_cluster_args) is not None or \
    re.search(old_hash_join_regex, test_start_cluster_args) is not None

class SkipIfOldAggsJoins:
  nested_types = pytest.mark.skipif(using_old_aggs_joins,
      reason="Nested types not supported with old aggs and joins")
  unsupported = pytest.mark.skipif(using_old_aggs_joins,
      reason="Query unsupported with old aggs and joins")

class SkipIfLocal:
  # These ones are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS caching not supported on local file system")
  hdfs_blocks = pytest.mark.skipif(IS_LOCAL,
      reason="Files on local filesystem are not split into blocks")
  hdfs_encryption = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS encryption is not supported on local filesystem")
  hive = pytest.mark.skipif(IS_LOCAL,
      reason="Hive not started when using local file system")
  multiple_impalad = pytest.mark.skipif(IS_LOCAL,
      reason="Multiple impalads are not supported when using local file system")
  parquet_file_size = pytest.mark.skipif(IS_LOCAL,
      reason="Parquet block size incorrectly determined")

  # These ones need test infra work to re-enable.
  hbase = pytest.mark.skipif(IS_LOCAL,
      reason="HBase not started when using local file system")
  hdfs_client = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS not started when using local file system")
  mem_usage_different = pytest.mark.skipif(IS_LOCAL,
      reason="Memory limit too low when running single node")
  qualified_path = pytest.mark.skipif(IS_LOCAL,
      reason="Tests rely on HDFS qualified paths")
  root_path = pytest.mark.skipif(IS_LOCAL,
      reason="Tests rely on the root directory")

# Try to derive the build type. Assume it's 'latest' by default.
impala_home = os.environ.get("IMPALA_HOME", "")
build_type_regex = re.compile(r'--build_type=(\w+)', re.I)
build_type_search_result = re.search(build_type_regex, test_start_cluster_args)

if build_type_search_result is not None:
  build_type = build_type_search_result.groups()[0].lower()
else:
  build_type = 'latest'

# Resolve any symlinks in the path.
impalad_basedir = \
    os.path.realpath(os.path.join(impala_home, 'be/build', build_type)).rstrip('/')
debug_build = os.path.basename(impalad_basedir).lower() == 'debug'

class SkipIfNotDebugBuild:
  debug_only = pytest.mark.skipif(not debug_build,
      reason="Tests depends on debug build startup option.")
