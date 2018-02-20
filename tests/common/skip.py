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
# Impala py.test skipif markers.  When a test can't be run against S3,
# choose the appropriate reason (or add a new one if needed) and
# annotate the class or test routine with the marker.
#

import os
import pytest
from functools import partial

from tests.common.environ import IMPALAD_BUILD
from tests.util.filesystem_utils import (
    IS_ISILON,
    IS_LOCAL,
    IS_HDFS,
    IS_S3,
    IS_ADLS,
    SECONDARY_FILESYSTEM)


class SkipIfS3:

  # These ones are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_S3, reason="SET CACHED not implemented for S3")
  hive = pytest.mark.skipif(IS_S3, reason="Hive doesn't work with S3")
  hdfs_block_size = pytest.mark.skipif(IS_S3, reason="S3 uses it's own block size")
  hdfs_acls = pytest.mark.skipif(IS_S3, reason="HDFS acls are not supported on S3")
  jira = partial(pytest.mark.skipif, IS_S3)
  hdfs_encryption = pytest.mark.skipif(IS_S3,
      reason="HDFS encryption is not supported with S3")

  # These ones need test infra work to re-enable.
  udfs = pytest.mark.skipif(IS_S3, reason="udas/udfs not copied to S3")
  datasrc = pytest.mark.skipif(IS_S3, reason="data sources not copied to S3")
  hbase = pytest.mark.skipif(IS_S3, reason="HBase not started with S3")
  qualified_path = pytest.mark.skipif(IS_S3,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")

class SkipIfADLS:

  # These ones are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_ADLS, reason="SET CACHED not implemented for ADLS")
  hive = pytest.mark.skipif(IS_ADLS, reason="Hive doesn't work with ADLS")
  hdfs_block_size = pytest.mark.skipif(IS_ADLS, reason="ADLS uses it's own block size")
  hdfs_acls = pytest.mark.skipif(IS_ADLS, reason="HDFS acls are not supported on ADLS")
  jira = partial(pytest.mark.skipif, IS_ADLS)
  hdfs_encryption = pytest.mark.skipif(IS_ADLS,
      reason="HDFS encryption is not supported with ADLS")

  # These ones need test infra work to re-enable.
  udfs = pytest.mark.skipif(IS_ADLS, reason="udas/udfs not copied to ADLS")
  datasrc = pytest.mark.skipif(IS_ADLS, reason="data sources not copied to ADLS")
  hbase = pytest.mark.skipif(IS_ADLS, reason="HBase not started with ADLS")
  qualified_path = pytest.mark.skipif(IS_ADLS,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")
  eventually_consistent = pytest.mark.skipif(IS_ADLS,
      reason="The client is slow to realize changes to file metadata")

class SkipIfKudu:
  unsupported_env = pytest.mark.skipif(os.environ["KUDU_IS_SUPPORTED"] == "false",
      reason="Kudu is not supported in this environment")

class SkipIf:
  skip_hbase = pytest.mark.skipif(pytest.config.option.skip_hbase,
      reason="--skip_hbase argument specified")
  kudu_not_supported = pytest.mark.skipif(os.environ["KUDU_IS_SUPPORTED"] == "false",
      reason="Kudu is not supported")
  not_s3 = pytest.mark.skipif(not IS_S3, reason="S3 Filesystem needed")
  not_hdfs = pytest.mark.skipif(not IS_HDFS, reason="HDFS Filesystem needed")
  no_secondary_fs = pytest.mark.skipif(not SECONDARY_FILESYSTEM,
      reason="Secondary filesystem needed")
  not_krpc = pytest.mark.skipif(pytest.config.option.test_no_krpc,
      reason="Test is only supported when using KRPC.")
  not_thrift = pytest.mark.skipif(not pytest.config.option.test_no_krpc,
      reason="Test is only supported when using Thrift RPC.")

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
  hdfs_fd_caching = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS file handle caching not supported for local non-HDFS files")

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

class SkipIfNotHdfsMinicluster:
  # These ones are skipped when not running against a local HDFS mini-cluster.
  plans = pytest.mark.skipif(not IS_HDFS or pytest.config.option.testing_remote_cluster,
      reason="Test assumes plans from local HDFS mini-cluster")

class SkipIfBuildType:
  not_dev_build = pytest.mark.skipif(not IMPALAD_BUILD.is_dev(),
      reason="Tests depends on debug build startup option.")
