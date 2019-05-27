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

from tests.common.environ import (IMPALA_TEST_CLUSTER_PROPERTIES,
    IS_DOCKERIZED_TEST_CLUSTER, IS_BUGGY_EL6_KERNEL, HIVE_MAJOR_VERSION)
from tests.common.kudu_test_suite import get_kudu_master_flag
from tests.util.filesystem_utils import (
    IS_ABFS,
    IS_ADLS,
    IS_EC,
    IS_HDFS,
    IS_ISILON,
    IS_LOCAL,
    IS_S3,
    SECONDARY_FILESYSTEM)


class SkipIfS3:

  # These are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_S3, reason="SET CACHED not implemented for S3")
  hive = pytest.mark.skipif(IS_S3, reason="Hive doesn't work with S3")
  hdfs_block_size = pytest.mark.skipif(IS_S3, reason="S3 uses it's own block size")
  hdfs_acls = pytest.mark.skipif(IS_S3, reason="HDFS acls are not supported on S3")
  jira = partial(pytest.mark.skipif, IS_S3)
  hdfs_encryption = pytest.mark.skipif(IS_S3,
      reason="HDFS encryption is not supported with S3")
  empty_directory = pytest.mark.skipif(IS_S3,
      reason="Empty directories are not supported on S3")

  # These need test infra work to re-enable.
  udfs = pytest.mark.skipif(IS_S3, reason="udas/udfs not copied to S3")
  datasrc = pytest.mark.skipif(IS_S3, reason="data sources not copied to S3")
  hbase = pytest.mark.skipif(IS_S3, reason="HBase not started with S3")
  qualified_path = pytest.mark.skipif(IS_S3,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")
  eventually_consistent = pytest.mark.skipif(IS_S3,
      reason="Flakiness on account of S3 eventual consistency.")


class SkipIfABFS:

  # These are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_ABFS, reason="SET CACHED not implemented for ABFS")
  hive = pytest.mark.skipif(IS_ABFS, reason="Hive doesn't work with ABFS")
  hdfs_block_size = pytest.mark.skipif(IS_ABFS, reason="ABFS uses it's own block size")
  hdfs_acls = pytest.mark.skipif(IS_ABFS, reason="HDFS acls are not supported on ABFS")
  jira = partial(pytest.mark.skipif, IS_ABFS)
  hdfs_encryption = pytest.mark.skipif(IS_ABFS,
      reason="HDFS encryption is not supported with ABFS")
  trash = pytest.mark.skipif(IS_ABFS,
      reason="Drop/purge not working as expected on ABFS, IMPALA-7726")

  # These need test infra work to re-enable.
  udfs = pytest.mark.skipif(IS_ABFS, reason="udas/udfs not copied to ABFS")
  datasrc = pytest.mark.skipif(IS_ABFS, reason="data sources not copied to ABFS")
  hbase = pytest.mark.skipif(IS_ABFS, reason="HBase not started with ABFS")
  qualified_path = pytest.mark.skipif(IS_ABFS,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")

class SkipIfADLS:

  # These are skipped due to product limitations.
  caching = pytest.mark.skipif(IS_ADLS, reason="SET CACHED not implemented for ADLS")
  hive = pytest.mark.skipif(IS_ADLS, reason="Hive doesn't work with ADLS")
  hdfs_block_size = pytest.mark.skipif(IS_ADLS, reason="ADLS uses it's own block size")
  hdfs_acls = pytest.mark.skipif(IS_ADLS, reason="HDFS acls are not supported on ADLS")
  jira = partial(pytest.mark.skipif, IS_ADLS)
  hdfs_encryption = pytest.mark.skipif(IS_ADLS,
      reason="HDFS encryption is not supported with ADLS")

  # These need test infra work to re-enable.
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
  no_hybrid_clock = pytest.mark.skipif(
      get_kudu_master_flag("--use_hybrid_clock") == "false",
      reason="Test relies on --use_hybrid_clock=true in Kudu.")

class SkipIf:
  skip_hbase = pytest.mark.skipif(pytest.config.option.skip_hbase,
      reason="--skip_hbase argument specified")
  kudu_not_supported = pytest.mark.skipif(os.environ["KUDU_IS_SUPPORTED"] == "false",
      reason="Kudu is not supported")
  not_s3 = pytest.mark.skipif(not IS_S3, reason="S3 Filesystem needed")
  not_hdfs = pytest.mark.skipif(not IS_HDFS, reason="HDFS Filesystem needed")
  not_ec = pytest.mark.skipif(not IS_EC, reason="Erasure Coding needed")
  no_secondary_fs = pytest.mark.skipif(not SECONDARY_FILESYSTEM,
      reason="Secondary filesystem needed")
  is_buggy_el6_kernel = pytest.mark.skipif(
      IS_BUGGY_EL6_KERNEL, reason="Kernel is affected by KUDU-1508")


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
  # These are skipped due to product limitations.
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

  # These need test infra work to re-enable.
  hbase = pytest.mark.skipif(IS_LOCAL,
      reason="HBase not started when using local file system")
  hdfs_client = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS not started when using local file system")
  qualified_path = pytest.mark.skipif(IS_LOCAL,
      reason="Tests rely on HDFS qualified paths")
  root_path = pytest.mark.skipif(IS_LOCAL,
      reason="Tests rely on the root directory")

class SkipIfNotHdfsMinicluster:
  # These are skipped when not running against a local HDFS mini-cluster.
  plans = pytest.mark.skipif(not IS_HDFS or pytest.config.option.testing_remote_cluster,
      reason="Test assumes plans from local HDFS mini-cluster")
  tuned_for_minicluster = pytest.mark.skipif(
      not IS_HDFS or IS_EC or pytest.config.option.testing_remote_cluster,
      reason="Test is tuned for 3-node HDFS minicluster with no EC")

class SkipIfBuildType:
  not_dev_build = pytest.mark.skipif(not IMPALA_TEST_CLUSTER_PROPERTIES.is_dev(),
      reason="Test depends on debug build startup option.")
  remote = pytest.mark.skipif(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
      reason="Test depends on running against a local Impala cluster")

class SkipIfEC:
  remote_read = pytest.mark.skipif(IS_EC, reason="EC files are read remotely and "
      "features relying on local read do not work.")
  oom = pytest.mark.skipif(IS_EC, reason="Probably broken by HDFS-13540.")
  fix_later = pytest.mark.skipif(IS_EC, reason="It should work but doesn't.")
  scheduling = pytest.mark.skipif(IS_EC, reason="Scheduling is different on EC")


class SkipIfDockerizedCluster:
  catalog_service_not_exposed = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Catalog service not exposed.")
  statestore_not_exposed = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Statestore service not exposed.")
  internal_hostname = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Internal hostname is used, not local hostname.")
  daemon_logs_not_exposed = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Daemon logs not exposed in host.")
  accesses_host_filesystem = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Daemon would need to access host filesystem.")
  insert_acls = pytest.mark.skipif(IS_DOCKERIZED_TEST_CLUSTER,
      reason="IMPALA-8384: insert ACL tests are broken on dockerised minicluster.")


class SkipIfHive3:
  sentry_not_supported = pytest.mark.skipif(HIVE_MAJOR_VERSION >= 3,
      reason="Sentry HMS follower does not work with HMS-3. See SENTRY-2518 for details")
  slow_nested_types = pytest.mark.skipif(HIVE_MAJOR_VERSION >= 3,
      reason="Deeply nested types can be slow in Hive 3. See HIVE-21796 for details")


class SkipIfHive2:
  acid = pytest.mark.skipif(HIVE_MAJOR_VERSION == 2,
      reason="Acid tables are only supported with Hive 3.")


class SkipIfCatalogV2:
  """Expose decorators as methods so that is_catalog_v2_cluster() can be evaluated lazily
  when needed, instead of whenever this module is imported."""
  @classmethod
  def stats_pulling_disabled(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="Local catalog does not use incremental stats pulling.")

  @classmethod
  def catalog_v1_test(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="Test is specific to old implementation of catalog.")

  # TODO: IMPALA-8486: fix invalidation or update tests to reflect expected behaviour.
  @classmethod
  def lib_cache_invalidation_broken(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-8486: LibCache isn't invalidated by function DDL.")

  # TODO: IMPALA-7131: add support or update tests to reflect expected behaviour.
  @classmethod
  def data_sources_unsupported(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-7131: data sources not supported.")

  # TODO: IMPALA-7538: add support or update tests to reflect expected behaviour.
  @classmethod
  def hdfs_caching_ddl_unsupported(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-7538: HDFS caching DDL not supported.")

  # TODO: IMPALA-8489: fix this bug.
  @classmethod
  def impala_8489(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-8489: TestRecoverPartitions.test_post_invalidate "
             "IllegalStateException.")

  # TODO: IMPALA-8459: fix this bug.
  @classmethod
  def impala_8459(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-8459: some kudu DDL is broken for local catalog")

  # TODO: IMPALA-7539: fix this bug.
  @classmethod
  def impala_7539(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-7539: support HDFS permission checks for LocalCatalog")
