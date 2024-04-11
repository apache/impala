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

from __future__ import absolute_import, division, print_function
import pytest
from functools import partial

from tests.common.environ import (ImpalaTestClusterProperties,
                                  IS_DOCKERIZED_TEST_CLUSTER, IS_BUGGY_EL6_KERNEL,
                                  HIVE_MAJOR_VERSION, IS_REDHAT_6_DERIVATIVE,
                                  IS_APACHE_HIVE, IS_TEST_JDK)
from tests.common.kudu_test_suite import get_kudu_master_flag
from tests.util.filesystem_utils import (
    IS_ABFS,
    IS_ADLS,
    IS_GCS,
    IS_COS,
    IS_OSS,
    IS_OBS,
    IS_EC,
    IS_HDFS,
    IS_ISILON,
    IS_LOCAL,
    IS_S3,
    IS_OZONE,
    SECONDARY_FILESYSTEM)

IMPALA_TEST_CLUSTER_PROPERTIES = ImpalaTestClusterProperties.get_instance()


class SkipIfFS:
  # These are skipped due to product limitations.
  hdfs_caching = pytest.mark.skipif(not IS_HDFS, reason="SET CACHED not implemented")
  hdfs_encryption = pytest.mark.skipif(not IS_HDFS,
      reason="HDFS encryption is not supported")
  # EC reports block groups of 3 blocks, and the minimum block size is 1MB.
  hdfs_small_block = pytest.mark.skipif(not IS_HDFS or IS_EC,
      reason="Requires tables with 1MB block size")
  hdfs_block_size = pytest.mark.skipif(not IS_HDFS,
      reason="Size of block reported to Impala is not ~128MB")
  hdfs_acls = pytest.mark.skipif(not IS_HDFS, reason="HDFS acls are not supported")

  # Special case product limitations.
  empty_directory = pytest.mark.skipif(IS_S3,
      reason="Empty directories are not supported on S3")
  file_or_folder_name_ends_with_period = pytest.mark.skipif(IS_ABFS,
      reason="ABFS does not support file / directories that end with a period")
  stress_insert_timeouts = pytest.mark.skipif(IS_COS or IS_GCS or IS_OSS or IS_OBS,
      reason="IMPALA-10563, IMPALA-10773")
  shutdown_idle_fails = pytest.mark.skipif(IS_COS or IS_GCS or IS_OSS or IS_OBS,
      reason="IMPALA-10562")
  late_filters = pytest.mark.skipif(IS_ISILON, reason="IMPALA-6998")
  read_past_eof = pytest.mark.skipif(IS_S3 or IS_GCS or (IS_OZONE and IS_EC),
      reason="IMPALA-2512")
  large_block_size = pytest.mark.skipif(IS_OZONE or IS_EC,
      reason="block size is larger than 128MB")
  read_speed_dependent = pytest.mark.skipif(not IS_HDFS or IS_EC,
      reason="success depends on fast scan node performance")
  incorrent_reported_ec = pytest.mark.skipif(IS_OZONE and IS_EC, reason="HDDS-8543")

  # These need test infra work to re-enable.
  hive = pytest.mark.skipif(not IS_HDFS, reason="Hive doesn't work")
  hbase = pytest.mark.skipif(not IS_HDFS, reason="HBase not started")
  qualified_path = pytest.mark.skipif(not IS_HDFS,
      reason="Tests rely on HDFS qualified paths, IMPALA-1872")
  no_partial_listing = pytest.mark.skipif(not IS_HDFS,
      reason="Tests rely on HDFS partial listing.")
  variable_listing_times = pytest.mark.skipif(IS_S3 or IS_GCS or IS_COS or IS_OSS
      or IS_OBS, reason="Flakiness due to unpredictable listing times on S3.")
  eventually_consistent = pytest.mark.skipif(IS_ADLS or IS_COS or IS_OSS or IS_OBS,
      reason="The client is slow to realize changes to file metadata")


class SkipIfKudu:
  """Expose decorators as methods so that kudu web pages can be checked lazily when
     needed, instead of whenever this module is imported. This helps to run Kudu
     unrelated tests without launching the Kudu cluster."""

  @classmethod
  def no_hybrid_clock(cls):
    return pytest.mark.skipif(
        get_kudu_master_flag("--use_hybrid_clock") == "false",
        reason="Test relies on --use_hybrid_clock=true in Kudu.")

  @classmethod
  def hms_integration_enabled(cls):
    return pytest.mark.skipif(
        get_kudu_master_flag("--hive_metastore_uris") != "",
        reason="Test assumes Kudu/HMS integration is not enabled.")


class SkipIf:
  skip_hbase = pytest.mark.skipif(pytest.config.option.skip_hbase,
      reason="--skip_hbase argument specified")
  not_s3 = pytest.mark.skipif(not IS_S3, reason="S3 Filesystem needed")
  not_hdfs = pytest.mark.skipif(not IS_HDFS, reason="HDFS admin needed")
  not_dfs = pytest.mark.skipif(not (IS_HDFS or IS_OZONE),
      reason="HDFS/Ozone Filesystem needed")
  not_scratch_fs = pytest.mark.skipif(not (IS_HDFS or IS_OZONE),
      reason="Scratch dirs for temporary file spilling not supported")
  sfs_unsupported = pytest.mark.skipif(not (IS_HDFS or IS_S3 or IS_ABFS or IS_ADLS
      or IS_GCS), reason="Hive support for sfs+ is limited, HIVE-26757")
  hardcoded_uris = pytest.mark.skipif(not IS_HDFS,
      reason="Iceberg delete files hardcode the full URI in parquet files")
  not_ec = pytest.mark.skipif(not IS_EC, reason="Erasure Coding needed")
  no_secondary_fs = pytest.mark.skipif(not SECONDARY_FILESYSTEM,
      reason="Secondary filesystem needed")
  is_buggy_el6_kernel = pytest.mark.skipif(
      IS_BUGGY_EL6_KERNEL, reason="Kernel is affected by KUDU-1508")
  is_test_jdk = pytest.mark.skipif(IS_TEST_JDK, reason="Testing with different JDK")
  runs_slowly = pytest.mark.skipif(IMPALA_TEST_CLUSTER_PROPERTIES.runs_slowly(),
      reason="Test cluster runs slowly due to enablement of code coverage or sanitizer")

class SkipIfLocal:
  # These are skipped due to product limitations.
  hdfs_blocks = pytest.mark.skipif(IS_LOCAL,
      reason="Files on local filesystem are not split into blocks")
  multiple_impalad = pytest.mark.skipif(IS_LOCAL,
      reason="Multiple impalads are not supported when using local file system")
  parquet_file_size = pytest.mark.skipif(IS_LOCAL,
      reason="Parquet block size incorrectly determined")
  hdfs_fd_caching = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS file handle caching not supported for local non-HDFS files")

  # These need test infra work to re-enable.
  hdfs_client = pytest.mark.skipif(IS_LOCAL,
      reason="HDFS not started when using local file system")
  root_path = pytest.mark.skipif(IS_LOCAL,
      reason="Tests rely on the root directory")

class SkipIfNotHdfsMinicluster:
  # These are skipped when not running against a local HDFS mini-cluster.
  plans = pytest.mark.skipif(
      not (IS_HDFS or IS_OZONE) or IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
      reason="Test assumes plans from local HDFS mini-cluster")
  tuned_for_minicluster = pytest.mark.skipif(not (IS_HDFS or IS_OZONE)
      or IS_EC or IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
      reason="Test is tuned for 3-node HDFS minicluster with no EC")
  scheduling = pytest.mark.skipif(
      not (IS_HDFS or IS_OZONE) or IS_EC or pytest.config.option.testing_remote_cluster,
      reason="Test is tuned for scheduling decisions made on a 3-node HDFS minicluster "
             "with no EC")

class SkipIfBuildType:
  dev_build = pytest.mark.skipif(IMPALA_TEST_CLUSTER_PROPERTIES.is_dev(),
      reason="Test takes too much time on debug build.")
  not_dev_build = pytest.mark.skipif(not IMPALA_TEST_CLUSTER_PROPERTIES.is_dev(),
      reason="Test depends on debug build startup option.")
  remote = pytest.mark.skipif(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
      reason="Test depends on running against a local Impala cluster")

class SkipIfEC:
  contain_full_explain = pytest.mark.skipif(IS_EC, reason="Contain full explain output "
              "for hdfs tables.")
  different_scan_split = pytest.mark.skipif(IS_EC, reason="Scan split of row "
              "groups for Parquet tables created in EC mode is different.")
  parquet_file_size = pytest.mark.skipif(IS_EC,
      reason="Fewer parquet files due to large block size, reducing parallelism")


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
  insufficient_mem_limit = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Test require high per-process mem_limit.")
  runs_slowly = pytest.mark.skipif(
      IS_DOCKERIZED_TEST_CLUSTER, reason="Dockerized env too slow for test.")
  jira = partial(pytest.mark.skipif, IS_DOCKERIZED_TEST_CLUSTER)


class SkipIfHive3:
  col_stat_separated_by_engine = pytest.mark.skipif(HIVE_MAJOR_VERSION >= 3,
      reason="Hive 3 separates column statistics by engine")
  without_hms_not_supported = pytest.mark.skipif(HIVE_MAJOR_VERSION >= 3,
      reason="Instantiating HMS server in embedded mode within Hive client requires more "
             "dependencies of Hive 3, see IMPALA-9287.")
  non_acid = pytest.mark.skipif(HIVE_MAJOR_VERSION >= 3,
      reason="This test expects tables in non-AICD format.")


class SkipIfHive2:
  acid = pytest.mark.skipif(HIVE_MAJOR_VERSION == 2,
      reason="Acid tables are only supported with Hive 3.")
  col_stat_not_separated_by_engine = pytest.mark.skipif(HIVE_MAJOR_VERSION == 2,
      reason="Hive 2 doesnt support separating column statistics by engine")
  create_external_kudu_table = pytest.mark.skipif(HIVE_MAJOR_VERSION == 2,
      reason="Hive 2 does not support creating external.table.purge Kudu tables."
             " See IMPALA-9092 for details.")
  orc = pytest.mark.skipif(HIVE_MAJOR_VERSION <= 2,
      reason="CREATE TABLE LIKE ORC is only supported with Hive version >= 3")
  ranger_auth = pytest.mark.skipif(HIVE_MAJOR_VERSION <= 2,
      reason="Hive 2 doesn't support Ranger authorization.")

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

  # TODO: IMPALA-8489: fix this bug.
  @classmethod
  def impala_8489(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-8489: TestRecoverPartitions.test_post_invalidate "
             "IllegalStateException.")

  # TODO: IMPALA-7539: fix this bug.
  @classmethod
  def impala_7539(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="IMPALA-7539: support HDFS permission checks for LocalCatalog")

  @classmethod
  def hms_event_polling_enabled(self):
    return pytest.mark.skipif(
      IMPALA_TEST_CLUSTER_PROPERTIES.is_catalog_v2_cluster(),
      reason="Table isn't invalidated with Local catalog and enabled hms_event_polling.")

  @classmethod
  def hms_event_polling_disabled(self):
    return pytest.mark.skipif(
      not IMPALA_TEST_CLUSTER_PROPERTIES.is_event_polling_enabled(),
      reason="Test expects event polling to be enabled.")

class SkipIfOS:
  redhat6 = pytest.mark.skipif(IS_REDHAT_6_DERIVATIVE,
                               reason="Flaky on redhat or centos 6")


class SkipIfApacheHive():
  feature_not_supported = pytest.mark.skipif(IS_APACHE_HIVE,
      reason="Apache Hive does not support this feature")
  data_connector_not_supported = pytest.mark.skipif(
      IS_APACHE_HIVE and HIVE_MAJOR_VERSION <= 3,
      reason="Apache Hive 3.1 or older version do not support DataConnector")
