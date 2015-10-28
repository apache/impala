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
# Impala tests for ALTER TABLE RECOVER PARTITIONS statement
import pytest
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *
from tests.common.skip import SkipIfS3, SkipIfLocal
from tests.util.filesystem_utils import WAREHOUSE, IS_DEFAULT_FS

# Validates ALTER TABLE RECOVER PARTITIONS statement

class TestRecoverPartitions(ImpalaTestSuite):
  TEST_DB = "recover_parts_db"
  TEST_TBL = "alter_recover_partitions"
  TEST_TBL2 = "alter_recover_partitions_all_types"
  BASE_DIR = 'test-warehouse/%s.db/%s/' % (TEST_DB, TEST_TBL)
  BASE_DIR2 = 'test-warehouse/%s.db/%s/' % (TEST_DB, TEST_TBL2)
  DEF_NULL_PART_KEY = "__HIVE_DEFAULT_PARTITION__"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRecoverPartitions, cls).add_test_dimensions()
    sync_ddl_opts = [0, 1]
    if cls.exploration_strategy() != 'exhaustive':
      # Only run with sync_ddl on exhaustive since it increases test runtime.
      sync_ddl_opts = [0]

    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB)
    self.client.execute("create database {0} location '{1}/{0}.db'".format(self.TEST_DB,
      WAREHOUSE))
    self.client.execute("use %s" % self.TEST_DB)

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB)

  @SkipIfS3.insert
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_recover_partitions(self, vector):
    """Test that RECOVER PARTITIONS correctly discovers new partitions added externally
    by the hdfs client.
    """
    part_name = "p2"
    leaf_dir = "i=0001/p=%s/" % part_name
    malformed_dir = "i=fish/p=%s/" % part_name
    file_path = "test"
    inserted_value = "2"
    null_dir = "i=1/p=%s/" % self.DEF_NULL_PART_KEY
    null_inserted_value = "4"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % (self.TEST_TBL))

    # Create a path for a new partition using hdfs client and add a file with some values.
    # Test that the partition can be recovered and that the inserted data are accessible.
    self.hdfs_client.make_dir(self.BASE_DIR + leaf_dir)
    self.hdfs_client.create_file(self.BASE_DIR + leaf_dir + file_path, inserted_value)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert self.has_value(part_name, result.data) == False
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert (self.has_value(part_name, result.data) == True,
        "ALTER TABLE %s RECOVER PARTITIONS failed." % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert (self.has_value(inserted_value, result.data) == True,
        "Failed to load tables after ALTER TABLE %s RECOVER PARTITIONS."
        % (self.TEST_TBL))

    # Test that invalid partition values are ignored during partition recovery.
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    old_length = len(result.data)
    self.hdfs_client.make_dir(self.BASE_DIR + malformed_dir)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert (len(result.data) == old_length,
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle invalid partition values."
        % (self.TEST_TBL))

    # Create a directory whose subdirectory names contain __HIVE_DEFAULT_PARTITION__
    # and check that is recovered as a NULL partition.
    self.hdfs_client.make_dir(self.BASE_DIR + null_dir)
    self.hdfs_client.create_file(self.BASE_DIR + null_dir + file_path, null_inserted_value)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert self.has_value(self.DEF_NULL_PART_KEY, result.data) == False
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert (self.has_value("NULL", result.data) == True,
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle null partition values."
        % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert self.has_value(null_inserted_value, result.data) == True

  @SkipIfS3.insert
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_nondefault_location_partitions(self, vector):
    """If the location of data files in one partition is changed, test that data files
    in the default location will not be loaded after partition recovery."""
    file_path = "test"
    leaf_dir = "i=1/p=p3/"
    inserted_value = "4"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % (self.TEST_TBL))

    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s ADD PARTITION(i=1, p='p3')" % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s PARTITION (i=1, p='p3') SET LOCATION '%s/%s.db/tmp' "
        % (self.TEST_TBL, WAREHOUSE, self.TEST_DB))
    self.hdfs_client.delete_file_dir(self.BASE_DIR + leaf_dir, recursive=True)
    self.hdfs_client.make_dir(self.BASE_DIR + leaf_dir);
    self.hdfs_client.create_file(self.BASE_DIR + leaf_dir + file_path, inserted_value)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    # Ensure that no duplicate partitions are recovered.
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert (self.has_value(inserted_value, result.data) == False,
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle non-default partition location."
        % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p3') VALUES(4)" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert self.has_value(inserted_value, result.data) == True

  @SkipIfS3.insert
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_duplicate_partitions(self, vector):
    """Test that RECOVER PARTITIONS does not recover equivalent partitions. Two partitions
    are considered equivalent if they correspond to distinct paths but can be converted
    to the same partition key values (e.g. "i=0005/p=p2" and "i=05/p=p2")."""
    same_value_dir1 = "i=0004/p=p2/"
    same_value_dir2 = "i=000004/p=p2/"
    file_path = "test"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % (self.TEST_TBL))

    # Create a partition with path "/i=1/p=p4".
    # Create a path "/i=0001/p=p4" using hdfs client, and add a file with some values.
    # Test that no new partition will be recovered and the inserted data are not accessible.
    leaf_dir = "i=0001/p=p4/"
    inserted_value = "5"

    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s ADD PARTITION(i=1, p='p4')" % (self.TEST_TBL))
    self.hdfs_client.make_dir(self.BASE_DIR + leaf_dir);
    self.hdfs_client.create_file(self.BASE_DIR + leaf_dir + file_path, inserted_value)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert (self.has_value(inserted_value, result.data) == False,
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle duplicate partition key values."
        % (self.TEST_TBL))

    # Create two paths '/i=0004/p=p2/' and "i=000004/p=p2/" using hdfs client.
    # Test that only one partition will be added.
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    old_length = len(result.data)
    self.hdfs_client.make_dir(self.BASE_DIR + same_value_dir1)
    self.hdfs_client.make_dir(self.BASE_DIR + same_value_dir2)
    # Only one partition will be added.
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL))
    assert ((old_length + 1) == len(result.data),
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle duplicate partition key values."
        % (self.TEST_TBL))

  @SkipIfS3.insert
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_post_invalidate(self, vector):
    """Test that RECOVER PARTITIONS works correctly after invalidate."""
    leaf_dir = "i=002/p=p2/"
    file_path = "test"
    inserted_value = "2"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % (self.TEST_TBL))

    # Test that the recovered partitions are properly stored in Hive MetaStore.
    # Invalidate the table metadata and then check if the recovered partitions
    # are accessible.
    self.hdfs_client.make_dir(self.BASE_DIR + leaf_dir);
    self.hdfs_client.create_file(self.BASE_DIR + leaf_dir + file_path, inserted_value)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert self.has_value(inserted_value, result.data) == True
    self.client.execute("INVALIDATE METADATA %s" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert (self.has_value(inserted_value, result.data) == True,
        "INVALIDATE can't work on partitions recovered by ALTER TABLE %s RECOVER PARTITIONS."
        % (self.TEST_TBL))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=002, p='p2') VALUES(4)" % (self.TEST_TBL))
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % self.TEST_TBL)
    assert self.has_value('4', result.data) == True

  @SkipIfS3.insert
  @SkipIfLocal.hdfs_client
  @pytest.mark.execute_serially
  def test_support_all_types(self, vector):
    """Test that RECOVER PARTITIONS works correctly on all supported data types."""
    normal_values = ["a=1", "b=128", "c=32768", "d=2147483648", "e=11.11",
                     "f=22.22", "g=33.33", "j=tchar", "k=tvchar", "s=recover"]
    malformed_values = ["a=a", "b=b", "c=c", "d=d", "e=e", "f=f", "g=g"]
    overflow_values = ["a=128", "b=-32769", "c=-2147483649", "d=9223372036854775808",
                     "e=11.11111111111111111111111111111111111111111111111111111",
                     "f=3.40282346638528860e+39", "g=1.79769313486231570e+309"]

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (i INT) PARTITIONED BY (a TINYINT, b SMALLINT, c INT, d BIGINT,"
        " e DECIMAL(4,2), f FLOAT, g DOUBLE, j CHAR(5), k VARCHAR(6), s STRING)"
        % (self.TEST_TBL2))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(a=1, b=2, c=3, d=4, e=55.55, f=6.6, g=7.7, "
        "j=cast('j' as CHAR(5)), k=cast('k' as VARCHAR(6)), s='s') VALUES(1)"
        % (self.TEST_TBL2))

    # Test valid partition values.
    normal_dir = ""
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL2))
    old_length = len(result.data)
    normal_dir = '/'.join(normal_values)
    self.hdfs_client.make_dir(self.BASE_DIR2 + normal_dir)
    # One partition will be added.
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL2))
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL2))
    assert (len(result.data) == (old_length + 1),
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle some data types."
        % (self.TEST_TBL))

    # Test malformed partition values.
    self.check_invalid_partition_values(normal_values, malformed_values)
    # Test overflow partition values.
    self.check_invalid_partition_values(normal_values, overflow_values)

  @SkipIfLocal.hdfs_client
  def check_invalid_partition_values(self, normal_values, invalid_values):
    """"Check that RECOVER PARTITIONS ignores partitions with invalid partition values."""
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % (self.TEST_TBL2))
    old_length = len(result.data)

    for i in range(len(invalid_values)):
      invalid_dir = ""
      for j in range(len(normal_values)):
        if i != j:
          invalid_dir += (normal_values[j] + "/")
        else:
          invalid_dir += (invalid_values[j] + "/")
      self.hdfs_client.make_dir(self.BASE_DIR2 + invalid_dir)
      # No partition will be added.
      self.execute_query_expect_success(self.client,
          "ALTER TABLE %s RECOVER PARTITIONS" % (self.TEST_TBL2))
      result = self.execute_query_expect_success(self.client,
          "SHOW PARTITIONS %s" % (self.TEST_TBL2))
      assert (len(result.data) == old_length,
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle invalid partition key values."
        % (self.TEST_TBL))

  def has_value(self, value, lines):
    """Check if lines contain value."""
    return any([line.find(value) != -1 for line in lines])

