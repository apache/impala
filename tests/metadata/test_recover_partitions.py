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
# Impala tests for ALTER TABLE RECOVER PARTITIONS statement

from __future__ import absolute_import, division, print_function
from builtins import range
import os
import shutil
from six.moves import urllib
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal, SkipIfFS, SkipIfCatalogV2
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.filesystem_utils import WAREHOUSE, IS_S3

from tests.common.test_dimensions import create_uncompressed_text_dimension

# Validates ALTER TABLE RECOVER PARTITIONS statement

class TestRecoverPartitions(ImpalaTestSuite):
  DEF_NULL_PART_KEY = "__HIVE_DEFAULT_PARTITION__"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRecoverPartitions, cls).add_test_dimensions()
    sync_ddl_opts = [0, 1]
    if cls.exploration_strategy() != 'exhaustive':
      # Only run without sync_ddl on exhaustive since it increases test runtime.
      sync_ddl_opts = [0]

    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=sync_ddl_opts))

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def __get_fs_location(self, db_name, table_name):
    return '%s/%s.db/%s/' % (WAREHOUSE, db_name, table_name)

  @SkipIfLocal.hdfs_client
  def test_recover_partitions(self, vector, unique_database):
    """Test that RECOVER PARTITIONS correctly discovers new partitions added externally
    by the hdfs client.
    """
    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)
    PART_NAME = "p2"
    LEAF_DIR = "i=0001/p=%s/" % PART_NAME
    MALFORMED_DIR = "i=fish/p=%s/" % PART_NAME
    FILE_PATH = "test"
    INSERTED_VALUE = "2"
    NULL_DIR = "i=1/p=%s/" % self.DEF_NULL_PART_KEY
    NULL_INSERTED_VALUE = "4"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % (FQ_TBL_NAME))
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % (FQ_TBL_NAME))

    # Create a path for a new partition using hdfs client and add a file with some values.
    # Test that the partition can be recovered and that the inserted data are accessible.
    self.create_fs_partition(TBL_LOCATION, LEAF_DIR, FILE_PATH, INSERTED_VALUE)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert not self.has_value(PART_NAME, result.data)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert self.has_value(PART_NAME, result.data),\
        "ALTER TABLE %s RECOVER PARTITIONS failed." % FQ_TBL_NAME
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value(INSERTED_VALUE, result.data),\
        "Failed to load tables after ALTER TABLE %s RECOVER PARTITIONS."\
        % FQ_TBL_NAME

    # Test that invalid partition values are ignored during partition recovery.
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    old_length = len(result.data)
    self.create_fs_partition(TBL_LOCATION, MALFORMED_DIR, FILE_PATH, INSERTED_VALUE)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert len(result.data) == old_length,\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle invalid partition values."\
        % FQ_TBL_NAME

    # Create a directory whose subdirectory names contain __HIVE_DEFAULT_PARTITION__
    # and check that is recovered as a NULL partition.
    self.create_fs_partition(TBL_LOCATION, NULL_DIR, FILE_PATH, NULL_INSERTED_VALUE)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert not self.has_value(self.DEF_NULL_PART_KEY, result.data)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert self.has_value("NULL", result.data),\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle null partition values."\
        % FQ_TBL_NAME
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value(NULL_INSERTED_VALUE, result.data)

  @SkipIfLocal.hdfs_client
  def test_nondefault_location_partitions(self, vector, unique_database):
    """If the location of data files in one partition is changed, test that data files
    in the default location will not be loaded after partition recovery."""
    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)
    FILE_PATH = "test"
    LEAF_DIR = "i=1/p=p3/"
    INSERTED_VALUE = "4"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % FQ_TBL_NAME)
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % FQ_TBL_NAME)

    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s ADD PARTITION(i=1, p='p3')" % FQ_TBL_NAME)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s PARTITION (i=1, p='p3') SET LOCATION '%s/%s.db/tmp' "
        % (FQ_TBL_NAME, WAREHOUSE, unique_database))
    self.filesystem_client.delete_file_dir(TBL_LOCATION + LEAF_DIR, recursive=True)
    self.create_fs_partition(TBL_LOCATION, LEAF_DIR, FILE_PATH, INSERTED_VALUE)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    # Ensure that no duplicate partitions are recovered.
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert not self.has_value(INSERTED_VALUE, result.data),\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle "\
        "non-default partition location." % FQ_TBL_NAME
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p3') VALUES(4)" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value(INSERTED_VALUE, result.data)

  @SkipIfLocal.hdfs_client
  def test_recover_many_partitions(self, vector, unique_database):
    """Test that RECOVER PARTITIONS correctly discovers new partitions added externally
    by the hdfs client, recovered in batches"""

    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    DB_LOCATION = '%s/%s.db/' % (WAREHOUSE, unique_database)

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (s string)" % (FQ_TBL_NAME))

    # Create 700 partitions externally
    try:
      SRC_DIR = os.path.join("/tmp", unique_database, TBL_NAME)
      if os.path.exists(SRC_DIR):
          shutil.rmtree(SRC_DIR)
      os.makedirs(SRC_DIR)
      for i in range(1, 700):
        partition_dir = os.path.join(SRC_DIR, "s=part%d/" % i)
        os.makedirs(partition_dir)
        with open(os.path.join(partition_dir, "test"), 'w') as f:
          f.write("666")
      self.filesystem_client.copy_from_local(SRC_DIR, DB_LOCATION)
    finally:
      shutil.rmtree(SRC_DIR)

    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    for i in range(1, 700):
        PART_DIR = "part%d\t" % i
        assert not self.has_value(PART_DIR, result.data)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    for i in range(1, 700):
        PART_DIR = "part%d\t" % i
        assert self.has_value(PART_DIR, result.data)

  @SkipIfLocal.hdfs_client
  def test_duplicate_partitions(self, vector, unique_database):
    """Test that RECOVER PARTITIONS does not recover equivalent partitions. Two partitions
    are considered equivalent if they correspond to distinct paths but can be converted
    to the same partition key values (e.g. "i=0005/p=p2" and "i=05/p=p2")."""
    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)
    SAME_VALUE_DIR1 = "i=0004/p=p2/"
    SAME_VALUE_DIR2 = "i=000004/p=p2/"
    FILE_PATH = "test"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % FQ_TBL_NAME)
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % FQ_TBL_NAME)

    # Create a partition with path "/i=1/p=p4".
    # Create a path "/i=0001/p=p4" using hdfs client, and add a file with some values.
    # Test that no new partition will be recovered and the inserted data are not
    # accessible.
    LEAF_DIR = "i=0001/p=p4/"
    INSERTED_VALUE = "5"

    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s ADD PARTITION(i=1, p='p4')" % FQ_TBL_NAME)
    self.create_fs_partition(TBL_LOCATION, LEAF_DIR, FILE_PATH, INSERTED_VALUE)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert not self.has_value(INSERTED_VALUE, result.data),\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle "\
        "duplicate partition key values." % FQ_TBL_NAME

    # Create two paths '/i=0004/p=p2/' and "i=000004/p=p2/" using hdfs client.
    # Test that only one partition will be added.
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    old_length = len(result.data)
    self.create_fs_partition(TBL_LOCATION, SAME_VALUE_DIR1, FILE_PATH, INSERTED_VALUE)
    self.create_fs_partition(TBL_LOCATION, SAME_VALUE_DIR2, FILE_PATH, INSERTED_VALUE)
    # Only one partition will be added.
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert old_length + 1 == len(result.data),\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle "\
        "duplicate partition key values." % FQ_TBL_NAME

  @SkipIfLocal.hdfs_client
  @SkipIfCatalogV2.impala_8489()
  def test_post_invalidate(self, vector, unique_database):
    """Test that RECOVER PARTITIONS works correctly after invalidate."""
    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)
    LEAF_DIR = "i=002/p=p2/"
    FILE_PATH = "test"
    INSERTED_VALUE = "2"

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, p string)" % FQ_TBL_NAME)
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=1, p='p1') VALUES(1)" % FQ_TBL_NAME)

    # Test that the recovered partitions are properly stored in Hive MetaStore.
    # Invalidate the table metadata and then check if the recovered partitions
    # are accessible.
    self.create_fs_partition(TBL_LOCATION, LEAF_DIR, FILE_PATH, INSERTED_VALUE)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value(INSERTED_VALUE, result.data)
    self.client.execute("INVALIDATE METADATA %s" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value(INSERTED_VALUE, result.data),\
        "INVALIDATE can't work on partitions recovered by "\
        "ALTER TABLE %s RECOVER PARTITIONS." % FQ_TBL_NAME
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(i=002, p='p2') VALUES(4)" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "select c from %s" % FQ_TBL_NAME)
    assert self.has_value('4', result.data)

  @SkipIfLocal.hdfs_client
  def test_support_all_types(self, vector, unique_database):
    """Test that RECOVER PARTITIONS works correctly on all supported data types."""
    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)

    normal_values = ["a=1", "b=128", "c=32768", "d=2147483648", "e=11.11",
                     "f=22.22", "g=33.33", "j=tchar", "k=tvchar", "s=recover"]
    malformed_values = ["a=a", "b=b", "c=c", "d=d", "e=e", "f=f", "g=g"]
    overflow_values = ["a=128", "b=-32769", "c=-2147483649", "d=9223372036854775808",
                     "e=11.11111111111111111111111111111111111111111111111111111",
                     "f=3.40282346638528860e+39", "g=1.79769313486231570e+309"]

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (i INT) PARTITIONED BY (a TINYINT, b SMALLINT, c INT, d BIGINT,"
        " e DECIMAL(4,2), f FLOAT, g DOUBLE, j CHAR(5), k VARCHAR(6), s STRING)"
        % FQ_TBL_NAME)
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s PARTITION(a=1, b=2, c=3, d=4, e=55.55, f=6.6, g=7.7, "
        "j=cast('j' as CHAR(5)), k=cast('k' as VARCHAR(6)), s='s') VALUES(1)"
        % FQ_TBL_NAME)

    # Test valid partition values.
    normal_dir = ""
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    old_length = len(result.data)
    normal_dir = '/'.join(normal_values)
    self.create_fs_partition(TBL_LOCATION, normal_dir, "test", "5")
    # One partition will be added.
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert len(result.data) == (old_length + 1),\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle some data types."\
        % FQ_TBL_NAME

    # Test malformed partition values.
    self.check_invalid_partition_values(FQ_TBL_NAME, TBL_LOCATION,
        normal_values, malformed_values)
    # Test overflow partition values.
    self.check_invalid_partition_values(FQ_TBL_NAME, TBL_LOCATION,
        normal_values, overflow_values)

  @SkipIfLocal.hdfs_client
  def test_encoded_partition(self, vector, unique_database):
    """IMPALA-6619: Test that RECOVER PARTITIONS does not create unnecessary partitions
       when dealing with URL encoded partition value."""
    TBL_NAME = "test_encoded_partition"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME

    self.execute_query_expect_success(
      self.client, "CREATE TABLE %s (s string) PARTITIONED BY (p string)" % FQ_TBL_NAME)
    self.execute_query_expect_success(
      self.client, "ALTER TABLE %s ADD PARTITION (p='100%%')" % FQ_TBL_NAME)

    # Running ALTER TABLE RECOVER PARTITIONS multiple times should only produce
    # a single partition when adding a single partition.
    for i in range(3):
      self.execute_query_expect_success(
        self.client, "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
      result = self.execute_query_expect_success(
        self.client, "SHOW PARTITIONS %s" % FQ_TBL_NAME)
      assert self.count_partition(result.data) == 1, \
        "ALTER TABLE %s RECOVER PARTITIONS produced more than 1 partitions" % FQ_TBL_NAME
      assert self.count_value('p=100%25', result.data) == 1, \
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle encoded partitioned value" % \
        FQ_TBL_NAME

  @SkipIfLocal.hdfs_client
  def test_unescaped_string_partition(self, vector, unique_database):
    """IMPALA-7784: Test that RECOVER PARTITIONS correctly parses unescaped string
       values"""
    tbl_name = "test_unescaped_string_partition"
    fq_tbl_name = unique_database + "." + tbl_name
    tbl_location = self.__get_fs_location(unique_database, tbl_name)

    self.execute_query_expect_success(
        self.client, "CREATE TABLE %s (i int) PARTITIONED BY (p string)" % fq_tbl_name)
    parts = ["\'", "\"", "\\\'", "\\\"", "\\\\\'", "\\\\\""]
    for i in range(len(parts)):
      # When creating partition directories, Hive replaces special characters in
      # partition value string using the %xx escape. e.g. p=' will become p=%27.
      hex_part = urllib.parse.quote(parts[i])
      self.create_fs_partition(tbl_location, "p=%s" % hex_part, "file_%d" % i, str(i))

    self.execute_query_expect_success(
        self.client, "ALTER TABLE %s RECOVER PARTITIONS" % fq_tbl_name)
    result = self.execute_query_expect_success(
        self.client, "SHOW PARTITIONS %s" % fq_tbl_name)
    self.verify_partitions(parts, result.data)

  @SkipIfLocal.hdfs_client
  @SkipIfFS.empty_directory
  def test_empty_directory(self, vector, unique_database):
    """Explicitly test how empty directories are handled when partitions are recovered."""

    TBL_NAME = "test_recover_partitions"
    FQ_TBL_NAME = unique_database + "." + TBL_NAME
    TBL_LOCATION = self.__get_fs_location(unique_database, TBL_NAME)

    self.execute_query_expect_success(self.client,
        "CREATE TABLE %s (c int) PARTITIONED BY (i int, s string)" % (FQ_TBL_NAME))

    # Adds partition directories.
    num_partitions = 10
    for i in range(1, num_partitions):
        PART_DIR = "i=%d/s=part%d" % (i,i)
        self.filesystem_client.make_dir(TBL_LOCATION + PART_DIR)

    # Adds a duplicate directory name.
    self.filesystem_client.make_dir(TBL_LOCATION + "i=001/s=part1")

    # Adds a malformed directory name.
    self.filesystem_client.make_dir(TBL_LOCATION + "i=wrong_type/s=part1")

    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert 0 == self.count_partition(result.data)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s RECOVER PARTITIONS" % FQ_TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % FQ_TBL_NAME)
    assert num_partitions - 1 == self.count_partition(result.data)
    for i in range(1, num_partitions):
        PART_DIR = "part%d\t" % i
        assert self.has_value(PART_DIR, result.data)

  def create_fs_partition(self, root_path, new_dir, new_file, value):
    """Creates an fs directory and writes a file to it. Empty directories are no-op's
       for S3, so enforcing a non-empty directory is less error prone across
       filesystems."""
    partition_dir = os.path.join(root_path, new_dir)
    self.filesystem_client.make_dir(partition_dir);
    self.filesystem_client.create_file(os.path.join(partition_dir, new_file),
                                       value)

  def check_invalid_partition_values(self, fq_tbl_name, tbl_location,
    normal_values, invalid_values):
    """"Check that RECOVER PARTITIONS ignores partitions with invalid partition values."""
    result = self.execute_query_expect_success(self.client,
        "SHOW PARTITIONS %s" % fq_tbl_name)
    old_length = len(result.data)

    for i in range(len(invalid_values)):
      invalid_dir = ""
      for j in range(len(normal_values)):
        if i != j:
          invalid_dir += (normal_values[j] + "/")
        else:
          invalid_dir += (invalid_values[j] + "/")
      self.filesystem_client.make_dir(tbl_location + invalid_dir)
      # No partition will be added.
      self.execute_query_expect_success(self.client,
          "ALTER TABLE %s RECOVER PARTITIONS" % fq_tbl_name)
      result = self.execute_query_expect_success(self.client,
          "SHOW PARTITIONS %s" % fq_tbl_name)
      assert len(result.data) == old_length,\
        "ALTER TABLE %s RECOVER PARTITIONS failed to handle "\
        "invalid partition key values." % fq_tbl_name

  def count_partition(self, lines):
    """Count the number of partitions in the lines."""
    return self.count_value(WAREHOUSE, lines)

  def count_value(self, value, lines):
    """Count the number of lines that contain value."""
    return len([line for line in lines if line.find(value) != -1])

  def verify_partitions(self, expected_parts, lines):
    """Check if all partition values are expected"""
    values = [line.split('\t')[0] for line in lines]
    assert len(values) == len(expected_parts) + 1
    for p in expected_parts:
      assert p in values
