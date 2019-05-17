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

import getpass
import itertools
import pytest
import re
import time

from test_ddl_base import TestDdlBase
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import LOG
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import (SkipIf, SkipIfABFS, SkipIfADLS, SkipIfKudu, SkipIfLocal,
                               SkipIfCatalogV2)
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import (
    WAREHOUSE,
    IS_HDFS,
    IS_S3,
    IS_ADLS,
    FILESYSTEM_NAME)
from tests.common.impala_cluster import ImpalaCluster

# Validates DDL statements (create, drop)
class TestDdlStatements(TestDdlBase):
  @SkipIfLocal.hdfs_client
  @SkipIfABFS.trash
  def test_drop_table_with_purge(self, unique_database):
    """This test checks if the table data is permanently deleted in
    DROP TABLE <tbl> PURGE queries"""
    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    self.client.execute("create table {0}.t2(i int)".format(unique_database))
    # Create sample test data files under the table directories
    self.filesystem_client.create_file("test-warehouse/{0}.db/t1/t1.txt".\
        format(unique_database), file_data='t1')
    self.filesystem_client.create_file("test-warehouse/{0}.db/t2/t2.txt".\
        format(unique_database), file_data='t2')
    # Drop the table (without purge) and make sure it exists in trash
    self.client.execute("drop table {0}.t1".format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/t1.txt".\
        format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/".\
        format(unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/t1.txt".\
        format(getpass.getuser(), unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1".\
        format(getpass.getuser(), unique_database))
    # Drop the table (with purge) and make sure it doesn't exist in trash
    self.client.execute("drop table {0}.t2 purge".format(unique_database))
    if not IS_S3 and not IS_ADLS:
      # In S3, deletes are eventual. So even though we dropped the table, the files
      # belonging to this table may still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      # The ADLS Python client is not strongly consistent, so these files may still be
      # visible after a DROP. (Remove after IMPALA-5335 is resolved)
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/".\
          format(unique_database))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t2/t2.txt".\
          format(unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2/t2.txt".\
        format(getpass.getuser(), unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t2".\
        format(getpass.getuser(), unique_database))
    # Create an external table t3 and run the same test as above. Make
    # sure the data is not deleted
    self.filesystem_client.make_dir(
        "test-warehouse/{0}.db/data_t3/".format(unique_database), permission=777)
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/data_t3/data.txt".format(unique_database), file_data='100')
    self.client.execute("create external table {0}.t3(i int) stored as "
      "textfile location \'/test-warehouse/{0}.db/data_t3\'" .format(unique_database))
    self.client.execute("drop table {0}.t3 purge".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/data_t3/data.txt".format(unique_database))
    self.filesystem_client.delete_file_dir(
        "test-warehouse/{0}.db/data_t3".format(unique_database), recursive=True)

  @SkipIfADLS.eventually_consistent
  @SkipIfLocal.hdfs_client
  def test_drop_cleans_hdfs_dirs(self, unique_database):
    self.client.execute('use default')
    # Verify the db directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    # Verify the table directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))

    # Dropping the table removes the table's directory and preserves the db's directory
    self.client.execute("drop table {0}.t1".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    # Dropping the db removes the db's directory
    self.client.execute("drop database {0}".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    # Dropping the db using "cascade" removes all tables' and db's directories
    # but keeps the external tables' directory
    self._create_db(unique_database)
    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    self.client.execute("create table {0}.t2(i int)".format(unique_database))
    result = self.client.execute("create external table {0}.t3(i int) "
        "location '{1}/{0}/t3/'".format(unique_database, WAREHOUSE))
    self.client.execute("drop database {0} cascade".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}.db/t2/".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}/t3/".format(unique_database))
    self.filesystem_client.delete_file_dir(
        "test-warehouse/{0}/t3/".format(unique_database), recursive=True)
    assert not self.filesystem_client.exists(
        "test-warehouse/{0}/t3/".format(unique_database))
    # Re-create database to make unique_database teardown succeed.
    self._create_db(unique_database)

  @SkipIfADLS.eventually_consistent
  @SkipIfLocal.hdfs_client
  def test_truncate_cleans_hdfs_files(self, unique_database):
    # Verify the db directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/".format(unique_database))

    self.client.execute("create table {0}.t1(i int)".format(unique_database))
    # Verify the table directory exists
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/t1/".format(unique_database))

    try:
      # If we're testing S3, we want the staging directory to be created.
      self.client.execute("set s3_skip_insert_staging=false")
      # Should have created one file in the table's dir
      self.client.execute("insert into {0}.t1 values (1)".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t1/".format(unique_database))) == 2

      # Truncating the table removes the data files and preserves the table's directory
      self.client.execute("truncate table {0}.t1".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t1/".format(unique_database))) == 1

      self.client.execute(
          "create table {0}.t2(i int) partitioned by (p int)".format(unique_database))
      # Verify the table directory exists
      assert self.filesystem_client.exists(
          "test-warehouse/{0}.db/t2/".format(unique_database))

      # Should have created the partition dir, which should contain exactly one file
      self.client.execute(
          "insert into {0}.t2 partition(p=1) values (1)".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))) == 1

      # Truncating the table removes the data files and preserves the partition's directory
      self.client.execute("truncate table {0}.t2".format(unique_database))
      assert self.filesystem_client.exists(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))
      assert len(self.filesystem_client.ls(
          "test-warehouse/{0}.db/t2/p=1".format(unique_database))) == 0
    finally:
      # Reset to its default value.
      self.client.execute("set s3_skip_insert_staging=true")

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_truncate_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/truncate-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_database(self, vector, unique_database):
    # The unique_database provides the .test a unique database name which allows
    # us to run this test in parallel with others.
    self.run_test_case('QueryTest/create-database', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  def test_comment_on_database(self, vector, unique_database):
    comment = self._get_db_comment(unique_database)
    assert '' == comment

    self.client.execute("comment on database {0} is 'comment'".format(unique_database))
    comment = self._get_db_comment(unique_database)
    assert 'comment' == comment

    self.client.execute("comment on database {0} is ''".format(unique_database))
    comment = self._get_db_comment(unique_database)
    assert '' == comment

    self.client.execute("comment on database {0} is '\\'comment\\''".format(unique_database))
    comment = self._get_db_comment(unique_database)
    assert "\\'comment\\'" == comment

    self.client.execute("comment on database {0} is null".format(unique_database))
    comment = self._get_db_comment(unique_database)
    assert '' == comment

  def test_alter_database_set_owner(self, vector, unique_database):
    self.client.execute("alter database {0} set owner user foo_user".format(
      unique_database))
    properties = self._get_db_owner_properties(unique_database)
    assert len(properties) == 1
    assert {'foo_user': 'USER'} == properties

    self.client.execute("alter database {0} set owner role foo_role".format(
      unique_database))
    properties = self._get_db_owner_properties(unique_database)
    assert len(properties) == 1
    assert {'foo_role': 'ROLE'} == properties

  def test_alter_table_set_owner(self, vector, unique_database):
    table_name = "{0}.test_owner_tbl".format(unique_database)
    self.client.execute("create table {0}(i int)".format(table_name))
    self.client.execute("alter table {0} set owner user foo_user".format(table_name))
    owner = self._get_table_or_view_owner(table_name)
    assert ('foo_user', 'USER') == owner

    self.client.execute("alter table {0} set owner role foo_role".format(table_name))
    owner = self._get_table_or_view_owner(table_name)
    assert ('foo_role', 'ROLE') == owner

  def test_alter_view_set_owner(self, vector, unique_database):
    view_name = "{0}.test_owner_tbl".format(unique_database)
    self.client.execute("create view {0} as select 1".format(view_name))
    self.client.execute("alter view {0} set owner user foo_user".format(view_name))
    owner = self._get_table_or_view_owner(view_name)
    assert ('foo_user', 'USER') == owner

    self.client.execute("alter view {0} set owner role foo_role".format(view_name))
    owner = self._get_table_or_view_owner(view_name)
    assert ('foo_role', 'ROLE') == owner

  # There is a query in QueryTest/create-table that references nested types, which is not
  # supported if old joins and aggs are enabled. Since we do not get any meaningful
  # additional coverage by running a DDL test under the old aggs and joins, it can be
  # skipped.
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-table', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_like_file(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-like-file', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_create_table_as_select(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/create-table-as-select', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIf.kudu_not_supported
  @UniqueDatabase.parametrize(sync_ddl=True)
  @SkipIfKudu.no_hybrid_clock
  def test_create_kudu(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    vector.get_value('exec_option')['kudu_read_mode'] = "READ_AT_SNAPSHOT"
    self.run_test_case('QueryTest/kudu_create', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  def test_comment_on_table(self, vector, unique_database):
    table = '{0}.comment_table'.format(unique_database)
    self.client.execute("create table {0} (i int)".format(table))

    comment = self._get_table_or_view_comment(table)
    assert comment is None

    self.client.execute("comment on table {0} is 'comment'".format(table))
    comment = self._get_table_or_view_comment(table)
    assert "comment" == comment

    self.client.execute("comment on table {0} is ''".format(table))
    comment = self._get_table_or_view_comment(table)
    assert "" == comment

    self.client.execute("comment on table {0} is '\\'comment\\''".format(table))
    comment = self._get_table_or_view_comment(table)
    assert "\\\\'comment\\\\'" == comment

    self.client.execute("comment on table {0} is null".format(table))
    comment = self._get_table_or_view_comment(table)
    assert comment is None

  def test_comment_on_view(self, vector, unique_database):
    view = '{0}.comment_view'.format(unique_database)
    self.client.execute("create view {0} as select 1".format(view))

    comment = self._get_table_or_view_comment(view)
    assert comment is None

    self.client.execute("comment on view {0} is 'comment'".format(view))
    comment = self._get_table_or_view_comment(view)
    assert "comment" == comment

    self.client.execute("comment on view {0} is ''".format(view))
    comment = self._get_table_or_view_comment(view)
    assert "" == comment

    self.client.execute("comment on view {0} is '\\'comment\\''".format(view))
    comment = self._get_table_or_view_comment(view)
    assert "\\\\'comment\\\\'" == comment

    self.client.execute("comment on view {0} is null".format(view))
    comment = self._get_table_or_view_comment(view)
    assert comment is None

  def test_comment_on_column(self, vector, unique_database):
    table = "{0}.comment_table".format(unique_database)
    self.client.execute("create table {0} (i int) partitioned by (j int)".format(table))

    comment = self._get_column_comment(table, 'i')
    assert '' == comment

    # Updating comment on a regular column.
    self.client.execute("comment on column {0}.i is 'comment 1'".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "comment 1" == comment

    # Updating comment on a partition column.
    self.client.execute("comment on column {0}.j is 'comment 2'".format(table))
    comment = self._get_column_comment(table, 'j')
    assert "comment 2" == comment

    self.client.execute("comment on column {0}.i is ''".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    self.client.execute("comment on column {0}.i is '\\'comment\\''".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "\\'comment\\'" == comment

    self.client.execute("comment on column {0}.i is null".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    view = "{0}.comment_view".format(unique_database)
    self.client.execute("create view {0}(i) as select 1".format(view))

    comment = self._get_column_comment(view, 'i')
    assert "" == comment

    self.client.execute("comment on column {0}.i is 'comment'".format(view))
    comment = self._get_column_comment(view, 'i')
    assert "comment" == comment

    self.client.execute("comment on column {0}.i is ''".format(view))
    comment = self._get_column_comment(view, 'i')
    assert "" == comment

    self.client.execute("comment on column {0}.i is '\\'comment\\''".format(view))
    comment = self._get_column_comment(view, 'i')
    assert "\\'comment\\'" == comment

    self.client.execute("comment on column {0}.i is null".format(view))
    comment = self._get_column_comment(view, 'i')
    assert "" == comment

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_sync_ddl_drop(self, vector, unique_database):
    """Verifies the catalog gets updated properly when dropping objects with sync_ddl
    enabled"""
    self.client.set_configuration({'sync_ddl': 1})
    # Drop the database immediately after creation (within a statestore heartbeat) and
    # verify the catalog gets updated properly.
    self.client.execute("drop database {0}".format(unique_database))
    assert unique_database not in self.all_db_names()
    # Re-create database to make unique_database teardown succeed.
    self._create_db(unique_database)

  # TODO: don't use hdfs_client
  @SkipIfLocal.hdfs_client
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  def test_alter_table(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False

    # Create an unpartitioned table to get a filesystem directory that does not
    # use the (key=value) format. The directory is automatically cleanup up
    # by the unique_database fixture.
    self.client.execute("create table {0}.part_data (i int)".format(unique_database))
    assert self.filesystem_client.exists(
        "test-warehouse/{0}.db/part_data".format(unique_database))
    self.filesystem_client.create_file(
        "test-warehouse/{0}.db/part_data/data.txt".format(unique_database),
        file_data='1984')
    self.run_test_case('QueryTest/alter-table', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIf.not_hdfs
  @SkipIfLocal.hdfs_client
  @SkipIfCatalogV2.hdfs_caching_ddl_unsupported()
  @UniqueDatabase.parametrize(sync_ddl=True, num_dbs=2)
  def test_alter_table_hdfs_caching(self, vector, unique_database):
    self.run_test_case('QueryTest/alter-table-hdfs-caching', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_alter_set_column_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/alter-table-set-column-stats', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIfLocal.hdfs_client
  @SkipIfABFS.trash
  def test_drop_partition_with_purge(self, vector, unique_database):
    """Verfies whether alter <tbl> drop partition purge actually skips trash"""
    self.client.execute(
        "create table {0}.t1(i int) partitioned by (j int)".format(unique_database))
    # Add two partitions (j=1) and (j=2) to table t1
    self.client.execute("alter table {0}.t1 add partition(j=1)".format(unique_database))
    self.client.execute("alter table {0}.t1 add partition(j=2)".format(unique_database))
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=1/j1.txt".format(unique_database), file_data='j1')
    self.filesystem_client.create_file(\
        "test-warehouse/{0}.db/t1/j=2/j2.txt".format(unique_database), file_data='j2')
    # Drop the partition (j=1) without purge and make sure it exists in trash
    self.client.execute("alter table {0}.t1 drop partition(j=1)".format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1/j1.txt".\
        format(unique_database))
    assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=1".\
        format(unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1/j1.txt".\
        format(getpass.getuser(), unique_database))
    assert self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=1".\
        format(getpass.getuser(), unique_database))
    # Drop the partition (with purge) and make sure it doesn't exist in trash
    self.client.execute("alter table {0}.t1 drop partition(j=2) purge".\
        format(unique_database));
    if not IS_S3 and not IS_ADLS:
      # In S3, deletes are eventual. So even though we dropped the partition, the files
      # belonging to this partition may still be visible for some unbounded time. This
      # happens only with PURGE. A regular DROP TABLE is just a copy of files which is
      # consistent.
      # The ADLS Python client is not strongly consistent, so these files may still be
      # visible after a DROP. (Remove after IMPALA-5335 is resolved)
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2/j2.txt".\
          format(unique_database))
      assert not self.filesystem_client.exists("test-warehouse/{0}.db/t1/j=2".\
          format(unique_database))
    assert not self.filesystem_client.exists(\
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2".\
        format(getpass.getuser(), unique_database))
    assert not self.filesystem_client.exists(
        "user/{0}/.Trash/Current/test-warehouse/{1}.db/t1/j=2/j2.txt".\
        format(getpass.getuser(), unique_database))

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_views_ddl(self, vector, unique_database):
    vector.get_value('exec_option')['abort_on_error'] = False
    self.run_test_case('QueryTest/views-ddl', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @UniqueDatabase.parametrize()
  def test_view_hints(self, vector, unique_database):
    # Test that plan hints are stored in the view's comment field; this should work
    # regardless of how Hive formats the output.  Getting this to work with the
    # automated test case runner is rather difficult, so verify directly.  There
    # should be two # of each join hint, one for the original text, one for the expanded
    self.client.execute("""
        create view {0}.hints_test as
        select /* +straight_join */ a.* from functional.alltypestiny a
        inner join /* +broadcast */ functional.alltypes b on a.id = b.id
        inner join /* +shuffle */ functional.alltypessmall c on b.id = c.id
        """.format(unique_database))
    results = self.execute_query("describe formatted %s.hints_test" % unique_database)
    sj, bc, shuf = 0,0,0
    for row in results.data:
        sj += '-- +straight_join' in row
        bc += '-- +broadcast' in row
        shuf += '-- +shuffle' in row
    assert sj == 2
    assert bc == 2
    assert shuf == 2

    # Test querying the hinted view.
    results = self.execute_query("select count(*) from %s.hints_test" % unique_database)
    assert results.success
    assert len(results.data) == 1
    assert results.data[0] == '8'

    # Test the plan to make sure hints were applied correctly
    plan = self.execute_query("explain select * from %s.hints_test" % unique_database,
        query_options={'explain_level':0})
    plan_match = """PLAN-ROOT SINK
08:EXCHANGE [UNPARTITIONED]
04:HASH JOIN [INNER JOIN, PARTITIONED]
|--07:EXCHANGE [HASH(c.id)]
|  02:SCAN {filesystem_name} [functional.alltypessmall c]
06:EXCHANGE [HASH(b.id)]
03:HASH JOIN [INNER JOIN, BROADCAST]
|--05:EXCHANGE [BROADCAST]
|  01:SCAN {filesystem_name} [functional.alltypes b]
00:SCAN {filesystem_name} [functional.alltypestiny a]"""
    assert plan_match.format(filesystem_name=FILESYSTEM_NAME) in '\n'.join(plan.data)

  def _verify_describe_view(self, vector, view_name, expected_substr):
    """
    Verify across all impalads that the view 'view_name' has the given substring in its
    expanded SQL.

    If SYNC_DDL is enabled, the verification should complete immediately. Otherwise,
    loops waiting for the expected condition to pass.
    """
    if vector.get_value('exec_option')['sync_ddl']:
      num_attempts = 1
    else:
      num_attempts = 60
    for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
      client = impalad.service.create_beeswax_client()
      try:
        for attempt in itertools.count(1):
          assert attempt <= num_attempts, "ran out of attempts"
          try:
            result = self.execute_query_expect_success(
                client, "describe formatted %s" % view_name)
            exp_line = [l for l in result.data if 'View Expanded' in l][0]
          except ImpalaBeeswaxException, e:
            # In non-SYNC_DDL tests, it's OK to get a "missing view" type error
            # until the metadata propagates.
            exp_line = "Exception: %s" % e
          if expected_substr in exp_line.lower():
            return
          time.sleep(1)
      finally:
        client.close()


  def test_views_describe(self, vector, unique_database):
    # IMPALA-6896: Tests that altered views can be described by all impalads.
    impala_cluster = ImpalaCluster.get_e2e_test_cluster()
    impalads = impala_cluster.impalads
    view_name = "%s.test_describe_view" % unique_database
    query_opts = vector.get_value('exec_option')
    first_client = impalads[0].service.create_beeswax_client()
    try:
      # Create a view and verify it's visible.
      self.execute_query_expect_success(first_client,
                                        "create view {0} as "
                                        "select * from functional.alltypes"
                                        .format(view_name), query_opts)
      self._verify_describe_view(vector, view_name, "select * from functional.alltypes")

      # Alter the view and verify the alter is visible.
      self.execute_query_expect_success(first_client,
                                        "alter view {0} as "
                                        "select * from functional.alltypesagg"
                                        .format(view_name), query_opts)
      self._verify_describe_view(vector, view_name,
                                 "select * from functional.alltypesagg")
    finally:
      first_client.close()


  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_functions_ddl(self, vector, unique_database):
    self.run_test_case('QueryTest/functions-ddl', vector, use_db=unique_database,
        multiple_impalad=self._use_multiple_impalad(vector))

  @SkipIfLocal.hdfs_client
  def test_create_alter_bulk_partition(self, vector, unique_database):
    # Change the scale depending on the exploration strategy, with 50 partitions this
    # test runs a few minutes, with 10 partitions it takes ~50s for two configurations.
    num_parts = 50 if self.exploration_strategy() == 'exhaustive' else 10
    fq_tbl_name = unique_database + ".part_test_tbl"
    self.client.execute("create table {0}(i int) partitioned by(j int, s string) "
         "location '{1}/{0}'".format(fq_tbl_name, WAREHOUSE))

    # Add some partitions (first batch of two)
    for i in xrange(num_parts / 5):
      start = time.time()
      self.client.execute(
          "alter table {0} add partition(j={1}, s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Modify one of the partitions
    self.client.execute("alter table {0} partition(j=1, s='1')"
        " set fileformat parquetfile".format(fq_tbl_name))

    # Alter one partition to a non-existent location twice (IMPALA-741)
    self.filesystem_client.delete_file_dir("tmp/dont_exist1/", recursive=True)
    self.filesystem_client.delete_file_dir("tmp/dont_exist2/", recursive=True)

    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist1'"
        .format(fq_tbl_name, WAREHOUSE))
    self.execute_query_expect_success(self.client,
        "alter table {0} partition(j=1,s='1') set location '{1}/tmp/dont_exist2'"
        .format(fq_tbl_name, WAREHOUSE))

    # Add some more partitions
    for i in xrange(num_parts / 5, num_parts):
      start = time.time()
      self.client.execute(
          "alter table {0} add partition(j={1},s='{1}')".format(fq_tbl_name, i))
      LOG.info('ADD PARTITION #%d exec time: %s' % (i, time.time() - start))

    # Insert data and verify it shows up.
    self.client.execute(
        "insert into table {0} partition(j=1, s='1') select 1".format(fq_tbl_name))
    assert '1' == self.execute_scalar("select count(*) from {0}".format(fq_tbl_name))

  def test_alter_table_create_many_partitions(self, vector, unique_database):
    """
    Checks that creating more partitions than the MAX_PARTITION_UPDATES_PER_RPC
    batch size works, in that it creates all the underlying partitions.
    """
    self.client.execute(
        "create table {0}.t(i int) partitioned by (p int)".format(unique_database))
    MAX_PARTITION_UPDATES_PER_RPC = 500
    alter_stmt = "alter table {0}.t add ".format(unique_database) + " ".join(
        "partition(p=%d)" % (i,) for i in xrange(MAX_PARTITION_UPDATES_PER_RPC + 2))
    self.client.execute(alter_stmt)
    partitions = self.client.execute("show partitions {0}.t".format(unique_database))
    # Show partitions will contain partition HDFS paths, which we expect to contain
    # "p=val" subdirectories for each partition. The regexp finds all the "p=[0-9]*"
    # paths, converts them to integers, and checks that wehave all the ones we
    # expect.
    PARTITION_RE = re.compile("p=([0-9]+)")
    assert map(int, PARTITION_RE.findall(str(partitions))) == \
        range(MAX_PARTITION_UPDATES_PER_RPC + 2)

  def test_create_alter_tbl_properties(self, vector, unique_database):
    fq_tbl_name = unique_database + ".test_alter_tbl"

    # Specify TBLPROPERTIES and SERDEPROPERTIES at CREATE time
    self.client.execute("""create table {0} (i int)
    with serdeproperties ('s1'='s2', 's3'='s4')
    tblproperties ('p1'='v0', 'p1'='v1')""".format(fq_tbl_name))
    properties = self._get_tbl_properties(fq_tbl_name)

    assert len(properties) == 2
    # The transient_lastDdlTime is variable, so don't verify the value.
    assert 'transient_lastDdlTime' in properties
    del properties['transient_lastDdlTime']
    assert {'p1': 'v1'} == properties

    properties = self._get_serde_properties(fq_tbl_name)
    assert {'s1': 's2', 's3': 's4'} == properties

    # Modify the SERDEPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table {0} set serdeproperties "
        "('s1'='new', 's5'='s6')".format(fq_tbl_name))
    properties = self._get_serde_properties(fq_tbl_name)
    assert {'s1': 'new', 's3': 's4', 's5': 's6'} == properties

    # Modify the TBLPROPERTIES using ALTER TABLE SET.
    self.client.execute("alter table {0} set tblproperties "
        "('prop1'='val1', 'p2'='val2', 'p2'='val3', ''='')".format(fq_tbl_name))
    properties = self._get_tbl_properties(fq_tbl_name)

    assert 'transient_lastDdlTime' in properties
    assert properties['p1'] == 'v1'
    assert properties['prop1'] == 'val1'
    assert properties['p2'] == 'val3'
    assert properties[''] == ''

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_partition_ddl_predicates(self, vector, unique_database):
    self.run_test_case('QueryTest/partition-ddl-predicates-all-fs', vector,
        use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))
    if IS_HDFS:
      self.run_test_case('QueryTest/partition-ddl-predicates-hdfs-only', vector,
          use_db=unique_database, multiple_impalad=self._use_multiple_impalad(vector))

  def test_create_table_file_format(self, vector, unique_database):
    # When default_file_format query option is not specified, the default table file
    # format is TEXT.
    text_table = "{0}.text_tbl".format(unique_database)
    self.execute_query_expect_success(
        self.client, "create table {0}(i int)".format(text_table))
    result = self.execute_query_expect_success(
        self.client, "show create table {0}".format(text_table))
    assert any("TEXTFILE" in x for x in result.data)

    self.execute_query_expect_failure(
        self.client, "create table {0}.foobar_tbl".format(unique_database),
        {"default_file_format": "foobar"})

    parquet_table = "{0}.parquet_tbl".format(unique_database)
    self.execute_query_expect_success(
        self.client, "create table {0}(i int)".format(parquet_table),
        {"default_file_format": "parquet"})
    result = self.execute_query_expect_success(
        self.client, "show create table {0}".format(parquet_table))
    assert any("PARQUET" in x for x in result.data)

    # The table created should still be ORC even though the default_file_format query
    # option is set to parquet.
    orc_table = "{0}.orc_tbl".format(unique_database)
    self.execute_query_expect_success(
        self.client,
        "create table {0}(i int) stored as orc".format(orc_table),
        {"default_file_format": "parquet"})
    result = self.execute_query_expect_success(
      self.client, "show create table {0}".format(orc_table))
    assert any("ORC" in x for x in result.data)

  def test_kudu_column_comment(self, vector, unique_database):
    table = "{0}.kudu_table0".format(unique_database)
    self.client.execute("create table {0}(x int comment 'x' primary key) \
                        stored as kudu".format(table))
    comment = self._get_column_comment(table, 'x')
    assert "x" == comment

    table = "{0}.kudu_table".format(unique_database)
    self.client.execute("create table {0}(i int primary key) stored as kudu"
                        .format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    self.client.execute("comment on column {0}.i is 'comment1'".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "comment1" == comment

    self.client.execute("comment on column {0}.i is ''".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    self.client.execute("comment on column {0}.i is 'comment2'".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "comment2" == comment

    self.client.execute("comment on column {0}.i is null".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    self.client.execute("alter table {0} alter column i set comment 'comment3'"
                        .format(table))
    comment = self._get_column_comment(table, 'i')
    assert "comment3" == comment

    self.client.execute("alter table {0} alter column i set comment ''".format(table))
    comment = self._get_column_comment(table, 'i')
    assert "" == comment

    self.client.execute("alter table {0} add columns (j int comment 'comment4')"
                        .format(table))
    comment = self._get_column_comment(table, 'j')
    assert "comment4" == comment

# IMPALA-2002: Tests repeated adding/dropping of .jar and .so in the lib cache.
class TestLibCache(TestDdlBase):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLibCache, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  def test_create_drop_function(self, vector, unique_database):
    """This will create, run, and drop the same function repeatedly, exercising the
    lib cache mechanism.
    """
    create_fn_stmt = ("create function {0}.f() returns int "
                      "location '{1}/libTestUdfs.so' symbol='NoArgs'"
                      .format(unique_database, WAREHOUSE))
    select_stmt = ("select {0}.f() from functional.alltypes limit 10"
                   .format(unique_database))
    drop_fn_stmt = "drop function %s {0}.f()".format(unique_database)
    self.create_drop_ddl(vector, [create_fn_stmt], [drop_fn_stmt], select_stmt)

  # Run serially because this test inspects global impalad metrics.
  # TODO: The metrics checks could be relaxed to enable running this test in
  # parallel, but that might need a more general wait_for_metric_value().
  @SkipIfCatalogV2.data_sources_unsupported()
  @pytest.mark.execute_serially
  def test_create_drop_data_src(self, vector, unique_database):
    """This will create, run, and drop the same data source repeatedly, exercising
    the lib cache mechanism.
    """
    data_src_name = unique_database + "_datasrc"
    create_ds_stmt = ("CREATE DATA SOURCE {0} "
        "LOCATION '{1}/data-sources/test-data-source.jar' "
        "CLASS 'org.apache.impala.extdatasource.AllTypesDataSource' "
        "API_VERSION 'V1'".format(data_src_name, WAREHOUSE))
    create_tbl_stmt = ("CREATE TABLE {0}.data_src_tbl (x int) "
        "PRODUCED BY DATA SOURCE {1}('dummy_init_string')")\
        .format(unique_database, data_src_name)
    drop_ds_stmt = "drop data source %s {0}".format(data_src_name)
    drop_tbl_stmt = "drop table %s {0}.data_src_tbl".format(unique_database)
    select_stmt = "select * from {0}.data_src_tbl limit 1".format(unique_database)
    class_cache_hits_metric = "external-data-source.class-cache.hits"
    class_cache_misses_metric = "external-data-source.class-cache.misses"

    create_stmts = [create_ds_stmt, create_tbl_stmt]
    drop_stmts = [drop_tbl_stmt, drop_ds_stmt]

    # The ImpaladService is used to capture metrics
    service = self.impalad_test_service

    # Initial metric values
    class_cache_hits = service.get_metric_value(class_cache_hits_metric)
    class_cache_misses = service.get_metric_value(class_cache_misses_metric)
    # Test with 1 node so we can check the metrics on only the coordinator
    vector.get_value('exec_option')['num_nodes'] = 1
    num_iterations = 2
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, num_iterations)

    # Check class cache metrics. Shouldn't have any new cache hits, there should be
    # 2 cache misses for every iteration (jar is loaded by both the FE and BE).
    expected_cache_misses = class_cache_misses + (num_iterations * 2)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits)
    service.wait_for_metric_value(class_cache_misses_metric,
        expected_cache_misses)

    # Test with a table that caches the class
    create_tbl_stmt = ("CREATE TABLE {0}.data_src_tbl (x int) "
        "PRODUCED BY DATA SOURCE {1}('CACHE_CLASS::dummy_init_string')")\
        .format(unique_database, data_src_name)
    create_stmts = [create_ds_stmt, create_tbl_stmt]
    # Run once before capturing metrics because the class already may be cached from
    # a previous test run.
    # TODO: Provide a way to clear the cache
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, 1)

    # Capture metric values and run again, should hit the cache.
    class_cache_hits = service.get_metric_value(class_cache_hits_metric)
    class_cache_misses = service.get_metric_value(class_cache_misses_metric)
    self.create_drop_ddl(vector, create_stmts, drop_stmts, select_stmt, 1)
    service.wait_for_metric_value(class_cache_hits_metric, class_cache_hits + 2)
    service.wait_for_metric_value(class_cache_misses_metric, class_cache_misses)

  def create_drop_ddl(self, vector, create_stmts, drop_stmts, select_stmt,
      num_iterations=3):
    """Helper method to run CREATE/DROP DDL commands repeatedly and exercise the lib
    cache. create_stmts is the list of CREATE statements to be executed in order
    drop_stmts is the list of DROP statements to be executed in order. Each statement
    should have a '%s' placeholder to insert "IF EXISTS" or "". The select_stmt is just a
    single statement to test after executing the CREATE statements.
    TODO: it's hard to tell that the cache is working (i.e. if it did nothing to drop
    the cache, these tests would still pass). Testing that is a bit harder and requires
    us to update the udf binary in the middle.
    """
    self.client.set_configuration(vector.get_value("exec_option"))
    for drop_stmt in drop_stmts: self.client.execute(drop_stmt % ("if exists"))
    for i in xrange(0, num_iterations):
      for create_stmt in create_stmts: self.client.execute(create_stmt)
      self.client.execute(select_stmt)
      for drop_stmt in drop_stmts: self.client.execute(drop_stmt % (""))
