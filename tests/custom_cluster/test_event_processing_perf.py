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
from __future__ import absolute_import, division, print_function
from builtins import range
import logging
import multiprocessing.pool
import pytest
import re
import threading
import time
import traceback
from tests.common.impala_connection import IMPALA_CONNECTION_EXCEPTION
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_vector import HS2
from tests.util.event_processor_utils import EventProcessorUtils

LOG = logging.getLogger(__name__)
LOG.setLevel(level=logging.INFO)


@SkipIfFS.hive
class TestEventProcessingPerf(CustomClusterTestSuite):
  """This class contains tests to measure the event processing time on catalogd.
  Measures performance for various operations with queries executed from hive and impala
  clients."""
  # Below parameters are set to lower values so that tests are run faster. Need to
  # increase values and run the tests manually to measure the performance.
  db_count = 3
  table_count = 3
  partition_count = 100
  insert_nonpartition_values_count = 100
  insert_nonpartition_repeat_count = 2
  # process_events_together flag indicates whether to process events of all operations
  # together or not. Except create/drop databases and tables, remaining operations are
  # processed together when set to true. If it is set to false, each type of operation
  # (such as add partitions, insert into partitions, insert into table, refresh
  # partitions, refresh tables, compute stats etc) are processed separately to get the
  # time taken to process each type of operation.
  process_events_together = False
  db_prefix = "perf_db"
  table_prefix = "perf_table"
  stage_table = "perf_stage_table"

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestEventProcessingPerf, cls).setup_class()

  def setup_method(self, method):
    super(TestEventProcessingPerf, self).setup_method(method)
    self.__cleanup()

  def teardown_method(self, method):
    self.__cleanup()
    super(TestEventProcessingPerf, self).teardown_method(method)

  def __cleanup(self):
    self.__drop_databases()
    self.client.execute("drop table if exists {}".format(self.stage_table))
    self.__ensure_events_processed()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_hive_external_non_part_table(self):
    self.__run_event_processing_tests("test_perf_hive_external_non_part_table", True,
                                      False, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--min_event_processor_idle_ms=600000 "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_hive_external_non_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_hive_external_non_part_table_hierarchical", True, False, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_hive_external_part_table(self):
    self.__run_event_processing_tests("test_perf_hive_external_part_table", True,
                                      False, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--min_event_processor_idle_ms=600000 "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_hive_external_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_hive_external_part_table_hierarchical", True, False, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_hive_transact_non_part_table(self):
    self.__run_event_processing_tests("test_perf_hive_transact_non_part_table", True,
                                      True, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--min_event_processor_idle_ms=600000 "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_hive_transact_non_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_hive_transact_non_part_table_hierarchical", True, True, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_hive_transact_part_table(self):
    self.__run_event_processing_tests("test_perf_hive_transact_part_table", True, True,
                                      True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--min_event_processor_idle_ms=600000 "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_hive_transact_part_table_hierarchical(self):
    self.__run_event_processing_tests("test_perf_hive_transact_part_table_hierarchical",
                                      True, True, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_impala_external_non_part_table(self):
    self.__run_event_processing_tests("test_perf_impala_external_non_part_table", False,
                                      False, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_impala_external_non_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_impala_external_non_part_table_hierarchical", False, False, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_impala_external_part_table(self):
    self.__run_event_processing_tests("test_perf_impala_external_part_table", False,
                                      False, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_impala_external_part_table_hierarchical(self):
    self.__run_event_processing_tests("test_perf_impala_external_part_table_hierarchical",
                                      False, False, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_impala_transact_non_part_table(self):
    self.__run_event_processing_tests("test_perf_impala_transact_non_part_table", False,
                                      True, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_impala_transact_non_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_impala_transact_non_part_table_hierarchical", False, True, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true")
  def test_perf_impala_transact_part_table(self):
    self.__run_event_processing_tests("test_perf_impala_transact_part_table", False,
                                      True, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=0.2 "
                  "--enable_reload_events=true "
                  "--enable_hierarchical_event_processing=true")
  def test_perf_impala_transact_part_table_hierarchical(self):
    self.__run_event_processing_tests(
        "test_perf_impala_transact_part_table_hierarchical", False, True, True)

  def __run_event_processing_tests(self, case, is_hive, is_transactional, is_partitioned):
    """Method to measure time taken to create databases, tables(external if
    is_transactional is false otherwise transactional), partitions(if is_partitioned is
    true), insert data into tables, refresh partitions and tables, compute table stats
    and drop tables and databases in the end. is_hive is used to control all the queries
    to be executed on hive or impala."""
    LOG.info("Test: %s::%s" % (type(self).__name__, case))
    refresh_table_format = "refresh {}.{}"
    refresh_partition_format = refresh_table_format + " partition (j='{}')"
    stats = "analyze table {}.{} compute statistics for columns"
    dyn_part_cfg = "set hive.exec.dynamic.partition.mode=nonstrict;"
    dyn_part_cfg += "set hive.exec.max.dynamic.partitions={};" \
        .format(self.partition_count)
    dyn_part_cfg += "set hive.exec.max.dynamic.partitions.pernode={};" \
        .format(self.partition_count)
    if not is_hive:
      stats = "compute stats {}.{}"
      dyn_part_cfg = ""

    create_table_query = " ".join(["create", "external" if not is_transactional else '',
                      "table `{}`.`{}` (i int)",
                      " partitioned by (j string) " if is_partitioned else '',
                      self.__get_transactional_tblproperties(is_transactional)])

    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self, is_hive):
        # called for main thread and each thread in the pool
        self.is_hive = is_hive
        self.name = threading.currentThread().name
        LOG.info("Initializing for thread %s", self.name)

      def __del__(self):
        # Invoked only for main thread
        LOG.info("Deleting for thread %s", self.name)

      def __enter__(self):
        # Invoked only for main thread
        LOG.info("Entering for thread %s", self.name)
        return self

      def __exit__(self, exc_type, exc_val, exc_tb):
        # Invoked only for main thread
        LOG.info("Exiting for thread %s", self.name)
        if exc_type is not None:
          traceback.print_exception(exc_type, exc_val, exc_tb)

      def run_query(self, query, run_with_impala=None):
        # Creating and closing impala_client for each query. There is no good way to close
        # the impala_client connection for each ThreadLocalClient instance if we maintain
        # at instance level. Only __init__ is called for each ThreadLocalClient instance.
        # Establishing and closing connections are faster. So it should be ok to create
        # and close connection for each query.
        is_hive = self.is_hive
        if run_with_impala:
          # Overriding is_hive for some queries like describe formatted to load tables on
          # impala side
          is_hive = False

        if is_hive:
          with test_self.create_impala_client(host_port=pytest.config.option.hive_server2,
                                              protocol=HS2,
                                              is_hive=is_hive) as hive_client:
            for query in query.split(';'):
              hive_client.execute(query)
        else:
          with test_self.create_impala_client() as impala_client:
            try:
              handle = impala_client.execute_async(query)
              is_finished = impala_client.wait_for_finished_timeout(handle, timeout=60)
              assert is_finished, "Query timeout(60s): " + query
              impala_client.close_query(handle)
            except IMPALA_CONNECTION_EXCEPTION as e:
              LOG.debug(e)

    pool = multiprocessing.pool.ThreadPool(processes=8)
    with ThreadLocalClient(is_hive) as tls:
      dbs = []
      for iter in range(self.db_count):
        dbs.append(self.db_prefix + str(iter))

      tables = []
      for iter1 in range(self.table_count):
        table_name = self.table_prefix + str(iter1)
        for iter2 in range(self.db_count):
          tables.append((self.db_prefix + str(iter2), table_name))

      def create_database(dbname):
        tls.run_query("create database {}".format(dbname))

      def create_table(table_name_tuple):
        tls.run_query(create_table_query.format(table_name_tuple[0], table_name_tuple[1]))

      start_event_id = self.__pause_event_processing()
      pool.map_async(create_database, dbs).get()
      pool.map_async(create_table, tables).get()
      create_db_table_time = self.__process_events_now(start_event_id)

      def load_table(table_name_tuple):
        tls.run_query("describe formatted {}.{}"
                      .format(table_name_tuple[0], table_name_tuple[1]), True)

      pool.map_async(load_table, tables).get()

      add_part_time = None
      insert_into_part_time = None
      insert_time = None
      if is_partitioned:
        # Stage table to create dynamic partitions
        self.client.execute("create table {} (i int, j string)".format(self.stage_table))
        insert_query = "insert into {} values {}"
        values = ",".join([("(" + str(item) + ",'" + str(item) + "')")
                           for item in range(self.partition_count)])
        self.client.execute(insert_query.format(self.stage_table, values))
        self.__process_events_now(start_event_id)

        # Create dynamic partitions
        def add_or_insert_into_partitions(table_name_tuple):
          tls.run_query("{} insert into {}.{} partition(j) select * from {}"
                        .format(dyn_part_cfg, table_name_tuple[0], table_name_tuple[1],
                                self.stage_table))

        start_event_id = self.__pause_event_processing()
        pool.map_async(add_or_insert_into_partitions, tables).get()
        add_part_time = self.__process_events(start_event_id)

        # Insert into existing partitions
        start_event_id = self.__pause_event_processing()
        pool.map_async(add_or_insert_into_partitions, tables).get()
        insert_into_part_time = self.__process_events(start_event_id)
      else:
        repeat_insert_into_tables = []
        for i in range(self.insert_nonpartition_repeat_count):
          for table_name_tuple in tables:
            repeat_insert_into_tables.append(table_name_tuple)

        insert_query = "insert into {}.{} values {}"
        values = ",".join([("(" + str(item) + ")")
                           for item in range(self.insert_nonpartition_values_count)])

        def insert_into_table(table_name_tuple):
          tls.run_query(insert_query
                        .format(table_name_tuple[0], table_name_tuple[1], values))

        start_event_id = self.__pause_event_processing()
        pool.map_async(insert_into_table, repeat_insert_into_tables).get()
        insert_time = self.__process_events(start_event_id)

      # Refresh
      refresh_part_time = None
      refresh_table_time = None
      if not is_hive:
        # Refresh partitions
        if is_partitioned and not is_transactional:
          partitions = []
          for iter1 in range(self.table_count):
            table_name = self.table_prefix + str(iter1)
            for iter2 in range(self.db_count):
              for iter3 in range(self.partition_count):
                partitions.append((self.db_prefix + str(iter2), table_name, str(iter3)))

          def refresh_partition(table_name_tuple):
            tls.run_query(refresh_partition_format
                          .format(table_name_tuple[0], table_name_tuple[1],
                                  table_name_tuple[2]))

          start_event_id = self.__pause_event_processing()
          pool.map_async(refresh_partition, partitions).get()
          refresh_part_time = self.__process_events(start_event_id)

        # Refresh tables
        def refresh_table(table_name_tuple):
          tls.run_query(refresh_table_format
                        .format(table_name_tuple[0], table_name_tuple[1]))

        start_event_id = self.__pause_event_processing()
        pool.map_async(refresh_table, tables).get()
        refresh_table_time = self.__process_events(start_event_id)

      # compute statistics
      def compute_stats(table_name_tuple):
        tls.run_query(stats.format(table_name_tuple[0], table_name_tuple[1]))

      start_event_id = self.__pause_event_processing()
      pool.map_async(compute_stats, tables).get()
      compute_stats_time = self.__process_events(start_event_id)

      total_time = None
      if self.process_events_together:
        total_time = self.__process_events_now(start_event_id)

      def drop_database(dbname):
        tls.run_query("drop database if exists {} cascade".format(dbname))

      start_event_id = self.__pause_event_processing()
      pool.map_async(drop_database, dbs).get()
      drop_db_table_time = self.__process_events_now(start_event_id)

    pool.terminate()

    LOG.info("[Performance] Create database and table: Event count: {}, Time taken: {} s"
             .format(create_db_table_time[0], create_db_table_time[1]))
    if add_part_time is not None:
      LOG.info("[Performance] Add partition: Event count: {}, Time taken: {} s"
               .format(add_part_time[0], add_part_time[1]))
      LOG.info("[Performance] Insert into partition: Event count: {}, Time taken: {} s"
               .format(insert_into_part_time[0], insert_into_part_time[1]))
    if insert_time is not None:
      LOG.info("[Performance] Insert: Event count: {}, Time taken: {} s"
               .format(insert_time[0], insert_time[1]))
    if refresh_part_time is not None:
      LOG.info("[Performance] Refresh partition: Event count: {}, Time taken: {} s"
               .format(refresh_part_time[0], refresh_part_time[1]))
    if refresh_table_time is not None:
      LOG.info("[Performance] Refresh table: Event count: {}, Time taken: {} s"
               .format(refresh_table_time[0], refresh_table_time[1]))
    if compute_stats_time is not None:
      LOG.info("[Performance] Compute statistics: Event count: {}, Time taken: {} s"
               .format(compute_stats_time[0], compute_stats_time[1]))
    if total_time is not None:
      LOG.info("[Performance] Processed together: Event count: {}, Time taken: {} s"
               .format(total_time[0], total_time[1]))
    LOG.info("[Performance] Drop table and database: Event count: {}, Time taken: {} s"
             .format(drop_db_table_time[0], drop_db_table_time[1]))

  def __drop_databases(self):
    for iter in range(self.db_count):
      self.client.execute("drop database if exists {} cascade"
                          .format(self.db_prefix + str(iter)))

  def __process_events_now(self, start_event_id):
    end_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
    start_time = time.time()
    self.__ensure_events_processed()
    return end_event_id - start_event_id, time.time() - start_time

  def __process_events(self, start_event_id):
    if not self.process_events_together:
      return self.__process_events_now(start_event_id)

  def __ensure_events_processed(self):
    self.client.execute(":event_processor('start')")
    EventProcessorUtils.wait_for_event_processing(self, 100)

  def __pause_event_processing(self):
    output = self.client.execute(":event_processor('pause')").get_data()
    lastSyncedEventId = re.search(r"LastSyncedEventId:\s*(\d+)", output)
    if lastSyncedEventId:
      return int(lastSyncedEventId.group(1))
    else:
      return EventProcessorUtils.get_current_notification_id(self.hive_client)

  def __get_transactional_tblproperties(self, is_transactional):
    """Get the tblproperties for transactional tables"""
    return "tblproperties ('transactional'='true'," \
           "'transactional_properties'='insert_only')" if is_transactional else ""
