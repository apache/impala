#!/usr/bin/env python

# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This modules allows for querying and inserting perf result data into the
# perf datastore. Currently it has very basic functionality supported with
# little error handling.
# TODO: Make this more robust, add better logging
#
import MySQLdb
import os
import sys
from datetime import datetime
from functools import wraps

# Class that allows for interaction with the perf backend.
class PerfResultDataStore(object):
  def __init__(self, host, username, password, database_name):
    print 'Database Connection Info -> %s:%s@%s/%s' % \
          (username, password, host, database_name)
    self.connection = MySQLdb.connect(host, username, password, database_name)

  def get_file_format_id(self, file_format, compression):
    """ Gets the file_format_id for the fiven file_format/compression codec"""
    return self.__get_file_format_id(file_format, compression)

  def get_query_id(self, query_name, query):
    """ Gets the query_id for the given query name and query text """
    return self.__get_query_id(query_name, query)

  def get_workload_id(self, workload, scale_factor):
    """ Gets the workload_id for the given workload / scale factor """
    return self.__get_workload_id(workload, scale_factor)

  def insert_query_info(self, query_name, query_string):
    """ Inserts a new record into the Query table and returns the ID """
    return self.__insert_query_info(query_name, query_string)

  def insert_run_info(self, run_info):
    """ Inserts a new record into the run_info table and returns the ID """
    return self.__insert_run_info(run_info)

  def insert_workload_info(self, workload_name, scale_factor):
    """ Inserts a new record into the Workload table and returns the ID """
    return self.__insert_workload_info(workload_name, scale_factor)

  def insert_execution_result(self, query_id, workload_id, file_type_id, cluster_name,
        executor_name, avg_time, stddev, run_date, version, notes, run_info_id,
        is_official=False):
    """ Inserts a perf execution result record """
    return self.__insert_execution_result(query_id, workload_id, file_type_id,
        cluster_name, executor_name, avg_time, stddev, run_date, version, notes,
        run_info_id, is_official)

  def print_execution_results(self, run_info_id):
    """ Prints results that were inserted for the given run_info_id """
    self.__print_execution_results(run_info_id)

  def cursor_wrapper(function):
    """ Handles the common initialize/close pattern for cursor objects """
    @wraps(function)
    def wrapper(*args, **kwargs):
      # args[0] is should be "self" -> PerfResultDataStore.
      # TODO: Is there a better way to get at 'self' from here?
      cursor = args[0].connection.cursor()
      result = function(*args, cursor=cursor)
      cursor.close()
      return result
    return wrapper

  # Internal methods
  @cursor_wrapper
  def __get_file_format_id(self, file_format, compression, cursor):
    """ Gets the file_format_id for the fiven file_format/compression codec"""
    if compression == 'none':
      compression_codec, compression_type = ['none', 'none']
    else:
      compression_codec, compression_type = compression.split('/')
    result = cursor.execute("select file_type_id from FileType where format='%s' and "\
                            "compression_codec='%s' and compression_type='%s'" %
                            (file_format, compression_codec, compression_type))

    file_format_id = cursor.fetchone()
    return file_format_id[0] if file_format_id else None

  @cursor_wrapper
  def __get_query_id(self, query_name, query, cursor):
    result = cursor.execute("select query_id from Query where name='%s'" % (query_name))
    query_id = cursor.fetchone()
    return query_id[0] if query_id else None

  @cursor_wrapper
  def __get_workload_id(self, workload, scale_factor, cursor):
    result = cursor.execute("select workload_id from Workload where name='%s' and "\
                            "scale_factor='%s'" % (workload, scale_factor))
    workload_id = cursor.fetchone()
    return workload_id[0] if workload_id else None

  @cursor_wrapper
  def __insert_run_info(self, run_info, cursor):
    cursor.execute("insert into RunInfo (run_info) values ('%s')" % run_info)
    result = cursor.execute("SELECT LAST_INSERT_ID()")
    run_info_id = cursor.fetchone()
    return run_info_id[0] if run_info_id else None

  @cursor_wrapper
  def __insert_execution_result(self, query_id, workload_id, file_type_id, cluster_name,
                             executor_name, avg_time, stddev, run_date, version, notes,
                             run_info_id, is_official, cursor):
    result = cursor.execute("insert into ExecutionResults (run_info_id, query_id, "\
        "workload_id, file_type_id, cluster_name, executor_name,  avg_time, stddev, "\
        "run_date, version, notes, is_official) values (%d, %d, %d, %d, '%s', '%s', %s,"\
        " %s, '%s', '%s', '%s', %s)" %\
        (run_info_id, query_id, workload_id, file_type_id, cluster_name, executor_name,
         avg_time, stddev, run_date, version, notes, is_official))

  @cursor_wrapper
  def __insert_query_info(self, name, query, cursor):
    cursor.execute("insert into Query (name, query) values ('%s', '%s')" % (name, query))
    result = cursor.execute("SELECT LAST_INSERT_ID()")
    query_id = cursor.fetchone()
    return query_id[0] if query_id else None

  @cursor_wrapper
  def __insert_workload_info(self, name, scale_factor, cursor):
    cursor.execute("insert into Workload (name, scale_factor) "\
        "values('%s', '%s')" % (name, scale_factor))
    result = cursor.execute("SELECT LAST_INSERT_ID()")
    workload_id = cursor.fetchone()
    return workload_id[0] if workload_id else None

  @cursor_wrapper
  def __print_execution_results(self, run_info_id, cursor):
    result = cursor.execute("select e.executor_name, e.run_date, q.name, w.name, "\
                            "f.format, f.compression_codec, f.compression_type, "\
                            "e.avg_time, e.cluster_name, e.notes, r.run_info, "\
                            "r.run_info_id "\
                            "from ExecutionResults e "\
                            "join RunInfo r on (e.run_info_id = r.run_info_id) "\
                            "join Query q on (e.query_id = q.query_id) "\
                            "join Workload w on (e.workload_id = w.workload_id) "\
                            "join FileType f on (e.file_type_id = f.file_type_id) "\
                            "where e.run_info_id=%d" % run_info_id)
    results = cursor.fetchall()
    for row in results:
      print row
