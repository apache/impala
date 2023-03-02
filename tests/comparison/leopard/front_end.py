#!/usr/bin/env impala-python
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

from __future__ import absolute_import, division, print_function
import logging
import os
import pickle
import stat
import time
from time import sleep
try:
  from flask import Flask, render_template, request
except ImportError as e:
  raise Exception(
      "Please run impala-pip install -r $IMPALA_HOME/infra/python/deps/extended-test-"
      "requirements.txt:\n{0}".format(str(e)))
from tests.comparison.leopard.schedule_item import ScheduleItem
from tests.comparison.leopard.controller import PATH_TO_REPORTS, PATH_TO_SCHEDULE
from threading import Thread
from tests.comparison.query_profile import DefaultProfile
from tests.comparison.db_types import (
     Boolean,
     Char,
     Decimal,
     Float,
     Int,
     TYPES,
     Timestamp)

MAX_REPORT_AGE = 21 * 24 * 3600 # 21 days
SLEEP_LENGTH = 20 * 60 # 20 min

LOG = logging.getLogger('leopard.front_end')

app = Flask(__name__)
app.reports = {}


ASSETS = {'bootstrap_css': 'css/bootstrap.min.css',
    'hljs_css': 'css/default.css',
    'favicon': 'favicon.ico',
    'bootstrap_js': 'js/bootstrap.min.js',
    'hljs_js': 'js/highlight.pack.js'}

@app.route('/reports/<report_id>')
def show_report(report_id):
  '''Renders a report as HTML. '''

  if report_id not in app.reports:
    with open(os.path.join(PATH_TO_REPORTS, report_id), 'r') as f:
      app.reports[report_id] = pickle.load(f)

  report = app.reports[report_id]

  def get_next_id():
    '''Generates all natural numbers. '''
    i = 0
    while True:
      yield str(i)
      i += 1

  gen = get_next_id()

  # Generate HTML for displaying the crashes
  outer_crashes_list = []
  for first_impala_frame in report.grouped_stacks:
    crashes_list = []
    # results are sorted on the length of the query SQL
    for result in sorted(report.grouped_stacks[first_impala_frame],
        key = lambda result: len(result['test_sql'])):
      inner_id = next(gen)
      inner_title = 'Lines in Stack: {0}'.format(
          len(result['formatted_stack'].split('\n')))
      content = ('<h4>Impala Query:</h4><pre><code>{0}</code></pre>'
          '<h4>Stack:</h4><pre>{1}</pre>').format(
              result['test_sql'], result['formatted_stack'][:50000])
      crashes_list.append((inner_id, inner_title, content))
    id = next(gen)
    title = first_impala_frame
    outer_crashes_list.append((id, title, crashes_list))

  # Generate HTML for displaying result row count mismatches
  row_count_list = []
  for result in sorted(report.grouped_results['row_counts'],
      key = lambda result: len(result['test_sql'])):
    id = next(gen)
    title = 'Impala Rows: {0}, Postgres Rows: {1}'.format(
        result['test_row_count'], result['ref_row_count'])
    content = ('<h4>Impala Query:</h4><pre><code>{0}</code></pre>'
        '<h4>Postgres Query:</h4><pre><code>{1}</code></pre>').format(
        result['test_sql'], result['ref_sql'])
    row_count_list.append((id, title, content))

  # Generate HTML for displaying result content mismatches
  mismatch_list = []
  for result in sorted(report.grouped_results['mismatch'],
      key = lambda result: len(result['test_sql'])):
    id = next(gen)
    title = 'Query Length: {0}'.format(len(result['test_sql']))
    content = ('<h4>Impala Query:</h4><pre><code>{0}</code></pre>'
        '<h4>Postgres Query:</h4><pre><code>{1}</code></pre>'
        '<h4>Mismatch Impala Row:</h4><pre><code>{2}</code></pre>'
        '<h4>Mismatch Postgres Row:</h4><pre><code>{3}</code></pre>').format(
        result['test_sql'],
        result['ref_sql'],
        result['mismatch_test_row'],
        result['mismatch_ref_row'])
    mismatch_list.append((id, title, content))

  return render_template(
      'report.template',
      assets=ASSETS,
      report=report,
      outer_crashes_list=outer_crashes_list,
      row_count_list=row_count_list,
      mismatch_list=mismatch_list)

@app.route('/start_run', methods=['POST', 'GET'])
def start_run():
  '''Method that receives POST requests and gernerates a schedule item.'''

  if request.method != 'POST': return 'fail'

  if 'time_limit' in request.form:
    # This is a custom run because time_limit item is present only in the custom_run form.
    # Values will be extracted from the form and a new profile will be generated.

    new_profile = DefaultProfile()

    # Bounds
    new_profile._bounds['MAX_NESTED_QUERY_COUNT'] = (
        int(request.form['max_nested_query_count_from']),
        int(request.form['max_nested_query_count_to']))
    new_profile._bounds['MAX_NESTED_EXPR_COUNT'] = (
        int(request.form['max_nested_expr_count_from']),
        int(request.form['max_nested_expr_count_to']))
    new_profile._bounds['SELECT_ITEM_COUNT'] = (
        int(request.form['select_item_count_from']),
        int(request.form['select_item_count_to']))
    new_profile._bounds['WITH_TABLE_COUNT'] = (
        int(request.form['with_table_count_from']),
        int(request.form['with_table_count_to']))
    new_profile._bounds['TABLE_COUNT'] = (
        int(request.form['table_count_from']),
        int(request.form['table_count_to']))
    new_profile._bounds['ANALYTIC_LEAD_LAG_OFFSET'] = (
        int(request.form['analytic_lead_lag_offset_from']),
        int(request.form['analytic_lead_lag_offset_to']))
    new_profile._bounds['ANALYTIC_WINDOW_OFFSET'] = (
        int(request.form['analytic_window_offset_from']),
        int(request.form['analytic_window_offset_to']))

    # Select Item Category
    new_profile._weights['SELECT_ITEM_CATEGORY']['AGG'] = int(
        request.form['select_agg'])
    new_profile._weights['SELECT_ITEM_CATEGORY']['ANALYTIC'] = int(
        request.form['select_analytic'])
    new_profile._weights['SELECT_ITEM_CATEGORY']['BASIC'] = int(
        request.form['select_basic'])

    # Types
    new_profile._weights['TYPES'][Boolean] = int(request.form['types_boolean'])
    new_profile._weights['TYPES'][Char] = int(request.form['types_char'])
    new_profile._weights['TYPES'][Decimal] = int(request.form['types_decimal'])
    new_profile._weights['TYPES'][Float] = int(request.form['types_float'])
    new_profile._weights['TYPES'][Int] = int(request.form['types_int'])
    new_profile._weights['TYPES'][Timestamp] = int(request.form['types_timestamp'])

    # Join
    new_profile._weights['JOIN']['INNER'] = int(request.form['join_inner'])
    new_profile._weights['JOIN']['LEFT'] = int(request.form['join_left'])
    new_profile._weights['JOIN']['RIGHT'] = int(request.form['join_right'])
    new_profile._weights['JOIN']['FULL_OUTER'] = int(request.form['join_full_outer'])
    new_profile._weights['JOIN']['CROSS'] = int(request.form['join_cross'])

    # Optional Query Clauses Probabilities
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['WITH'] = float(
        request.form['optional_with'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['FROM'] = float(
        request.form['optional_from'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['WHERE'] = float(
        request.form['optional_where'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['GROUP_BY'] = float(
        request.form['optional_group_by'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['HAVING'] = float(
        request.form['optional_having'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['UNION'] = float(
        request.form['optional_union'])
    new_profile._probabilities['OPTIONAL_QUERY_CLAUSES']['ORDER_BY'] = float(
        request.form['optional_order_by'])

    # Optional Analytic Clauses Probabilities
    new_profile._probabilities['OPTIONAL_ANALYTIC_CLAUSES']['PARTITION_BY'] = float(
        request.form['optional_analytic_partition_by'])
    new_profile._probabilities['OPTIONAL_ANALYTIC_CLAUSES']['ORDER_BY'] = float(
        request.form['optional_analytic_order_by'])
    new_profile._probabilities['OPTIONAL_ANALYTIC_CLAUSES']['WINDOW'] = float(
        request.form['optional_analytic_window'])

    # Misc Probabilities
    new_profile._probabilities['MISC']['INLINE_VIEW'] = float(
        request.form['misc_inline_view'])
    new_profile._probabilities['MISC']['SELECT_DISTINCT'] = float(
        request.form['misc_select_distinct'])
    new_profile._probabilities['MISC']['SCALAR_SUBQUERY'] = float(
        request.form['misc_scalar_subquery'])
    new_profile._probabilities['MISC']['UNION_ALL'] = float(
        request.form['misc_union_all'])

    # Analytic Designs
    new_profile._flags['ANALYTIC_DESIGNS']['TOP_LEVEL_QUERY_WITHOUT_LIMIT'] = \
        'analytic_designs_top_level_no_limit' in request.form
    new_profile._flags['ANALYTIC_DESIGNS']['DETERMINISTIC_ORDER_BY'] = \
        'analytic_designs_deterministic_order_by' in request.form
    new_profile._flags['ANALYTIC_DESIGNS']['NO_ORDER_BY'] = \
        'analytic_designs_no_order_by' in request.form
    new_profile._flags['ANALYTIC_DESIGNS']['ONLY_SELECT_ITEM'] = \
        'analytic_designs_only_select_item' in request.form
    new_profile._flags['ANALYTIC_DESIGNS']['UNBOUNDED_WINDOW'] = \
        'analytic_designs_unbounded_window' in request.form
    new_profile._flags['ANALYTIC_DESIGNS']['RANK_FUNC'] = \
        'analytic_designs_rank_func' in request.form

    schedule_item = ScheduleItem(
        run_name = request.form['run_name'],
        query_profile = new_profile,
        time_limit_sec = int(request.form['time_limit']),
        git_command = request.form['git_command'],
        parent_job = '')
  else:
    # Run based on previous run
    schedule_item = ScheduleItem(
        run_name = request.form['run_name'],
        query_profile = DefaultProfile(),
        time_limit_sec = 24 * 3600, # Default time limit is 24 hours
        git_command = request.form['git_command'],
        parent_job = request.form['report_id'])

  schedule_item.save_pickle()

  return 'success'

@app.route("/custom_run")
def custom_run():
  '''Render the custom run page.
  '''
  return render_template(
      'custom_run.template',
      assets=ASSETS)

def reload_reports():
  '''Reload reports in the reports directory every 20 minutes. Loaded reports are placed
  into app.reports. This allows new reports to appear on the front page. Only reports
  from the past 7 days are loaded. This method should be run in a separate thread.
  '''
  while True:
    new_reports = {}
    try:
      report_ids = os.listdir(PATH_TO_REPORTS)
    except EnvironmentError as e:
      report_ids = []
      LOG.warn('{0}: {1}'.format(e.filename, e.strerror))
    for report_id in report_ids:
      file_age = time.time() - os.stat(
          os.path.join(PATH_TO_REPORTS, report_id))[stat.ST_MTIME]
      if file_age < MAX_REPORT_AGE:
        # We want this report
        if report_id in app.reports:
          new_reports[report_id] = app.reports[report_id]
        else:
          with open(os.path.join(PATH_TO_REPORTS, report_id), 'r') as f:
            new_reports[report_id] = pickle.load(f)
    app.reports = new_reports
    sleep(SLEEP_LENGTH)

@app.route("/")
def front_page():
  '''Renders the front page as HTML.
  '''

  try:
    schedule_item_ids = os.listdir(PATH_TO_SCHEDULE)
  except EnvironmentError as e:
    schedule_item_ids = []
    LOG.warn('{0}: {1}'.format(e.filename, e.strerror))

  schedule_items = []

  for schedule_item_id in schedule_item_ids:
    schedule_items.append(pickle.load(
      open(os.path.join(PATH_TO_SCHEDULE, schedule_item_id), 'r')))

  return render_template(
      'index.template',
      assets=ASSETS,
      reports=sorted(
          app.reports.items(), key=lambda k, report: report.run_date, reverse=True),
      schedule_items=schedule_items)

if __name__ == '__main__':
  logging.basicConfig(
      format='%(asctime)s %(levelname)s [%(name)s.%(threadName)s:%(lineno)s]: '
             '%(message)s',
      level=logging.INFO)
  thread = Thread(name='reload_reports', target=reload_reports)
  thread.daemon = True
  thread.start()
  app.run(host='0.0.0.0', debug=False)
