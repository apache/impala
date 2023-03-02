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
import random
import string
import pickle
import os
from tests.comparison.query_profile import DefaultProfile, ImpalaNestedTypesProfile
from threading import Thread

ID_LEN = 16

class ScheduleItem(object):
  '''This describes the run. This class generates a Job with the generate_job method.
  This class can be extended in the future to be able to specify to run the job on a
  cluster or in stress mode.

  Attributes:
    run_name (str): This is displayed on the front page. The user enters a name when
        starting a custom run.
    git_command (str): Custom command to execute before starting a run.
    query_profile (DefaultProfile or similar)
    parent_job (str): job_id of the parent job
    parent_job_name (str): used for displaying the parent job name on the front page (in
        the schedule section)
    job_id (str): Unique string associated with this run. It is generated here and will
        be the same in Job and Report. The file name of pickle object will be this
        string.
    time_limit_sec (Number): Number of seconds to run.
  '''

  def __init__(
      self,
      run_name='default',
      query_profile=None,
      time_limit_sec=24 * 3600,
      git_command='',
      parent_job=None):
    self.run_name = run_name
    self.git_command = git_command
    self.query_profile = None
    self.parent_job = parent_job
    # It takes a while to extract the parent job name, so it's done in the save_pickle
    # method in a separate thread.
    self.parent_job_name = ''
    self.job_id = self.generate_job_id()
    self.time_limit_sec = time_limit_sec

  def generate_job_id(self):
    '''Generate a random string that should be unique.
    '''
    return ''.join([random.choice(
      string.ascii_lowercase + string.digits) for _ in range(ID_LEN)])

  def save_pickle(self):
    from tests.comparison.leopard.controller import (
        PATH_TO_SCHEDULE,
        PATH_TO_FINISHED_JOBS)

    def inner():
      if self.parent_job:
        with open(os.path.join(PATH_TO_FINISHED_JOBS,
            self.parent_job), 'r') as f:
          parent_job = pickle.load(f)
          self.parent_job_name = parent_job.job_name

      with open(os.path.join(PATH_TO_SCHEDULE, self.job_id), 'w') as f:
        pickle.dump(self, f)

    thread = Thread(target=inner)
    thread.start()

  def generate_job(self):
    '''Converts ScheduleItem into a Job.
    '''
    from job import Job
    return Job(query_profile = self.query_profile,
        job_id=self.job_id,
        run_name=self.run_name,
        time_limit_sec=self.time_limit_sec,
        git_command=self.git_command,
        parent_job=self.parent_job)
