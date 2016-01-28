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

'''This module generates a docker environment for a job'''

from __future__ import division
from fabric.api import sudo, run, settings
from logging import getLogger
from os.path import join as join_path
from time import sleep
from tests.comparison.leopard.controller import (
    SHOULD_BUILD_IMPALA,
    SHOULD_PULL_DOCKER_IMAGE)
import random
import os

IMPALA_HOME = '/home/dev/Impala'
CORE_PATH = '/tmp/core_files'
DEFAULT_BRANCH_NAME = 'origin/cdh5-trunk'
DEFAULT_DOCKER_IMAGE_NAME = 'impala-desktop.ca.cloudera.com:5000/ubuntu-14.04:cdh5-trunk'
DOCKER_USER_NAME = 'dev'
NUM_START_ATTEMPTS = 50
NUM_FABRIC_ATTEMPTS = 50
LOG = getLogger('ImpalaDockerEnv')

def retry(func):
  '''Retry decorator.'''

  def wrapper(*args, **kwargs):
    attempt_num = 0
    while True:
      attempt_num += 1
      try:
        return func(*args, **kwargs)
      except:
        LOG.exception('{0} exception [{1}] (try: {2})'.format(
            func.__name__, args[0], attempt_num))
        if attempt_num == NUM_FABRIC_ATTEMPTS:
          raise
        sleep_time = random.randint(1, attempt_num)
        sleep(sleep_time)

  return wrapper

class ImpalaDockerEnv(object):
  '''Represents an Impala environemnt inside a Docker container. Used for starting
  Impala, getting stack traces after a crash and keeping track of the ports on which SSH,
  Postgres and Impala are running.
  '''

  def __init__(self, git_command):
    self.ssh_port = None
    self.impala_port = None
    self.postgres_port = None
    self.container_id = None
    self.git_command = git_command
    self.host = os.environ['TARGET_HOST']
    self.host_username = os.environ['TARGET_HOST_USERNAME']
    self.docker_image_name = os.environ.get(
        'DOCKER_IMAGE_NAME', DEFAULT_DOCKER_IMAGE_NAME)

  def stop_docker(self):
    with settings(warn_only = True, host_string = self.host, user = self.host_username):
      retry(sudo)('docker stop {0}'.format(self.container_id), pty=True)
      retry(sudo)('docker rm {0}'.format(self.container_id), pty=True)

  def start_new_container(self):
    '''Starts a container with port forwarding for ssh, impala and postgres. '''
    for _ in range(NUM_START_ATTEMPTS):
      with settings(warn_only = True, host_string = self.host, user = self.host_username):
        set_core_dump_location_command = \
            "echo '/tmp/core_files/core.%e.%p' | sudo tee /proc/sys/kernel/core_pattern"
        sudo(set_core_dump_location_command, pty=True)
        port = random.randint(0, 999)
        self.ssh_port = 55000 + port
        self.impala_port = 56000 + port
        self.postgres_port = 57000 + port

        start_command = ''
        if SHOULD_PULL_DOCKER_IMAGE:
          start_command = 'docker pull {docker_image_name} && '.format(
              docker_image_name = self.docker_image_name)
        start_command += (
            'docker run -d -t -p {postgres_port}:5432 -p {ssh_port}:22 '
            '-p {impala_port}:21050 {docker_image_name} /bin/docker-boot-daemon').format(
                ssh_port = self.ssh_port,
                impala_port = self.impala_port,
                postgres_port = self.postgres_port,
                docker_image_name = self.docker_image_name)

        try:
          self.container_id = sudo(start_command, pty=True)
        except:
          LOG.exception('start_new_container')
      if self.container_id is not None:
        break
    else:
      LOG.error('Container failed to start after {0} attempts'.format(NUM_START_ATTEMPTS))

  def get_git_hash(self):
    '''Returns Git hash if the current commit. '''
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      git_hash = retry(run)('cd {IMPALA_HOME} && git rev-parse --short HEAD'.format(
        IMPALA_HOME = IMPALA_HOME))
      return git_hash

  def run_all(self):
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      run_all_command = (
          'mkdir -p {CORE_PATH} && chmod 777 {CORE_PATH} && cd {IMPALA_HOME} '
          '&& source {IMPALA_HOME}/bin/impala-config.sh '
          '&& {IMPALA_HOME}/bin/create-test-configuration.sh '
          '&& {IMPALA_HOME}/testdata/bin/run-all.sh').format(
              IMPALA_HOME = IMPALA_HOME,
              CORE_PATH=CORE_PATH)
      retry(run)(run_all_command, pty=False)

  def build_impala(self):
    '''Fetches and Builds Impala. If git_command is not present the latest version is
    fetched by default. '''

    build_command = None
    if self.git_command:
      build_command = (
          'docker-boot && cd {IMPALA_HOME} && {git_command} '
          '&& source {IMPALA_HOME}/bin/impala-config.sh '
          '&& {IMPALA_HOME}/buildall.sh -notests').format(
              git_command = self.git_command,
              IMPALA_HOME = IMPALA_HOME,
              CORE_PATH = CORE_PATH)
    elif SHOULD_BUILD_IMPALA:
      build_command = (
          'docker-boot && cd {IMPALA_HOME} '
          '&& git fetch --all && git checkout origin/cdh5-trunk '
          '&& source {IMPALA_HOME}/bin/impala-config.sh '
          '&& {IMPALA_HOME}/buildall.sh -notests').format(
              IMPALA_HOME = IMPALA_HOME,
              CORE_PATH = CORE_PATH)

    if build_command:
      with settings(
          warn_only = True,
          host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
          password = os.environ['DOCKER_PASSWORD']):
        result = retry(run)(build_command, pty=False)
        LOG.info('Build Complete, Result: {0}'.format(result))

  def start_impala(self):
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      start_command = ('source {IMPALA_HOME}/bin/impala-config.sh '
        '&& {IMPALA_HOME}/bin/start-impala-cluster.py').format(IMPALA_HOME = IMPALA_HOME)
      result = retry(run)(start_command, pty=False)
      return result

  def is_impala_running(self):
    '''Check that exactly 3 impalads are running inside the docker instance.'''
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      return retry(run)('ps aux | grep impalad').count('/service/impalad') == 3

  def get_stack(self):
    '''Finds the newest core file and extracts the stack trace from it using gdb.
    '''
    IMPALAD_PATH = '{IMPALA_HOME}/be/build/debug/service/impalad'.format(
        IMPALA_HOME = IMPALA_HOME)
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      core_file_name = retry(run)('ls {0} -t1 | head -1'.format(CORE_PATH))
      LOG.info('Core File Name: {0}'.format(core_file_name))
      if 'core' not in core_file_name:
        return None
      core_full_path = join_path(CORE_PATH, core_file_name)
      stack_trace = retry(run)('gdb {0} {1} --batch --quiet --eval-command=bt'.format(
        IMPALAD_PATH, core_full_path))
      self.delete_core_files()
      return stack_trace

  def delete_core_files(self):
    '''Delete all core files. This is usually done after the stack was extracted.'''
    with settings(
        warn_only = True,
        host_string = '{0}@{1}:{2}'.format(DOCKER_USER_NAME, self.host, self.ssh_port),
        password = os.environ['DOCKER_PASSWORD']):
      retry(run)('rm -f {0}/core.*'.format(CORE_PATH))

  def prepare(self):
    '''Create a new Impala Environment. Starts a docker container and builds Impala in it.
    '''
    self.start_new_container()
    LOG.info('Container Started')
    # Wait for the SSH service to start inside the docker instance.  Usually takes 1
    # second. This is simple and reliable. An alternative implementation is to poll with
    # timeout if SSH was started.
    sleep(10)
    self.build_impala()
    try:
      result = self.run_all()
    except Exception:
      LOG.info('run_all exception')
    LOG.info('Run All Complete, Result: {0}'.format(result))
