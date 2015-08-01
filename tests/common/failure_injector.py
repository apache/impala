# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Failure injection module for the Impala service. There are two main ways this module
# can be used - the first is to initialize the failure injector and then call start()
# which will kick off a timer that chooses a random impalad/state store process
# to fail each time timer fires.
# The second way this module can be used to to initialize it and call the actions
# directly (ex. kill_random_impalad()). This provides a bit more control over exactly
# when a failure will happen and is useful for targeted test scenarios.
import logging
import os
import sys
import time
from tests.common.impala_cluster import *
from random import choice
from threading import Timer

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('failure-injector')

# This class is used for injecting failures for the Impala service.
class FailureInjector(object):
  def __init__(self, impala_cluster, failure_frequency, impalad_exclude_list=None):
    """
    Initializes the FailureInjector object.

    impala_cluster - An ImpalaCluster object (see the impala_cluster module)
    failure_frequency - Interval to fire timer (in seconds)
    impalad_exclude_list - A list of impalad host:port name to not inject failures
    on. Useful to filter out the coordinator.
    """
    self.cluster = impala_cluster
    self.cluster.get_impala_service().set_process_auto_restart_config(value=True)
    # TODO: Do we need to restart the impala service to apply this?
    # self.cluster.get_impala_service().restart()
    self.failure_frequency = failure_frequency
    num_impalad_procs = len(self.cluster.get_impala_service().get_all_impalad_processes())

    self.impalad_exclude_list = impalad_exclude_list

    # Build a weighted list of possible actions. This is done using a trivial approach
    # where we just add the item multiple times (weight value) into the action list.
    # TODO: Provide a way to dynamically configure the weights
    actions_with_weights = {self.kill_random_impalad: num_impalad_procs * 2,
                            self.kill_state_store: 1}

    self.possible_actions = list()
    for key, value in actions_with_weights.items():
      self.possible_actions.extend([key] * value)

  def start(self):
    """ Starts the timer, triggering failures for the specified interval """
    self.__start_timer()

  def cancel(self):
    """ Stops the timer, canceling any additional failures from occurring """
    if self.__timer is not None:
      self.__timer.cancel()

  def kill_random_impalad(self):
    """ Kills a randomly selected impalad instance not in the exlude list """
    filtered_impalad = \
        filter(lambda impalad: '%s:%d' % (impalad.hostname, impalad.be_port)\
               not in self.impalad_exclude_list,
               self.cluster.get_impala_service().get_all_impalad_processes())
    self.kill_impalad(choice(filtered_impalad))

  def kill_impalad(self, impalad):
    """ Kills the specified impalad instance """
    LOG.info('Chose impalad on "%s" to kill' % impalad.hostname)
    impalad.kill()

  def kill_state_store(self):
    """ Kills the statestore process """
    state_store = self.cluster.get_impala_service().get_state_store_process()
    LOG.info('Chose statestore on "%s" to kill' % state_store.hostname)
    state_store.kill()

  def __start_timer(self):
    """ Starts a new timer, cancelling the previous timer if it is running """
    self.cancel()
    self.__timer = Timer(self.failure_frequency, self.__choose_action)
    self.__timer.start()

  def __choose_action(self):
    """ Chooses a failure action to perform """
    action = choice(self.possible_actions)
    LOG.info('Executing action: %s' % action)
    action()
    self.__start_timer()
