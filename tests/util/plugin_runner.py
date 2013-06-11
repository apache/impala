#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

import imp
import logging
import os
import pkgutil
import sys

PLUGIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'benchmark', 'plugins')

# Setup logging for this module.
logging.basicConfig(level=logging.INFO, format='%(filename)s: %(message)s')
LOG = logging.getLogger('plugin_runner')
LOG.setLevel(level=logging.DEBUG)

class PluginRunner(object):
  ''' Loads user specified plugins, if found, and initializes them.

  Looks in /tests/plugins and searches each module for plugin_name. plugin_name
  is the name of the class that the user has used to implement a plugin. If the class
  is found, it is initialized and added to self.__plugins. If it's not found, an error
  message is logged and the plugin in not loaded.
  '''

  def __init__(self, plugin_names):
    self.__available_modules = self.__get_plugin_modules()
    self.__get_plugins_from_modules(plugin_names)
    self.plugins = self.__plugins

  def __get_plugin_modules(self):
    ''' Gets all the modules in the directory and imports them'''
    modules = pkgutil.iter_modules(path=[PLUGIN_DIR])
    for loader, mod_name, ispkg in modules:
      yield __import__("tests.benchmark.plugins.%s" % mod_name, fromlist=[mod_name])

  def __get_plugins_from_modules(self, plugin_names):
    '''Look for user speicifed plugins in the availabe modules.'''
    self.__plugins = []
    for module in self.__available_modules:
      for plugin in plugin_names:
        if hasattr(module, plugin):
          self.__plugins.append(getattr(module, plugin)())
    # The plugin(s) that could not be loaded are captured in the set difference
    # between plugin_names and self.__plugins
    plugins_found = [p.__name__ for p in self.__plugins]
    LOG.debug("Plugins found: %s" % ', '.join(plugins_found))
    plugins_not_found = set(plugin_names).difference(plugins_found)
    # If the user's entered a plugin that does not exist, raise an error.
    if len(plugins_not_found):
      msg = "Plugin(s) not found: %s" % (','.join(list(plugins_not_found)))
      raise RuntimeError, msg

  def print_plugin_names(self):
    for p in self.__plugins:
      LOG.info("Plugin: %s" % p.__name__)

  def run_plugins_pre(self, context=None):
    if len(self.__plugins) == 0: return
    for p in self.__plugins:
      LOG.info('Running pre-hook for %s' % p.__name__)
      p.run_pre_hook(context=context)

  def run_plugins_post(self, context=None):
    if len(self.__plugins) == 0: return
    for p in self.__plugins:
      LOG.info('Running post-hook for %s' % p.__name__)
      p.run_post_hook(context=context)
