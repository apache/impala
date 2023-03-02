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
import pkgutil

PLUGIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'benchmark', 'plugins')

# Setup logging for this module.
LOG = logging.getLogger('plugin_runner')
LOG.setLevel(level=logging.INFO)

class PluginRunner(object):
  ''' Loads user specified plugins, if found, and initializes them.

  Looks in /tests/plugins and searches each module for plugin_name. plugin_name
  is the name of the class that the user has used to implement a plugin. If the class
  is found, it is initialized and added to self.__plugins. If it's not found, an error
  message is logged and the plugin in not loaded.
  '''

  def __init__(self, plugin_infos):
    self.__available_modules = self.__get_plugin_modules()
    self.__get_plugins_from_modules(plugin_infos)

  @property
  def plugins(self):
    return self.__plugins

  def __getstate__(self):
    state = self.__dict__.copy()
    del state['__available_modules']
    return state

  def __get_plugin_modules(self):
    ''' Gets all the modules in the directory and imports them'''
    modules = pkgutil.iter_modules(path=[PLUGIN_DIR])
    available_modules = []
    for loader, mod_name, ispkg in modules:
      yield  __import__("tests.benchmark.plugins.%s" % mod_name, fromlist=[mod_name])

  def __get_plugins_from_modules(self, plugin_infos):
    '''Look for user specified plugins in the available modules.'''
    self.__plugins = []
    plugin_names = []
    for module in self.__available_modules:
      for plugin_info in plugin_infos:
        plugin_name, scope = self.__get_plugin_info(plugin_info)
        plugin_names.append(plugin_name)
        if hasattr(module, plugin_name):
          self.__plugins.append(getattr(module, plugin_name)(scope=scope.lower()))
    # The plugin(s) that could not be loaded are captured in the set difference
    # between plugin_names and self.__plugins
    plugins_found = [p.__name__ for p in self.__plugins]
    LOG.debug("Plugins found: %s" % ', '.join(plugins_found))
    plugins_not_found = set(plugin_names).difference(plugins_found)
    # If the user's entered a plugin that does not exist, raise an error.
    if len(plugins_not_found):
      msg = "Plugin(s) not found: %s" % (','.join(list(plugins_not_found)))
      raise RuntimeError(msg)

  def __get_plugin_info(self, plugin_info):
    info = plugin_info.split(':')
    if len(info) == 1:
      return info[0], 'query'
    elif len(info) == 2:
      return info[0], info[1]
    else:
      raise ValueError("Plugin names specified in the form <plugin_name>[:<scope>]")

  def print_plugin_names(self):
    for p in self.__plugins:
      LOG.debug("Plugin: %s, Scope: %s" % (p.__name__, p.scope))

  def run_plugins_pre(self, context=None, scope=None):
    if len(self.__plugins) == 0: return
    if context: context['scope'] = scope
    for p in self.__plugins:
      if not scope or p.scope == scope.lower():
        LOG.debug('Running pre-hook for %s at scope %s' % (p.__name__, scope))
        p.run_pre_hook(context=context)

  def run_plugins_post(self, context=None, scope=None):
    if len(self.__plugins) == 0: return
    for p in self.__plugins:
      if not scope or p.scope == scope.lower():
        LOG.debug('Running post-hook for %s at scope %s' % (p.__name__, scope))
        p.run_post_hook(context=context)
