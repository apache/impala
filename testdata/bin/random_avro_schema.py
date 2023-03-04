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
from builtins import range
from random import choice, randint, random, shuffle
from os.path import join as join_path
from optparse import OptionParser

import json

MAX_NUM_STRUCT_FIELDS = 8
NULL_CHANCE = 0.5
SCALAR_TYPES = ['boolean', 'int', 'long', 'float', 'double', 'string']

class Node(object):

  def __init__(self, num_fields, node_type):
    self.node_type = node_type # one of struct, map, array
    self.num_fields = num_fields
    self.fields = []


class SchemaTreeGenerator(object):

  def __init__(self, target_num_scalars=10, target_depth=3):
    self._target_num_scalars = target_num_scalars
    self._target_depth = target_depth
    self._nodes = []
    self._num_scalars_created = 0
    self.root = None

  def _create_random_node(self):
    node_type = choice(('map', 'array', 'struct'))
    if node_type in ('map', 'array'):
      result_node = Node(1, node_type)
    else:
      num_fields = randint(1, MAX_NUM_STRUCT_FIELDS)
      self._num_scalars_created += num_fields - 1
      result_node = Node(num_fields, 'struct')
    self._nodes.append(result_node)
    return result_node

  def _get_random_existing_node(self):
    nodes = []
    for node in self._nodes:
      for _ in range(node.num_fields - len(node.fields)):
        nodes.append(node)
    return choice(nodes)

  def _generate_rest(self):
    while self._num_scalars_created < self._target_num_scalars:
      node = self._get_random_existing_node()
      node.fields.append(self._create_random_node())
    self._finalize()

  def _generate_trunk(self):
    cur = self.root
    for i in range(self._target_depth):
      new_node = self._create_random_node()
      self._nodes.append(new_node)
      cur.fields.append(new_node)
      cur = new_node

  def _finalize(self):
    for node in self._nodes:
      for _ in range(node.num_fields - len(node.fields)):
        node.fields.append(choice(SCALAR_TYPES))
      shuffle(node.fields)

  def create_tree(self):
    self.root = Node(randint(1, MAX_NUM_STRUCT_FIELDS), 'struct')
    self._nodes = [self.root]
    self._num_scalars_created = self.root.num_fields
    self._generate_trunk()
    self._generate_rest()
    return self.root


class AvroGenerator(object):

  def __init__(self, schema_tree_generator):
    self.cur_id = 0
    self._schema_tree_generator = schema_tree_generator

  def _next_id(self):
    self.cur_id += 1
    return str(self.cur_id)

  def clear_state(self):
    self.cur_id = 0

  def create(self, table_name):
    tree_root = self._schema_tree_generator.create_tree()
    result = {}
    result['type'] = 'record'
    result['namespace'] = 'org.apache.impala'
    result['name'] = table_name
    result['fields'] = self._convert_struct_fields(tree_root.fields)
    return result

  def _convert_struct_fields(self, fields):
    return [self._convert_struct_field(field) for field in fields]

  def _convert_struct_field(self, struct_field_node):
    result = {}
    result['type'] = self._convert_node(struct_field_node)
    result['name'] = 'field_' + self._next_id()
    return result

  def _convert_node(self, node):
    if isinstance(node, str):
      result = node
    elif node.node_type == 'array':
      result = self._convert_array(node)
    elif node.node_type == 'map':
      result = self._convert_map(node)
    elif node.node_type == 'struct':
      result = self._convert_struct(node)
    else:
      assert False, 'Unknown type: ' + node.node_types
    if random() < NULL_CHANCE:
      # Make it nullable
      return ['null', result]
    else:
      return result

  def _convert_array(self, array_node):
    result = {}
    result['type'] = 'array'
    result['items'] = self._convert_node(array_node.fields[0])
    return result

  def _convert_map(self, map_node):
    result = {}
    result['type'] = 'map'
    result['values'] = self._convert_node(map_node.fields[0])
    return result

  def _convert_struct(self, struct_node):
    result = {}
    result['type'] = 'record'
    result['name'] = 'struct_' + self._next_id()
    result['fields'] = self._convert_struct_fields(struct_node.fields)
    return result


if __name__ == '__main__':
  parser = OptionParser()
  parser.add_option('--target_dir', default='/tmp',
      help='Directory where the avro schemas will be saved.')
  parser.add_option('--num_tables', default='4', type='int',
      help='Number of schemas to generate.')
  parser.add_option('--num_scalars', default='10', type='int',
      help='Number of schemas to generate.')
  parser.add_option('--nesting_depth', default='3', type='int',
      help='Number of schemas to generate.')
  parser.add_option('--base_table_name', default='table_',
      help='Base table name.')
  options, args = parser.parse_args()

  schema_generator = SchemaTreeGenerator(target_num_scalars=options.num_scalars,
      target_depth=options.nesting_depth)
  writer = AvroGenerator(schema_generator)

  for table_num in range(options.num_tables):
    writer.clear_state()
    table_name = options.base_table_name + str(table_num)
    json_result = writer.create(table_name)
    file_path = join_path(options.target_dir, table_name + '.avsc')

    with open(file_path, 'w') as f:
      json.dump(json_result, f, indent=2, sort_keys=True)
