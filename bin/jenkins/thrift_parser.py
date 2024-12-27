#!/usr/bin/python3
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
#
# Developed based on example at:
# https://github.com/pyparsing/pyparsing/blob/c4cf4a5/examples/protobuf_parser.py
#
# TODO: Consider using thriftpy2.parser. Need it to expose lineno.
#

from collections import defaultdict
import os
import sys
from pyparsing import (
  Forward,
  Group,
  Keyword,
  OneOrMore,
  Optional,
  ParseBaseException,
  ParseResults,
  Suppress,
  Word,
  ZeroOrMore,
  alphanums,
  alphas,
  cppStyleComment,
  delimitedList,
  line,
  lineno,
  oneOf,
  pyparsing_common,
  pythonStyleComment,
  quotedString,
  restOfLine,
)

# Simply treating strings like 'CatalogServiceVersion.V2' as an identifier by allowing
# '.' in the bodyChars
ident = Word(alphas + "_", bodyChars=alphanums + "_.").setName("identifier")
integer = pyparsing_common.integer

LBRACE, RBRACE, LBRACK, RBRACK, LPAR, RPAR, EQ, SEMI, COMMA, COLON, LT, GT =\
    map(Suppress, "{}[]()=;,:<>")

optionalCommaOrSemi = Optional(COMMA | SEMI)

# Ref: https://github.com/apache/thrift/blob/154d154/compiler/cpp/src/thrift/thriftl.ll
TYPEDEF_ = Keyword('typedef')
NAMESPACE_ = Keyword('namespace')
EXCEPTION_ = Keyword('exception')
STRUCT_ = Keyword('struct')
UNION_ = Keyword('union')
REQUIRED_ = Keyword('required')
OPTIONAL_ = Keyword('optional')
ENUM_ = Keyword('enum')
EXTENDS_ = Keyword('extends')
SERVICE_ = Keyword('service')
TRUE_ = Keyword('true')
FALSE_ = Keyword('false')
THROWS_ = Keyword('throws')
LIST_ = Keyword('list')
MAP_ = Keyword('map')
SET_ = Keyword('set')
VOID_ = Keyword('void')
CONST_ = Keyword('const')

typespec = Forward()
# Scalar types and customized types
simple_types = oneOf("void bool byte i8 i16 i32 i64 double string binary") | ident
list_type = (LIST_ | SET_) + LT + Group(typespec) + GT
map_type = MAP_ + LT + Group(typespec + COMMA + typespec) + GT
typespec << (list_type | map_type | simple_types)
typespec.setName("typespec")

list_value = LBRACK + ZeroOrMore(delimitedList(ident)) + RBRACK
rvalue = pyparsing_common.fnumber | TRUE_ | FALSE_ | ident | quotedString | list_value


class ThriftField:
  def __init__(self, line_num, field_id, qualifier, ts, name, default_value):
    self.id = field_id
    self.qualifier = qualifier
    self.type = ts
    self.name = name
    self.default_value = default_value
    self.line_num = line_num

  def __repr__(self):
    return f"Field{self.id} {self.qualifier} {self.type} {self.name} L{self.line_num}"


def create_thrift_field(src_string, locn, toks):
  # 'qualifier' and 'default_value' are optional. Use getattr() to set them as None
  # when they are missing
  qualifier = getattr(toks, "qualifier")
  default_value = getattr(toks, "defaultValue")
  return ThriftField(lineno(locn, src_string), toks["id"], qualifier, toks["ts"],
                     toks["name"], default_value)


def gen_type_string(toks):
  """Generate type strings from the nested tokens, e.g.
  [['set', ['PlanNodes.TRuntimeFilterType']]] -> 'set<PlanNodes.TRuntimeFilterType>',
  [['map', ['int', 'string']]] -> 'map<int,string>'.
  """
  if not isinstance(toks, ParseResults):
    return str(toks)
  if len(toks) == 1:
    # Somehow the first level of 'toks' has two nested levels. E.g. for list<string>,
    # 'toks' is ParseResults([ParseResults(['list', ParseResults(['string'])])]).
    return gen_type_string(toks[0])
  assert len(toks) == 2
  if toks[0] == 'set' or toks[0] == 'list':
    return f"{toks[0]}<{gen_type_string(toks[1])}>"
  assert toks[0] == 'map'
  return f"map<{gen_type_string(toks[1][0])},{gen_type_string(toks[1][1])}>"


fieldDefn = (
    integer("id")
    + COLON
    + Optional(REQUIRED_ | OPTIONAL_, "optional")("qualifier")
    + Group(typespec)("ts").setParseAction(gen_type_string)
    + ident("name")
    + Optional(EQ + rvalue("defaultValue"))
    + optionalCommaOrSemi
).setParseAction(create_thrift_field)

fieldsDefBody = Group(ZeroOrMore(fieldDefn))("body")

structDefn = STRUCT_ - ident("name") + LBRACE + fieldsDefBody + RBRACE

unionDefn = UNION_ - ident("name") + LBRACE + fieldsDefBody + RBRACE


class ThriftEnumItem:
  def __init__(self, name, value, line_num):
    self.name = name
    self.value = value
    self.line_num = line_num

  def __repr__(self):
    return f"EnumItem {self.name} L{self.line_num}"


def create_thrift_enum_item(src_string, locn, toks):
  # toks[0] is in the form of [name] or [name, value]
  value = toks[0][1] if len(toks[0]) > 1 else None
  return ThriftEnumItem(toks[0][0], value, lineno(locn, src_string))


enumDefn = (
    ENUM_ - ident("name") + LBRACE
    + OneOrMore(
        Group(ident + Optional(EQ + integer)).setParseAction(create_thrift_enum_item)
        + optionalCommaOrSemi
    )("evalues")
    + RBRACE
)

exceptionsDefn = LPAR + OneOrMore(fieldDefn) + RPAR

methodDefn = (
    typespec("returnType")
    - ident("methodName")
    + LPAR
    + fieldsDefBody
    + RPAR
    + Optional(THROWS_ + Group(exceptionsDefn))
    + optionalCommaOrSemi
)

serviceDefn = (
    SERVICE_ - ident("serviceName") + Optional(EXTENDS_ + ident)
    + LBRACE + ZeroOrMore(Group(methodDefn)) + RBRACE
)
typeDefn = TYPEDEF_ - typespec("typespec") + ident("ident")

topLevelStatement = Group(
  structDefn("struct")
  | unionDefn("union")
  | enumDefn("enum")
  | serviceDefn("service")
)

thrift_parser = ZeroOrMore(topLevelStatement)

thrift_parser.ignore(cppStyleComment)
thrift_parser.ignore(pythonStyleComment)
# TODO: Add syntax for const variables. Currently they are all defined in a single line
#  so we can ignore them like this.
thrift_parser.ignore("const " + restOfLine)
thrift_parser.ignore("namespace " + restOfLine)
thrift_parser.ignore("include " + restOfLine)


def extract_thrift_defs(thrift_file_content):
  """Extract a dict of thrift structs from pyparsing.ParseResults"""
  parse_results = thrift_parser.parseString(thrift_file_content)
  structs = defaultdict()
  enums = defaultdict()
  for top_level_item in parse_results:
    if top_level_item.getName() == 'struct':
      # A dict from field id to thrift_parser.ThriftField
      struct_fields = defaultdict()
      for field in top_level_item.body:
        struct_fields[field.id] = field
      structs[top_level_item.name] = struct_fields
    elif top_level_item.getName() == 'enum':
      # A dict from enum value (i32) to thrift_parser.ThriftEnumItem
      enum_items = defaultdict()
      index = 0
      for item in top_level_item.evalues:
        if item.value is not None:
          enum_items[item.value] = item
        else:
          # Deal with enum items without value(index), e.g. items of TQueryTableColumn
          enum_items[index] = item
          index += 1
      enums[top_level_item.name] = enum_items
  return structs, enums


if __name__ == '__main__':
  # thrift_dir = os.environ["IMPALA_HOME"] + "/common/thrift"
  thrift_dir = sys.argv[1]
  for entry in os.scandir(thrift_dir):
    if entry.is_file() and entry.name.endswith(".thrift"):
      print("parsing " + entry.path)
      with open(entry.path, 'r') as in_file:
        contents = in_file.read()
        try:
          structs, enums = extract_thrift_defs(contents)
          for struct_name, struct_fields in structs.items():
            print(f"struct {struct_name}")
            for field in struct_fields.values():
              print(field)
          for enum_name, enum_items in enums.items():
            print(f"enum {enum_name}")
            for item in enum_items.values():
              print(item)
        except ParseBaseException as e:
          print("Failed to parse line:\n" + line(e.loc, contents))
          raise e
