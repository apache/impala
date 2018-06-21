// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/symbols-util.h"
#include <cxxabi.h>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <boost/preprocessor/stringize.hpp>

#include "common/names.h"

using boost::algorithm::split_regex;
using boost::regex;
using namespace impala;

// For the rules about gcc-compatible name mangling, see:
// http://mentorembedded.github.io/cxx-abi/abi.html#mangling
// This implementation *is* not generally compatible. It is harded coded to
// only work with functions that implement the UDF or UDA signature. That is,
// functions of the form:
//   namespace::Function(impala_udf::FunctionContext*, const impala_udf::AnyVal&, etc)
//
// The general idea is to walk the types left to right and output them. This happens
// in a single pass. User literals are output as <len><literal>. There are many reserved,
// usually single character tokens for native types and specifying if something is a
// pointer.
//
// One additional piece of complexity is that repeated literals are compressed out.
// As literals are output, they are associated with an ID. The next time that
// we encounter the literal, we output the ID instead.
// We don't implement this generally since the way the literals are added to the
// dictionary is much more general than we need.
// e.g. for the literal ns1::ns2::class::type,
// the dictionary would add 4 literals: 'ns1', 'ns1::ns2', 'ns1::ns2::class',
//    'ns1::ns2::class::type'
// We instead take some shortcuts since we know all the argument types are
// types we define.

// Mangled symbols must start with this.
const char* MANGLE_PREFIX = "_Z";

bool SymbolsUtil::IsMangled(const string& symbol) {
  return strncmp(symbol.c_str(), MANGLE_PREFIX, strlen(MANGLE_PREFIX)) == 0;
}

string SymbolsUtil::Demangle(const string& name) {
  int status = 0;
  char* demangled_name = abi::__cxa_demangle(name.c_str(), NULL, NULL, &status);
  if (status != 0) return name;
  string result = demangled_name;
  free(demangled_name);
  return result;
}

string SymbolsUtil::DemangleNoArgs(const string& symbol) {
  string fn_name = Demangle(symbol);
  // Chop off argument list (e.g. "foo(int)" => "foo")
  return fn_name.substr(0, fn_name.find('('));
}

string SymbolsUtil::DemangleNameOnly(const string& symbol) {
  string fn_name = DemangleNoArgs(symbol);
  // Chop off namespace and/or class name if present (e.g. "impala::foo" => "foo")
  // TODO: fix for templates
  return fn_name.substr(fn_name.find_last_of(':') + 1);
}

// Appends <Length><String> to the stream.
// e.g. Hello --> "5Hello"
static void AppendMangledToken(const string& s, stringstream* out) {
  DCHECK(!s.empty());
  (*out) << s.size() << s;
}

// Outputs the seq_id. This is base 36 encoded with an S prefix and _ suffix.
// As an added optimization, the "seq_id - 1" value is output with the first
// token as just "S".
// e.g. seq_id 0: "S_"
//      seq_id 1: "S0_"
//      seq_id 2: "S1_"
static void AppendSeqId(int seq_id, stringstream* out) {
  DCHECK_GE(seq_id, 0);
  if (seq_id == 0) {
    (*out) << "S_";
    return;
  }
  --seq_id;
  char buffer[10];
  char* ptr = buffer + 10;
  if (seq_id == 0) *--ptr = '0';
  while (seq_id != 0) {
    DCHECK(ptr > buffer);
    char c = static_cast<char>(seq_id % 36);
    *--ptr = (c < 10 ? '0' + c : 'A' + c - 10);
    seq_id /=36;
  }
  (*out) << "S";
  out->write(ptr, 10 - (ptr - buffer));
  (*out) << "_";
}

#define CASE_TYPE_APPEND_MANGLED_TOKEN(type_lit, type_val) \
    case type_lit: AppendMangledToken(#type_val, s); break;

static void AppendAnyValType(int namespace_id, const ColumnType& type, stringstream* s) {
  (*s) << "N";
  // All the AnyVal types are in the impala_udf namespace, that token
  // already came with impala_udf::FunctionContext
  AppendSeqId(namespace_id, s);

  switch (type.type) {
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_BOOLEAN, BooleanVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_TINYINT, TinyIntVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_SMALLINT, SmallIntVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_INT, IntVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_DATE, DateVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_BIGINT, BigIntVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_FLOAT, FloatVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_DOUBLE, DoubleVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_STRING, StringVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_VARCHAR, StringVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_CHAR, StringVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_TIMESTAMP, TimestampVal)
    CASE_TYPE_APPEND_MANGLED_TOKEN(TYPE_DECIMAL, DecimalVal)

    default:
      DCHECK(false) << "NYI: " << type.DebugString();
  }
  (*s) << "E"; // end impala_udf namespace
}

string SymbolsUtil::MangleUserFunction(const string& fn_name,
    const vector<ColumnType>& arg_types, bool has_var_args,
    ColumnType* ret_arg_type) {
  // We need to split fn_name by :: to separate scoping from tokens
  vector<string> name_tokens;
  split_regex(name_tokens, fn_name, regex("::"));

  // Mangled names use substitution as a builtin compression. The first time a token
  // is seen, we output the raw token string and store the index ("seq_id"). The
  // next time we see the same token, we output the index instead.
  int seq_id = 0;

  // Sequence id for the impala_udf namespace token
  int impala_udf_seq_id = -1;

  stringstream ss;
  ss << MANGLE_PREFIX;
  if (name_tokens.size() > 1) {
    ss << "N";  // Start namespace
    seq_id += name_tokens.size() - 1; // Append for all the name space tokens.
  }
  for (int i = 0; i < name_tokens.size(); ++i) {
    AppendMangledToken(name_tokens[i], &ss);
  }
  if (name_tokens.size() > 1) ss << "E"; // End fn namespace
  ss << "PN"; // First argument and start of FunctionContext namespace
  AppendMangledToken("impala_udf", &ss);
  impala_udf_seq_id = seq_id++;
  AppendMangledToken("FunctionContext", &ss);
  ++seq_id;
  ss << "E"; // E indicates end of namespace

  map<PrimitiveType, int> argument_map;
  for (int i = 0; i < arg_types.size(); ++i) {
    int repeated_symbol_idx = -1; // Set to >0, if we've seen the symbol.
    if (argument_map.find(arg_types[i].type) != argument_map.end()) {
      repeated_symbol_idx = argument_map[arg_types[i].type];
    }

    if (has_var_args && i == arg_types.size() - 1) {
      // We always specify varargs as int32 followed by the type.
      ss << "i"; // The argument for the number of varargs.
      ss << "P"; // This indicates what follows is a ptr (that is the array of varargs)
      ++seq_id; // For "P"
      if (repeated_symbol_idx > 0) {
        AppendSeqId(repeated_symbol_idx - 1, &ss);
        continue;
      }
    } else {
      if (repeated_symbol_idx > 0) {
        AppendSeqId(repeated_symbol_idx, &ss);
        continue;
      }
      ss << "R"; // This indicates it is a reference type
      ++seq_id; // For R.
    }

    ss << "K"; // This indicates it is const
    seq_id += 2; // For impala_udf::*Val, which is two tokens.
    AppendAnyValType(impala_udf_seq_id, arg_types[i], &ss);
    argument_map[arg_types[i].type] = seq_id;
  }

  // Output return argument.
  if (ret_arg_type != NULL) {
    int repeated_symbol_idx = -1;
    if (argument_map.find(ret_arg_type->type) != argument_map.end()) {
      repeated_symbol_idx = argument_map[ret_arg_type->type];
    }
    ss << "P"; // Return argument is a pointer

    if (repeated_symbol_idx != -1) {
      // This is always last and a pointer type.
      AppendSeqId(argument_map[ret_arg_type->type] - 2, &ss);
    } else {
      AppendAnyValType(impala_udf_seq_id, *ret_arg_type, &ss);
    }
  }

  return ss.str();
}

string SymbolsUtil::ManglePrepareOrCloseFunction(const string& fn_name) {
  // We need to split fn_name by :: to separate scoping from tokens
  vector<string> name_tokens;
  split_regex(name_tokens, fn_name, regex("::"));

  // Mangled names use substitution as a builtin compression. The first time a token
  // is seen, we output the raw token string and store the index ("seq_id"). The
  // next time we see the same token, we output the index instead.
  int seq_id = 0;

  stringstream ss;
  ss << MANGLE_PREFIX;
  if (name_tokens.size() > 1) {
    ss << "N";  // Start namespace
    seq_id += name_tokens.size() - 1; // Append for all the name space tokens.
  }
  for (int i = 0; i < name_tokens.size(); ++i) {
    AppendMangledToken(name_tokens[i], &ss);
  }
  if (name_tokens.size() > 1) ss << "E"; // End fn namespace

  ss << "PN"; // FunctionContext* argument and start of FunctionContext namespace
  AppendMangledToken("impala_udf", &ss);
  AppendMangledToken("FunctionContext", &ss);
  ss << "E"; // E indicates end of namespace

  ss << "NS"; // FunctionStateScope argument
  ss << seq_id;
  ss << "_";
  AppendMangledToken("FunctionStateScope", &ss);
  ss << "E"; // E indicates end of namespace

  return ss.str();
}
