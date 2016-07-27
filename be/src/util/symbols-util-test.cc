// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>

#include "util/symbols-util.h"

#include "common/names.h"

namespace impala {

void TestDemangling(const string& mangled, const string& expected_demangled) {
  string result = SymbolsUtil::Demangle(mangled);
  EXPECT_EQ(result, expected_demangled);
}

void TestDemanglingNoArgs(const string& mangled, const string& expected_demangled) {
  string result = SymbolsUtil::DemangleNoArgs(mangled);
  EXPECT_EQ(result, expected_demangled);
}

void TestDemanglingNameOnly(const string& mangled, const string& expected_demangled) {
  string result = SymbolsUtil::DemangleNameOnly(mangled);
  EXPECT_EQ(result, expected_demangled);
}

// Test mangling (name, var_args, args, ret_arg_type), validating the mangled
// string and then the unmangled (~function signature) results.
void TestMangling(const string& name, bool var_args, const vector<ColumnType> args,
    ColumnType* ret_arg_type, const string& expected_mangled,
    const string& expected_demangled) {
  string mangled = SymbolsUtil::MangleUserFunction(name, args, var_args, ret_arg_type);
  string demangled = SymbolsUtil::Demangle(mangled);

  // Check we could demangle it.
  EXPECT_TRUE(!demangled.empty()) << demangled;
  EXPECT_EQ(mangled, expected_mangled);
  EXPECT_EQ(demangled, expected_demangled);
}

void TestManglingPrepareOrClose(const string& name, const string& expected_mangled,
    const string& expected_demangled) {
  string mangled = SymbolsUtil::ManglePrepareOrCloseFunction(name);
  string demangled = SymbolsUtil::Demangle(mangled);

  // Check we could demangle it.
  EXPECT_TRUE(!demangled.empty()) << demangled;
  EXPECT_EQ(mangled, expected_mangled);
  EXPECT_EQ(demangled, expected_demangled);
}

// Not very thoroughly tested since our implementation is just a wrapper around
// the gcc library.
TEST(SymbolsUtil, Demangling) {
  TestDemangling("_Z6NoArgsPN10impala_udf15FunctionContextE",
      "NoArgs(impala_udf::FunctionContext*)");
  TestDemangling("_Z8IdentityPN10impala_udf15FunctionContextERKNS_10TinyIntValE",
      "Identity(impala_udf::FunctionContext*, impala_udf::TinyIntVal const&)");
  TestDemangling("_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE",
      "Identity(impala_udf::FunctionContext*, impala_udf::StringVal const&)");
  TestDemangling("_ZN3Foo4TESTEPN10impala_udf15FunctionContextERKNS0_6IntValES5_",
      "Foo::TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&)");
  TestDemangling(
      "_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE",
      "VarSumMultiply(impala_udf::FunctionContext*, impala_udf::DoubleVal const&, int, "
          "impala_udf::IntVal const*)");
  TestDemangling("FooBar", "FooBar");
  TestDemangling(
      "Foo::TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
      "impala_udf::IntVal const&)",
      "Foo::TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
      "impala_udf::IntVal const&)");
}

TEST(SymbolsUtil, DemanglingNoArgs) {
  TestDemanglingNoArgs("_Z6NoArgsPN10impala_udf15FunctionContextE", "NoArgs");
  TestDemanglingNoArgs("_Z8IdentityPN10impala_udf15FunctionContextERKNS_10TinyIntValE",
      "Identity");
  TestDemanglingNoArgs("_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE",
      "Identity");
  TestDemanglingNoArgs("_ZN3Foo4TESTEPN10impala_udf15FunctionContextERKNS0_6IntValES5_",
      "Foo::TEST");
  TestDemanglingNoArgs(
      "_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE",
      "VarSumMultiply");
  TestDemanglingNoArgs("FooBar", "FooBar");
  TestDemanglingNoArgs(
      "Foo::TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
      "impala_udf::IntVal const&)", "Foo::TEST");
}

TEST(SymbolsUtil, DemanglingNameOnly) {
  TestDemanglingNameOnly("_Z6NoArgsPN10impala_udf15FunctionContextE", "NoArgs");
  TestDemanglingNameOnly("_Z8IdentityPN10impala_udf15FunctionContextERKNS_10TinyIntValE",
      "Identity");
  TestDemanglingNameOnly("_Z8IdentityPN10impala_udf15FunctionContextERKNS_9StringValE",
      "Identity");
  TestDemanglingNameOnly("_ZN3Foo4TESTEPN10impala_udf15FunctionContextERKNS0_6IntValES5_",
      "TEST");
  TestDemanglingNameOnly(
      "_Z14VarSumMultiplyPN10impala_udf15FunctionContextERKNS_9DoubleValEiPKNS_6IntValE",
      "VarSumMultiply");
  TestDemanglingNameOnly("FooBar", "FooBar");
  TestDemanglingNameOnly(
      "Foo::TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
      "impala_udf::IntVal const&)", "TEST");
}

// TODO: is there a less arduous way to test this?
TEST(SymbolsUtil, Mangling) {
  ColumnType int_ret_type = ColumnType(TYPE_INT);
  ColumnType double_ret_type = ColumnType(TYPE_DOUBLE);

  vector<ColumnType> args;
  TestMangling("Foo", false, args, NULL, "_Z3FooPN10impala_udf15FunctionContextE",
      "Foo(impala_udf::FunctionContext*)");
  TestMangling("Foo", false, args, &int_ret_type,
      "_Z3FooPN10impala_udf15FunctionContextEPNS_6IntValE",
      "Foo(impala_udf::FunctionContext*, impala_udf::IntVal*)");
  TestMangling("A::B", false, args, NULL, "_ZN1A1BEPN10impala_udf15FunctionContextE",
      "A::B(impala_udf::FunctionContext*)");
  TestMangling("A::B::C", false, args, NULL, "_ZN1A1B1CEPN10impala_udf15FunctionContextE",
      "A::B::C(impala_udf::FunctionContext*)");
  TestMangling("A::B::C", false, args, &int_ret_type,
      "_ZN1A1B1CEPN10impala_udf15FunctionContextEPNS1_6IntValE",
      "A::B::C(impala_udf::FunctionContext*, impala_udf::IntVal*)");

  // Try functions with in input INT arg. Then try with varargs and int* return arg.
  args.push_back(TYPE_INT);
  TestMangling("F", false, args, NULL,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValE",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&)");
  TestMangling("F", false, args, &int_ret_type,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValEPS2_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, impala_udf::IntVal*)");
  TestMangling("F", true, args, NULL,
      "_Z1FPN10impala_udf15FunctionContextEiPKNS_6IntValE",
      "F(impala_udf::FunctionContext*, int, impala_udf::IntVal const*)");
  TestMangling("F", true, args, &int_ret_type,
      "_Z1FPN10impala_udf15FunctionContextEiPKNS_6IntValEPS2_",
      "F(impala_udf::FunctionContext*, int, impala_udf::IntVal const*, "
          "impala_udf::IntVal*)");
  TestMangling("F::B", false, args, NULL,
      "_ZN1F1BEPN10impala_udf15FunctionContextERKNS0_6IntValE",
      "F::B(impala_udf::FunctionContext*, impala_udf::IntVal const&)");
  TestMangling("F::B::C::D", false, args, NULL,
      "_ZN1F1B1C1DEPN10impala_udf15FunctionContextERKNS2_6IntValE",
      "F::B::C::D(impala_udf::FunctionContext*, impala_udf::IntVal const&)");
  TestMangling("F::B", true, args, NULL,
      "_ZN1F1BEPN10impala_udf15FunctionContextEiPKNS0_6IntValE",
      "F::B(impala_udf::FunctionContext*, int, impala_udf::IntVal const*)");
  TestMangling("F::B", true, args, &int_ret_type,
      "_ZN1F1BEPN10impala_udf15FunctionContextEiPKNS0_6IntValEPS3_",
      "F::B(impala_udf::FunctionContext*, int, impala_udf::IntVal const*, "
          "impala_udf::IntVal*)");

  // Try addding some redudant argument types. These should get compressed away.
  args.push_back(TYPE_INT);
  TestMangling("F", false, args, NULL,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValES4_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&)");
  TestMangling("F", false, args, &int_ret_type,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValES4_PS2_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&, impala_udf::IntVal*)");
  TestMangling("F", true, args, NULL,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValEiPS3_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, int, "
          "impala_udf::IntVal const*)");
  TestMangling("F", true, args, &int_ret_type,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValEiPS3_PS2_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, int, "
          "impala_udf::IntVal const*, impala_udf::IntVal*)");
  TestMangling("F::B", false, args, NULL,
      "_ZN1F1BEPN10impala_udf15FunctionContextERKNS0_6IntValES5_",
      "F::B(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&)");

  args.push_back(TYPE_INT);
  TestMangling("F", false, args, NULL,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValES4_S4_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&, impala_udf::IntVal const&)");
  TestMangling("F", false, args, &int_ret_type,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValES4_S4_PS2_",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&, impala_udf::IntVal const&, "
          "impala_udf::IntVal*)");
  TestMangling("F", false, args, &double_ret_type,
      "_Z1FPN10impala_udf15FunctionContextERKNS_6IntValES4_S4_PNS_9DoubleValE",
      "F(impala_udf::FunctionContext*, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&, impala_udf::IntVal const&, "
          "impala_udf::DoubleVal*)");

  // Try some more complicated cases.
  // fn(double, double, int, int)
  args.clear();
  args.push_back(TYPE_DOUBLE);
  args.push_back(TYPE_DOUBLE);
  args.push_back(TYPE_INT);
  args.push_back(TYPE_INT);
  TestMangling("TEST", false, args, NULL,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_9DoubleValES4_RKNS_6IntValES7_",
      "TEST(impala_udf::FunctionContext*, impala_udf::DoubleVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&)");
  TestMangling("TEST", true, args, NULL,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_9DoubleValES4_RKNS_6IntValEiPS6_",
      "TEST(impala_udf::FunctionContext*, impala_udf::DoubleVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::IntVal const&, int, "
          "impala_udf::IntVal const*)");
  TestMangling("TEST", false, args, &int_ret_type,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_9DoubleValES4_RKNS_6IntValES7_PS5_",
      "TEST(impala_udf::FunctionContext*, impala_udf::DoubleVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::IntVal const&, "
          "impala_udf::IntVal const&, impala_udf::IntVal*)");
  TestMangling("TEST", true, args, &double_ret_type,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_9DoubleValES4_RKNS_6IntValEiPS6_PS2_",
      "TEST(impala_udf::FunctionContext*, impala_udf::DoubleVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::IntVal const&, int, "
          "impala_udf::IntVal const*, impala_udf::DoubleVal*)");

  // fn(int, double, double, int)
  args.clear();
  args.push_back(TYPE_INT);
  args.push_back(TYPE_DOUBLE);
  args.push_back(TYPE_DOUBLE);
  args.push_back(TYPE_INT);
  TestMangling("TEST", false, args, NULL,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_6IntValERKNS_9DoubleValES7_S4_",
      "TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, impala_udf::"
          "DoubleVal const&, impala_udf::DoubleVal const&, impala_udf::IntVal const&)");
  TestMangling("TEST", true, args, NULL,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_6IntValERKNS_9DoubleValES7_iPS3_",
      "TEST(impala_udf::FunctionContext*, impala_udf::IntVal const&, impala_udf::"
          "DoubleVal const&, impala_udf::DoubleVal const&, int, "
          "impala_udf::IntVal const*)");

  // All the types.
  args.clear();
  args.push_back(TYPE_STRING);
  args.push_back(TYPE_BOOLEAN);
  args.push_back(TYPE_TINYINT);
  args.push_back(TYPE_SMALLINT);
  args.push_back(TYPE_INT);
  args.push_back(TYPE_BIGINT);
  args.push_back(TYPE_FLOAT);
  args.push_back(TYPE_DOUBLE);
  args.push_back(TYPE_TIMESTAMP);
  args.push_back(ColumnType::CreateCharType(10));
  TestMangling("AllTypes", false, args, NULL,
      "_Z8AllTypesPN10impala_udf15FunctionContextERKNS_9StringValERKNS_10"
          "BooleanValERKNS_10TinyIntValERKNS_11SmallIntValERKNS_6IntValERKNS_9BigIntVal"
          "ERKNS_8FloatValERKNS_9DoubleValERKNS_12TimestampValERKNS_9StringValE",
      "AllTypes(impala_udf::FunctionContext*, impala_udf::StringVal const&, "
          "impala_udf::BooleanVal const&, impala_udf::TinyIntVal const&, "
          "impala_udf::SmallIntVal const&, impala_udf::IntVal const&, "
          "impala_udf::BigIntVal const&, impala_udf::FloatVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::TimestampVal const&, "
          "impala_udf::StringVal const&)");
  TestMangling("AllTypes", false, args, &int_ret_type,
      "_Z8AllTypesPN10impala_udf15FunctionContextERKNS_9StringValERKNS_10BooleanValE"
          "RKNS_10TinyIntValERKNS_11SmallIntValERKNS_6IntValERKNS_9BigIntValE"
          "RKNS_8FloatValERKNS_9DoubleValERKNS_12TimestampValERKNS_9StringValEPSE_",
      "AllTypes(impala_udf::FunctionContext*, impala_udf::StringVal const&, "
          "impala_udf::BooleanVal const&, impala_udf::TinyIntVal const&, "
          "impala_udf::SmallIntVal const&, impala_udf::IntVal const&, "
          "impala_udf::BigIntVal const&, impala_udf::FloatVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::TimestampVal const&, "
          "impala_udf::StringVal const&, impala_udf::IntVal*)");
  TestMangling("AllTypes", false, args, &double_ret_type,
      "_Z8AllTypesPN10impala_udf15FunctionContextERKNS_9StringValERKNS_10BooleanValE"
          "RKNS_10TinyIntValERKNS_11SmallIntValERKNS_6IntValERKNS_9BigIntValE"
          "RKNS_8FloatValERKNS_9DoubleValERKNS_12TimestampValERKNS_9StringValEPSN_",
      "AllTypes(impala_udf::FunctionContext*, impala_udf::StringVal const&, "
          "impala_udf::BooleanVal const&, impala_udf::TinyIntVal const&, "
          "impala_udf::SmallIntVal const&, impala_udf::IntVal const&, "
          "impala_udf::BigIntVal const&, impala_udf::FloatVal const&, "
          "impala_udf::DoubleVal const&, impala_udf::TimestampVal const&, "
          "impala_udf::StringVal const&, impala_udf::DoubleVal*)");

  // CHAR(N) type. This has restricted use. It can only be an intermediate type.
  args.clear();
  ColumnType char_ret_type = ColumnType::CreateCharType(10);
  TestMangling("TEST", false, args, &char_ret_type,
      "_Z4TESTPN10impala_udf15FunctionContextEPNS_9StringValE",
      "TEST(impala_udf::FunctionContext*, impala_udf::StringVal*)");
  args.push_back(ColumnType::CreateCharType(10));
  TestMangling("TEST", false, args, &char_ret_type,
      "_Z4TESTPN10impala_udf15FunctionContextERKNS_9StringValEPS2_",
      "TEST(impala_udf::FunctionContext*, impala_udf::StringVal const&, "
      "impala_udf::StringVal*)");
}

TEST(SymbolsUtil, ManglingPrepareOrClose) {
  TestManglingPrepareOrClose("CountPrepare",
      "_Z12CountPreparePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE",
      "CountPrepare(impala_udf::FunctionContext*,"
      " impala_udf::FunctionContext::FunctionStateScope)");
  TestManglingPrepareOrClose("foo::bar",
      "_ZN3foo3barEPN10impala_udf15FunctionContextENS1_18FunctionStateScopeE",
      "foo::bar(impala_udf::FunctionContext*,"
      " impala_udf::FunctionContext::FunctionStateScope)");
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
