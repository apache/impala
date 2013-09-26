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


#ifndef IMPALA_UDF_TEST_HARNESS_H
#define IMPALA_UDF_TEST_HARNESS_H

#include <iostream>
#include <vector>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>

#include "udf/udf.h"
#include "udf/udf-debug.h"

namespace impala_udf {

// Utility class to help test UDFs.
class UdfTestHarness {
 public:
  // Template function to execute a UDF and validate the result. They should be
  // used like:
  // ValidateUdf(udf_fn, arg1, arg2, ..., expected_result);
  // Only functions with up to 8 arguments are supported
  //
  // For variable argument udfs, the variable arguments should be passed as
  // a std::vector:
  //   ValidateUdf(udf_fn, arg1, arg2, const vector<arg3>& args, expected_result);
  template<typename RET>
  static bool ValidateUdf(boost::function<RET(FunctionContext*)> fn,
      const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get());
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1>
  static bool ValidateUdf(boost::function<RET(FunctionContext*, const A1&)> fn,
      const A1& a1, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1>
  static bool ValidateUdf(boost::function<RET(FunctionContext*, int, const A1*)> fn,
      const std::vector<A1>& a1, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1.size(), &a1[0]);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&)> fn,
      const A1& a1, const A2& a2, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, int, const A2*)> fn,
      const A1& a1, const std::vector<A2>& a2, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2.size(), &a2[0]);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, int, const A3*)> fn,
      const A1& a1, const A2& a2, const std::vector<A3>& a3, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3.size(), &a3[0]);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          const A4&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const A4& a4, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          int, const A4*)> fn,
      const A1& a1, const A2& a2, const A3& a3, const std::vector<A4>& a4,
      const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4.size(), &a4[0]);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4,
      typename A5>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          const A4&, const A5&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const A4& a4,const A5& a5,
      const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4, a5);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4,
      typename A5, typename A6>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          const A4&, const A5&, const A6&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
      const A6& a6, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4, a5, a6);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4,
      typename A5, typename A6, typename A7>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          const A4&, const A5&, const A6&, const A7&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
      const A6& a6, const A7& a7, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4, a5, a6, a7);
    return Validate(context.get(), expected, ret);
  }

  template<typename RET, typename A1, typename A2, typename A3, typename A4,
      typename A5, typename A6, typename A7, typename A8>
  static bool ValidateUdf(
      boost::function<RET(FunctionContext*, const A1&, const A2&, const A3&,
          const A4&, const A5&, const A6&, const A7&)> fn,
      const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
      const A6& a6, const A7& a7, const A8& a8, const RET& expected) {
    boost::scoped_ptr<FunctionContext> context(FunctionContext::CreateTestContext());
    RET ret = fn(context.get(), a1, a2, a3, a4, a5, a6, a7, a8);
    return Validate(context.get(), expected, ret);
  }

 private:
  template<typename RET>
  static bool Validate(FunctionContext* context, const RET& expected, const RET& actual) {
    if (context->has_error()) {
      std::cerr << "Udf Failed: " << context->error_msg() << std::endl;
      return false;
    }
    if (actual == expected) return true;

    std::cerr << "UDF did not return the correct result:" << std::endl
              << "  Expected: " << DebugString(expected) << std::endl
              << "  Actual: " << DebugString(actual) << std::endl;
    return false;
  }
};

}

#endif

