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

#include "exprs/utility-functions.h"

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "util/debug-util.h"

namespace impala {

void UtilityFunctions::UuidPrepare(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope) {
  if (scope == FunctionContext::THREAD_LOCAL) {
    if (ctx->GetFunctionState(FunctionContext::THREAD_LOCAL) == NULL) {
      boost::uuids::random_generator* uuid_gen =
          new boost::uuids::random_generator;
      ctx->SetFunctionState(scope, uuid_gen);
    }
  }
}

// This function is not cross-compiled to avoid including unnecessary boost library's
// header files which bring in a bunch of unused code and global variables and increase
// the codegen time. The indirect call in this function is expensive enough that not
// inlining won't make much of a difference.
StringVal UtilityFunctions::GenUuid(FunctionContext* ctx) {
  void* uuid_gen = ctx->GetFunctionState(FunctionContext::THREAD_LOCAL);
  DCHECK(uuid_gen != NULL);
  boost::uuids::uuid uuid_value =
      (*reinterpret_cast<boost::uuids::random_generator*>(uuid_gen))();
  const std::string cxx_string = boost::uuids::to_string(uuid_value);
  return StringVal::CopyFrom(ctx, reinterpret_cast<const uint8_t*>(cxx_string.c_str()),
      cxx_string.length());
}

void UtilityFunctions::UuidClose(FunctionContext* ctx,
    FunctionContext::FunctionStateScope scope){
  if (scope == FunctionContext::THREAD_LOCAL) {
    boost::uuids::random_generator* uuid_gen =
        reinterpret_cast<boost::uuids::random_generator*>(
            ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
    DCHECK(uuid_gen != NULL);
    delete uuid_gen;
  }
}

}
