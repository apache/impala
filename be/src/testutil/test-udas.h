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

#ifndef IMPALA_UDF_TEST_UDAS_H
#define IMPALA_UDF_TEST_UDAS_H

// Don't include Impala internal headers - real UDAs won't include them.
#include "udf/udf.h"

using namespace impala_udf;

void MemTestInit(FunctionContext*, BigIntVal* total);
void MemTestUpdate(FunctionContext* context, const BigIntVal& bytes, BigIntVal* total);
void MemTestMerge(FunctionContext* context, const BigIntVal& src, BigIntVal* dst);
BigIntVal MemTestSerialize(FunctionContext* context, const BigIntVal& total);
BigIntVal MemTestFinalize(FunctionContext* context, const BigIntVal& total);

#endif
