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


/// Header with declarations of Impala IR data. Definitions of the arrays are generated
/// separately.

#ifndef IMPALA_CODEGEN_IR_DATA_H
#define IMPALA_CODEGEN_IR_DATA_H

extern const unsigned char impala_llvm_o1_ir[];
extern const size_t impala_llvm_o1_ir_len;

extern const unsigned char impala_llvm_o2_ir[];
extern const size_t impala_llvm_o2_ir_len;

extern const unsigned char impala_llvm_os_ir[];
extern const size_t impala_llvm_os_ir_len;

extern const unsigned char impala_legacy_avx_llvm_ir[];
extern const size_t impala_legacy_avx_llvm_ir_len;

#endif
