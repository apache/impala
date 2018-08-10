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

#ifndef IMPALA_SERVICE_FE_SUPPORT_H
#define IMPALA_SERVICE_FE_SUPPORT_H

namespace impala {

/// InitFeSupport registers native functions with JNI. When the java
/// function FeSupport.EvalPredicate is called within Impalad, the native
/// implementation FeSupport_EvalPredicateImpl already exists in Impalad binary.
/// In order to expose JNI functions from Impalad binary, we need to "register
/// native functions". See this link:
///     http://java.sun.com/docs/books/jni/html/other.html#29535
/// for details.
void InitFeSupport(bool disable_codegen = true);

}
#endif
