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

#ifndef UTIL_BACKEND_CONFIG_H_
#define UTIL_BACKEND_CONFIG_H_

#include <jni.h>

#include "common/status.h"

namespace impala {

class TBackendGflags;

Status PopulateThriftBackendGflags(TBackendGflags& cfg);
/// Builds the TBackendGflags object to pass to JNI. This is used to pass the gflag
/// configs to the Frontend and the Catalog.
Status GetThriftBackendGFlagsForJNI(JNIEnv* jni_env, jbyteArray* cfg_bytes);
}

#endif
