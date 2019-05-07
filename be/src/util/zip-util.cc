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

#include "util/zip-util.h"

#include "gen-cpp/Zip_types.h"
#include "rpc/jni-thrift-util.h"

#include "common/names.h"

namespace impala {

jclass ZipUtil::zip_util_class_;
jmethodID ZipUtil::extract_files_method; // ZipUtil.extractFiles()

void ZipUtil::InitJvm() {
  JNIEnv* env = JniUtil::GetJNIEnv();
  zip_util_class_ = env->FindClass("org/apache/impala/util/ZipUtil");
  ABORT_IF_EXC(env);
  JniMethodDescriptor extract_files_method_desc =
      {"extractFiles", "([B)V", &extract_files_method};

  ABORT_IF_ERROR(JniUtil::LoadStaticJniMethod(
      env, zip_util_class_, &extract_files_method_desc));
}

Status ZipUtil::ExtractFiles(const string& archive_file, const string& destination_dir) {
  TExtractFromZipParams params;
  params.archive_file = archive_file;
  params.destination_dir = destination_dir;
  return JniCall::static_method(zip_util_class_, extract_files_method)
      .with_thrift_arg(params).Call();
}

}
