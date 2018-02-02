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

#include "runtime/io/local-file-system-with-fault-injection.h"

namespace impala {
namespace io {

void LocalFileSystemWithFaultInjection::SetWriteFaultInjection(
    const string& function_name, int err_no) {
  fault_injection_to_write_.reset(WriteFaultInjectionItem(function_name, err_no));
}

int LocalFileSystemWithFaultInjection::OpenAux(const char* file, int option1,
    int option2) {
  if (DebugFaultInjection("open")) return -1;
  return LocalFileSystem::OpenAux(file, option1, option2);
}

FILE* LocalFileSystemWithFaultInjection::FdopenAux(int file_desc, const char* options) {
  if (DebugFaultInjection("fdopen")) return nullptr;
  return LocalFileSystem::FdopenAux(file_desc, options);
}

int LocalFileSystemWithFaultInjection::FseekAux(FILE* file_handle, off_t offset,
    int whence) {
  if (DebugFaultInjection("fseek")) return -1;
  return LocalFileSystem::FseekAux(file_handle, offset, whence);
}

size_t LocalFileSystemWithFaultInjection::FwriteAux(FILE* file_handle,
    const WriteRange* write_range) {
  if (DebugFaultInjection("fwrite")) return 0;
  return LocalFileSystem::FwriteAux(file_handle, write_range);
}

int LocalFileSystemWithFaultInjection::FcloseAux(FILE* file_handle) {
  if (DebugFaultInjection("fclose")) return EOF;
  return LocalFileSystem::FcloseAux(file_handle);
}

bool LocalFileSystemWithFaultInjection::DebugFaultInjection(
    const string& function_name) {
  if (fault_injection_to_write_ &&
      fault_injection_to_write_->function == function_name) {
    errno = fault_injection_to_write_->err_no;
    return true;
  }
  return false;
}

}
}
