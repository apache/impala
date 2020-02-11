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

#ifndef IMPALA_RUNTIME_IO_LOCAL_FILE_SYSTEM_WITH_FAULT_INJECTION
#define IMPALA_RUNTIME_IO_LOCAL_FILE_SYSTEM_WITH_FAULT_INJECTION

#include "runtime/io/local-file-system.h"

#include <boost/optional.hpp>
#include <string>

namespace impala {
namespace io {

// The purpose of this class is to override the functions in LocalFileSystem so that
// failure could be injected into them. This is to simulate if a disk I/O function fails.
class LocalFileSystemWithFaultInjection : public LocalFileSystem {
public:
  // Public interface to set the fault injection
  void SetWriteFaultInjection(const std::string& function_name, int err_no);

  virtual ~LocalFileSystemWithFaultInjection() {}

private:
  // Functions with the purpose of injecting faults into the respective functions in
  // LocalFileSystem. Checks DebugFaultInjection() and if it returns true, then these
  // functions return a value that indicates failure on the callsite. As a side effect
  // errno is also set. If DebugFaultInjection() returns false then no fault is injected
  // and the respective function in LocalFileSystem is invoked as it would normally.
  virtual int OpenAux(const char* file, int option1, int option2) override;
  virtual FILE* FdopenAux(int file_desc, const char* options) override;
  virtual int FseekAux(FILE* file_handle, off_t offset, int whence) override;
  virtual size_t FwriteAux(FILE* file_handle, const WriteRange* write_range) override;
  virtual int FcloseAux(FILE* file_handle) override;

  // Used for defining fault injection. This structure represents a function name meant
  // to fail alongside with the desired error code that will be used to populate errno.
  struct WriteFaultInjectionItem {
    WriteFaultInjectionItem(const std::string& function_name, int e)
        : function(function_name), err_no(e) {}
    std::string function;
    int err_no;
  };

  // Setting this member via SetWriteFaultInjection() will inject failure inside
  // LocalFileSystem's functions. The user who sets this member is also responsible for
  // clearing it via ClearWriteFaultInjection().
  boost::optional<WriteFaultInjectionItem> fault_injection_to_write_;

  // Compares 'function_name' to fault_injection_to_write_->first. If they match
  // then sets errno to fault_injection_to_write_->second and returns true. Returns false
  // otherwise.
  bool DebugFaultInjection(const std::string& function_name);
};

}
}
#endif
