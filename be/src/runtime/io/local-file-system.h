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

#ifndef IMPALA_RUNTIME_IO_LOCAL_FILE_SYSTEM_H
#define IMPALA_RUNTIME_IO_LOCAL_FILE_SYSTEM_H

#include "common/status.h"

namespace impala {
namespace io {

class WriteRange;

// This class introduces wrapper functions around open(), fdopen(), fseek(), fwrite() and
// fclose() disk I/O functions. Also includes the error checking logic in these wrapper
// functions and converts the outcome of the I/O operations to a Status object.
class LocalFileSystem {
public:
  // A wrapper around open() and fdopen(). For the possible values of oflag and mode
  // see the documentation of open(). Sets 'file' to a FILE* returned from fdopen().
 Status Open(
     const char* file_name, int oflag, int mode, const char* fd_option, FILE** file);
 Status OpenForRead(const char* file_name, int oflag, int mode, FILE** file);
 Status OpenForWrite(const char* file_name, int oflag, int mode, FILE** file);

 Status Fseek(FILE* file_handle, off_t offset, int whence, const WriteRange* write_range);
 Status Fwrite(FILE* file_handle, const WriteRange* write_range);
 Status Fread(FILE* file_handle, uint8_t* buffer, int64_t length, const char* file_path);
 Status Fclose(FILE* file_handle, const char* file_path);
 virtual ~LocalFileSystem() {}

 // Wrapper function to use write() to write the bytes.
 Status Write(int file_desc, const WriteRange* range);

protected:
  // Wrapper functions around open(), fdopen(), fseek(), fwrite() and fclose().
  // Introduced so that fault injection can be implemented through inheritance.
  virtual int OpenAux(const char* file, int option1, int option2);
  virtual FILE* FdopenAux(int file_desc, const char* options);
  virtual int FseekAux(FILE* file_handle, off_t offset, int whence);
  virtual size_t FwriteAux(FILE* file_handle, const WriteRange* write_range);
  virtual size_t FreadAux(FILE* file_handle, uint8_t* buffer, int64_t length);
  virtual int FcloseAux(FILE* file_handle);
};

}
}
#endif
