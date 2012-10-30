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


#ifndef IMPALA_EXEC_DDL_EXECUTOR_H
#define IMPALA_EXEC_DDL_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/Frontend_types.h"

namespace impala {

class ExecEnv;
class RowBatch;
class Status;
class ImpalaServer;

// The DdlExecutor is responsible for executing statements that modify or query table
// metadata explicitly. These include SHOW and DESCRIBE statements, and may in the future
// include CREATE and ALTER.
// One DdlExecutor is typically created per query statement. 
// Rows are returned in text format, rather than the row batches and tuple descriptors
// returned by query statements.
// All rows are available to be read after Exec() returns.
class DdlExecutor {
 public:
  // Delimiter is the string to use to separate columns values when printed to text.
  DdlExecutor(ImpalaServer* impala_server, const std::string& delimiter);

  // Runs a DDL query to completion. Once Exec() returns, all rows are available to be
  // read in all_rows_ascii()
  Status Exec(TDdlExecRequest* exec_request);

  // Returns the list of rows retrieved in Exec(). Rows are formatted as columnn values
  // printed as text, separated by the class delimiter.
  const std::vector<std::string>& all_rows_ascii() { return ascii_rows_; }

 private:
  // Column separator
  const std::string delimiter_;

  // The list of all materialised rows after Exec() has been called; empty before that. 
  std::vector<std::string> ascii_rows_;

  // Used to execute catalog queries to the Frontend via JNI. Not owned here.
  ImpalaServer* impala_server_;
};

}

#endif
