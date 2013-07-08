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


#ifndef IMPALA_EXEC_CATALOG_OP_EXECUTOR_H
#define IMPALA_EXEC_CATALOG_OP_EXECUTOR_H

#include <boost/scoped_ptr.hpp>
#include "gen-cpp/Frontend_types.h"

namespace impala {

class Status;

// The CatalogOpExecutor is responsible for executing catalog operations.
// This includes DDL statements such as CREATE and ALTER as well as statements such
// as INVALIDATE METADATA. One CatalogOpExecutor is typically created per catalog
// operation.
class CatalogOpExecutor {
 public:
  CatalogOpExecutor() {}

  // Executes the given catalog operation against the catalog server.
  Status Exec(const TCatalogOpRequest& catalog_op);

  // Set in Exec(), returns a pointer to the TDdlExecResponse of the DDL execution.
  // If called before Exec(), this will return NULL. Only set if the
  // TCatalogOpType is DDL.
  const TDdlExecResponse* ddl_exec_response() const { return exec_response_.get(); }

  // Set in Exec(), for operations that execute using the CatalogServer. Returns
  // a pointer to the TCatalogUpdateResult of the operation. This includes details on
  // the Status of the operation, the CatalogService ID that processed the request,
  // and the minimum catalog version that will reflect this change.
  // If called before Exec(), this will return NULL.
  const TCatalogUpdateResult* update_catalog_result() const {
    return catalog_update_result_.get();
  }

 private:
  // Response from executing the DDL request, see ddl_exec_response().
  boost::scoped_ptr<TDdlExecResponse> exec_response_;

  // Result of executing a DDL request using the CatalogService
  boost::scoped_ptr<TCatalogUpdateResult> catalog_update_result_;
};

}

#endif
