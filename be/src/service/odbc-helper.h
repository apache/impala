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

#pragma once

#include <string>
#include <memory>

#include "gen-cpp/TCLIService_types.h"
#include "service/impala-server.h"

namespace impala {

/// ODBC reserved keywords as per ISO/IEF CLI specification and ODBC standard.
/// From https://docs.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql#odbc-reserved-keywords
extern const std::string ODBC_KEYWORDS;

/// Populates the TGetInfoResp structure based on the requested info type.
/// This function handles the ODBC GetInfo metadata calls for HiveServer2.
///
/// Parameters:
///   return_val - The response structure to be populated
///   info_type - The type of information being requested
///   session - The session state (may be nullptr for session-independent queries)
///   sqlstate_optional_feature_not_implemented - SQLSTATE code for unsupported features
///
/// The function sets the infoValue field of return_val and may also set error status
/// for unsupported info types.
void PopulateOdbcGetInfo(
    apache::hive::service::cli::thrift::TGetInfoResp& return_val,
    apache::hive::service::cli::thrift::TGetInfoType::type info_type,
    const std::shared_ptr<ImpalaServer::SessionState>& session,
    const char* sqlstate_optional_feature_not_implemented);

}
