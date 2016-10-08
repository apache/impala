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

#ifndef IMPALA_EXEC_EXTERNAL_DATA_SOURCE_EXECUTOR_H
#define IMPALA_EXEC_EXTERNAL_DATA_SOURCE_EXECUTOR_H

#include <jni.h>
#include <string>

#include "common/status.h"

#include "gen-cpp/ExternalDataSource_types.h"

namespace impala {

class MetricGroup;

/// Wraps the Java class ExternalDataSourceExecutor to call a data source.
/// There is an explicit Init() method (rather than initializing in the c'tor) so
/// that the initialization can return an error status if an error occurs.
class ExternalDataSourceExecutor {
 public:
  ExternalDataSourceExecutor() : is_initialized_(false), executor_(NULL) { }

  /// Initialize static JNI state. Called on process startup.
  static Status InitJNI(MetricGroup* metrics);

  virtual ~ExternalDataSourceExecutor();

  /// Initialize the data source library. jar_path is the HDFS location of the jar
  /// containing the ExternalDataSource implementation specified by class_name. The
  /// class must implement the specified api_version.
  Status Init(const std::string& jar_path, const std::string& class_name,
      const std::string& api_version, const std::string& init_string);

  /// Calls ExternalDataSource.open()
  Status Open(const impala::extdatasource::TOpenParams& params,
      impala::extdatasource::TOpenResult* result);

  /// Calls ExternalDataSource.getNext()
  Status GetNext(const impala::extdatasource::TGetNextParams& params,
      impala::extdatasource::TGetNextResult* result);

  /// Calls ExternalDataSource.close() and deletes the reference to the
  /// external_data_source_executor_. After calling Close(), this should no
  /// longer be used.
  Status Close(const impala::extdatasource::TCloseParams& params,
      impala::extdatasource::TCloseResult* result);

 private:
  class JniState;

  bool is_initialized_; // Set true in Init() to ensure the class is initialized.

  /// Instance of org.apache.impala.extdatasource.ExternalDataSourceExecutor
  jobject executor_;
};

}

#endif
