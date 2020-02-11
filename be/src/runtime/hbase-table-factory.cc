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

#include "runtime/hbase-table-factory.h"

#include <mutex>

#include "common/status.h"
#include "common/logging.h"
#include "runtime/hbase-table.h"
#include "util/jni-util.h"

#include "common/names.h"

namespace impala {

HBaseTableFactory::HBaseTableFactory() : connection_(NULL), connection_cl_(NULL),
    connection_close_id_(NULL) { }

Status HBaseTableFactory::GetConnection(jobject* connection) {
  lock_guard<mutex> lock(connection_lock_);
  if (connection_ != NULL) {
    *connection = connection_;
    return Status::OK();
  }

  // Get the JNIEnv* corresponding to current thread.
  JNIEnv* env = JniUtil::GetJNIEnv();
  if (env == NULL) return Status("Error creating JNIEnv");
  JniLocalFrame jni_frame;
  RETURN_IF_ERROR(jni_frame.push(env));

  // Get o.a.h.Configuration via HBaseConfiguration. Used to create a connection.
  jclass hbase_conf_cl = env->FindClass("org/apache/hadoop/hbase/HBaseConfiguration");
  RETURN_ERROR_IF_EXC(env);

  jmethodID hbase_conf_create_id = env->GetStaticMethodID(hbase_conf_cl, "create",
          "()Lorg/apache/hadoop/conf/Configuration;");
  RETURN_ERROR_IF_EXC(env);

  // Cleaned up by JniLocalFrame.
  jobject conf = env->CallStaticObjectMethod(hbase_conf_cl, hbase_conf_create_id);
  RETURN_ERROR_IF_EXC(env);

  // Connection related methods.
  connection_cl_ = env->FindClass("org/apache/hadoop/hbase/client/Connection");
  RETURN_ERROR_IF_EXC(env);

  connection_close_id_ = env->GetMethodID(connection_cl_, "close", "()V");
  RETURN_ERROR_IF_EXC(env);

  jclass connection_factory_cl =
    env->FindClass("org/apache/hadoop/hbase/client/ConnectionFactory");
  RETURN_ERROR_IF_EXC(env);

  jmethodID connection_factory_create_connection = env->GetStaticMethodID(
      connection_factory_cl, "createConnection",
      "(Lorg/apache/hadoop/conf/Configuration;)"
      "Lorg/apache/hadoop/hbase/client/Connection;");
  RETURN_ERROR_IF_EXC(env);

  // Cleaned up by JniLocalFrame.
  jobject local_connection = env->CallStaticObjectMethod(connection_factory_cl,
        connection_factory_create_connection, conf);
  RETURN_ERROR_IF_EXC(env);

  RETURN_IF_ERROR(JniUtil::LocalToGlobalRef(env, local_connection, &connection_));
  RETURN_ERROR_IF_EXC(env);

  *connection = connection_;
  return Status::OK();
}

HBaseTableFactory::~HBaseTableFactory() {
  JNIEnv* env = JniUtil::GetJNIEnv();

  // Clean up the global refs and stop the threads.
  lock_guard<mutex> lock(connection_lock_);
  if (connection_ != NULL) {
    env->CallObjectMethod(connection_, connection_close_id_);
    Status s = JniUtil::GetJniExceptionMsg(env);
    // Not much we can do with the error except log it.
    if (!s.ok()) LOG(INFO) << "Exception when cleaning up HBase " << s;
    env->DeleteGlobalRef(connection_);
    connection_ = NULL;
  }
}

Status HBaseTableFactory::GetTable(const string& table_name,
                                   scoped_ptr<HBaseTable>* hbase_table) {
  jobject connection;
  RETURN_IF_ERROR(GetConnection(&connection));
  hbase_table->reset(new HBaseTable(table_name, connection));
  RETURN_IF_ERROR((*hbase_table)->Init());
  return Status::OK();
}

}  // namespace impala
