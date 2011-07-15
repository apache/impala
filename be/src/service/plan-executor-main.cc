// (c) 2011 Cloudera, Inc. All rights reserved.

#include <jni.h>
#include <Thrift.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TBufferTransports.h>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <vector>
#include <cstdlib>
#include "plan-executor.h"
#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "service/plan-executor.h"
#include "gen-cpp/LocalExecutor_types.h"
#include "gen-cpp/ImpalaService_types.h"

using namespace std;
using namespace boost;
using namespace impala;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

void InitVmArgs(JavaVMInitArgs& vm_args, int numOptions) {
  vm_args.nOptions = numOptions;
  vm_args.options = new JavaVMOption[numOptions];
  vm_args.version = JNI_VERSION_1_6;
  vm_args.ignoreUnrecognized = 0;
}

void AddVmOption(JavaVMInitArgs& vm_args, const string& opt_str, void* extra_info, int* opt_ix) {
  // Warning: This const_cast could be dangerous.
  // We don't know if anybody will try to write optionString (which is not const).
  vm_args.options[*opt_ix].optionString = const_cast<char*>(opt_str.c_str());
  vm_args.options[*opt_ix].extraInfo = extra_info;
  ++(*opt_ix);
}

JNIEnv* CreateVm(JavaVM** jvm) {
    JNIEnv *env;
    JavaVMInitArgs vm_args;

    InitVmArgs(vm_args, 7);
    int opt_ix = 0;

    // Use the environment variable for classpath
    string class_path = "-Djava.class.path=";
    const char* env_class_path = getenv("CLASSPATH");
    class_path.append(env_class_path);
    AddVmOption(vm_args, class_path, NULL, &opt_ix);

    string impala_home = getenv("IMPALA_HOME");
    string impala_fe = impala_home + "/fe";

    string hive_warehouse_dir;
    hive_warehouse_dir.append("-Dtest.hive.warehouse.dir=");
    hive_warehouse_dir.append(impala_fe);
    hive_warehouse_dir.append("/target/test-warehouse");
    AddVmOption(vm_args, hive_warehouse_dir, NULL, &opt_ix);

    string hive_metastore_jdbc_url;
    hive_metastore_jdbc_url.append("-Dtest.hive.metastore.jdbc.url=");
    hive_metastore_jdbc_url.append("jdbc:derby:;databaseName=");
    hive_metastore_jdbc_url.append(impala_fe);
    hive_metastore_jdbc_url.append("/target/test_metastore_db;");
    hive_metastore_jdbc_url.append("create=true;");
    hive_metastore_jdbc_url.append("logDevice=");
    hive_metastore_jdbc_url.append(impala_fe);
    hive_metastore_jdbc_url.append("/target/test_metastore_db;");
    AddVmOption(vm_args, hive_metastore_jdbc_url, NULL, &opt_ix);

    string hive_metastore_jdbc_driver;
    hive_metastore_jdbc_driver.append("-Dtest.hive.metastore.jdbc.driver=org.apache.derby.jdbc.EmbeddedDriver");
    AddVmOption(vm_args, hive_metastore_jdbc_driver, NULL, &opt_ix);

    string hive_metastore_jdbc_username;
    hive_metastore_jdbc_username.append("-Dtest.hive.metastore.jdbc.username=APP");
    AddVmOption(vm_args, hive_metastore_jdbc_username, NULL, &opt_ix);

    string hive_metastore_jdbc_password;
    hive_metastore_jdbc_password.append("-Dtest.hive.metastore.jdbc.password=mine");
    AddVmOption(vm_args, hive_metastore_jdbc_password, NULL, &opt_ix);

    string jni_check = "-Xcheck:jni";
    AddVmOption(vm_args, jni_check, NULL, &opt_ix);

    int ret = JNI_CreateJavaVM(jvm, (void**)&env, &vm_args);
    if (ret < 0) {
      cerr << "Fatal error: Failed to load JVM." << endl;
    }
    return env;
}

bool JavaExceptionOccurred(JNIEnv* env) {
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    return true;
  }
  return false;
}

// Execute given plan, printing the evaluation of select_list_exprs via cout.
// Returns Status::OK if all results have been fetched successfully.
// Returns error message upon failure.
Status ExecutePlan(PlanExecutor* executor, const vector<Expr*>& select_list_exprs, bool as_ascii, int* total_rows) {
  Status status;
  RETURN_IF_ERROR(executor->Exec());
  scoped_ptr<RowBatch> batch;
  while (true) {
    RowBatch* batch_ptr;
    RETURN_IF_ERROR(executor->FetchResult(&batch_ptr));
    batch.reset(batch_ptr);
    if (batch == NULL) {
      return Status("Row batch is NULL.");
    }
    for (int i = 0; i < batch->num_rows(); ++i) {
      TupleRow* row = batch->GetRow(i);
      for (int j = 0; j < select_list_exprs.size(); ++j) {
        TColumnValue col_val;
        select_list_exprs[j]->GetValue(row, as_ascii, &col_val);
        cout << col_val.stringVal << ",";
      }
      cout << endl;
    }
    *total_rows += batch->num_rows();
    if (batch->num_rows() < batch->capacity()) {
      break;
    }
  }

  return Status::OK;
}

int main(int argc, const char* argv[]) {
  if (argc != 2) {
    cerr << "coordinator \"query string\"" << endl;
    return 1;
  }

  JavaVM* jvm;
  JNIEnv* env = CreateVm(&jvm);
  if (env == NULL) {
    return 1;
  }

  // Create java TQueryRequest.
  jstring query_jstr = env->NewStringUTF(argv[1]);
  jboolean as_ascii = true;

  jclass coordinator_cl = env->FindClass("com/cloudera/impala/service/Coordinator");
  if (JavaExceptionOccurred(env)) return 1;
  jmethodID coordinator_get_plan_bytes = env->GetStaticMethodID(coordinator_cl, "getThriftPlan", "(Ljava/lang/String;)[B");
  if (JavaExceptionOccurred(env)) return 1;
  jbyteArray plan_bytes = (jbyteArray) env->CallStaticObjectMethod(coordinator_cl, coordinator_get_plan_bytes, query_jstr);
  if (JavaExceptionOccurred(env)) return 1;

  cout << "Deserializing plan." << endl;
  ExecNode* plan;
  DescriptorTbl* descs;
  vector<Expr*> select_list_exprs;
  Status status;
  ObjectPool obj_pool;
  DeserializeRequest(env, plan_bytes, &obj_pool, &plan, &descs, &select_list_exprs);

  // Prepare select list expressions.
  PlanExecutor executor(plan, *descs);
  for (int i = 0; i < select_list_exprs.size(); ++i) {
    select_list_exprs[i]->Prepare(executor.runtime_state());
  }

  cout << "Executing plan" << endl;
  int total_rows = 0;
  status = ExecutePlan(&executor, select_list_exprs, as_ascii, &total_rows);
  cout << "TOTAL ROWS: " << total_rows << endl;

  int n = jvm->DestroyJavaVM();
  return !status.ok();
}
