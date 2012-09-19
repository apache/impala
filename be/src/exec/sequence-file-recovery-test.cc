// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <math.h>
#include <gtest/gtest.h>
#include <boost/assign/list_of.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/unordered_map.hpp>

#include "common/object-pool.h"
#include "runtime/raw-value.h"
#include "runtime/primitive-type.h"
#include "runtime/string-value.h"
#include "testutil/in-process-query-executor.h"
#include "gen-cpp/Exprs_types.h"
#include "exprs/bool-literal.h"
#include "exprs/float-literal.h"
#include "exprs/function-call.h"
#include "exprs/int-literal.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal-predicate.h"
#include "exprs/null-literal.h"
#include "exprs/string-literal.h"
#include "codegen/llvm-codegen.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"

using namespace llvm;
using namespace std;
using namespace boost;
using namespace boost::assign;

DECLARE_int32(read_size);

namespace impala {

class SequenceTest : public testing::Test {

 public:
  virtual void SetUp() {
    // We need to test a sync hash on a block boundary.
    FLAGS_read_size = 10 * 1024;
    exec_env_.reset(new ExecEnv());
    executor_.reset(new InProcessQueryExecutor(exec_env_.get()));
    EXIT_IF_ERROR(executor_->Setup());
  }

  Status RunQuery(string& statement, string* result, string* errors) {
    vector<PrimitiveType> types;
    Status status = executor_->Exec(statement, &types);
    RETURN_IF_ERROR(status);
    status = executor_->FetchResult(result);
    RETURN_IF_ERROR(status);
    string dummy;
    status = executor_->FetchResult(&dummy);
    *errors = executor_->ErrorString();
    return status;
  }
 private:
  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<InProcessQueryExecutor> executor_;
};

const string EXPECTED_ERRORS = "\
Bad sync hash in HdfsSequenceScanner at file offset 899498.\n\
Expected: '6e 91 6 ec be 78 a0 ac 72 10 7e 41 b4 da 93 3c '\n\
Actual:   '6e 91 6 78 78 78 a0 ac 72 10 7e 41 b4 da 93 3c '\n\
Format error in record or block header at offset: 899494\n\
Format error in record or block header at offset: 1784305\n\
Format error in record or block header at offset: 1790543\n\
Format error in record or block header at offset: 1791224\n\
Format error in record or block header at end of file.\n\
First error while processing: hdfs://localhost:20500/test-warehouse/bad_seq_snap/bad_file at offset: 899514\
";

TEST_F(SequenceTest, SyncTest) {
  string result;
  string error;

  string query = "select count(*) from bad_seq_snap";
  Status status = RunQuery(query, &result, &error);

  DCHECK_EQ(result, "9434");
  DCHECK_EQ(error, EXPECTED_ERRORS);
}
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
  impala::LlvmCodeGen::InitializeLlvm();

  return RUN_ALL_TESTS();
}
