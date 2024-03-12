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

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "exec/tuple-file-reader.h"
#include "exec/tuple-file-writer.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/test-env.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "service/frontend.h"
#include "testutil/desc-tbl-builder.h"
#include "util/filesystem-util.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;

namespace impala {

class TupleFileReadWriteTest : public ::testing::Test {
public:
  TupleFileReadWriteTest()
    : mem_pool_(&tracker_), fe_(new Frontend()), test_env_(new TestEnv) {}

  ~TupleFileReadWriteTest() {
    mem_pool_.FreeAll();
  }

  void SetUp() override {
    tmp_dir_ = "/tmp" / boost::filesystem::unique_path();
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(tmp_dir_.string()));

    test_env_->SetBufferPoolArgs(1, 1024);
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    MemTracker* client_tracker =
        pool_.Add(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    ASSERT_OK(test_env_->exec_env()->buffer_pool()->RegisterClient(
        "TupleFileReadWriteTest", nullptr, runtime_state_->instance_buffer_reservation(),
        client_tracker, std::numeric_limits<int64>::max(),
        runtime_state_->runtime_profile(), &buffer_pool_client_));

    // Create nullable row descriptor
    DescriptorTblBuilder builder(fe_.get(), &pool_);
    builder.DeclareTuple() << TYPE_INT;
    DescriptorTbl* desc_tbl = builder.Build();

    vector<bool> nullable_tuples = {false};
    vector<TTupleId> tuple_id = {static_cast<TupleId>(0)};
    nullable_row_desc_ = RowDescriptor{*desc_tbl, tuple_id, nullable_tuples};
  }

  void TearDown() override {
    if (buffer_pool_client_.is_registered()) {
      test_env_->exec_env()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
    }
    ASSERT_OK(FileSystemUtil::RemovePaths({tmp_dir_.string()}));
  }

  string Path(string filename) const {
    return (tmp_dir_ / filename).string();
  }

  string TempPath(const TupleFileWriter& writer) const {
    return writer.TempPath();
  }

  unique_ptr<RowBatch> CreateBatch(int size = 10) {
    unique_ptr<RowBatch> batch =
        make_unique<RowBatch>(nullable_row_desc(), size, tracker());
    batch->AddRows(size);
    for (int i = 0; i < size; i++) {
      TupleRow* row = batch->GetRow(i);
      Tuple* tuple_mem = Tuple::Create(sizeof(char) + sizeof(int32_t), mem_pool());
      *reinterpret_cast<int32_t *>(tuple_mem->GetSlot(1)) = i;
      tuple_mem->SetNotNull(NullIndicatorOffset(0, 1));
      row->SetTuple(0, tuple_mem);
    }
    batch->CommitRows(size);
    return batch;
  }

  MemTracker* tracker() { return &tracker_; }

  MemPool* mem_pool() { return &mem_pool_; }

  RowDescriptor* nullable_row_desc() { return &nullable_row_desc_; }

  RuntimeState* runtime_state() { return runtime_state_; }

  RuntimeProfile* profile() { return runtime_state_->runtime_profile(); }

  BufferPool::ClientHandle* buffer_pool_client() { return &buffer_pool_client_; }

private:
  MemTracker tracker_;
  MemPool mem_pool_;
  RowDescriptor nullable_row_desc_;

  ObjectPool pool_;
  scoped_ptr<Frontend> fe_;

  // The temporary runtime environment used for the test.
  boost::scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
  BufferPool::ClientHandle buffer_pool_client_;
  filesystem::path tmp_dir_;
};

TEST_F(TupleFileReadWriteTest, TestBadRead) {
  TupleFileReader reader(Path("no-file"), tracker(), profile());
  EXPECT_FALSE(reader.Open(runtime_state()).ok());
}

TEST_F(TupleFileReadWriteTest, TestWriteRead) {
  string path = Path("a-file");
  filesystem::remove(path);
  TupleFileWriter writer(path, tracker(), profile());

  Status status = writer.Open(runtime_state());
  ASSERT_OK(status);

  unique_ptr<RowBatch> batch = CreateBatch();
  status = writer.Write(runtime_state(), batch.get());
  ASSERT_OK(status);

  // Reading before commit will fail.
  TupleFileReader failed_reader(path, tracker(), profile());
  EXPECT_FALSE(failed_reader.Open(runtime_state()).ok());

  status = writer.Commit(runtime_state());
  ASSERT_OK(status);
  ASSERT_FALSE(filesystem::exists(TempPath(writer)));
  ASSERT_TRUE(filesystem::exists(path));

  TupleFileReader reader(path, tracker(), profile());
  EXPECT_OK(reader.Open(runtime_state()));

  RowBatch output_row_batch(nullable_row_desc(), 10, tracker());
  bool eos = false;
  status =
      reader.GetNext(runtime_state(), buffer_pool_client(), &output_row_batch, &eos);
  EXPECT_OK(status);
  EXPECT_TRUE(eos);
  EXPECT_EQ(batch->num_rows(), output_row_batch.num_rows());
}

TEST_F(TupleFileReadWriteTest, TestEmptyWrite) {
  string path = Path("empty-file");
  filesystem::remove(path);
  TupleFileWriter writer(path, tracker(), profile());

  Status status = writer.Open(runtime_state());
  ASSERT_OK(status);

  RowBatch batch(nullable_row_desc(), 1, tracker());
  status = writer.Write(runtime_state(), &batch);
  ASSERT_OK(status);
  status = writer.Commit(runtime_state());
  ASSERT_OK(status);
  ASSERT_TRUE(filesystem::exists(path));

  // Read back the empty file and make sure the reader can handle it
  TupleFileReader reader(path, tracker(), profile());
  EXPECT_OK(reader.Open(runtime_state()));

  RowBatch output_row_batch(nullable_row_desc(), 1, tracker());
  bool eos = false;
  status =
      reader.GetNext(runtime_state(), buffer_pool_client(), &output_row_batch, &eos);
  EXPECT_OK(status);
  EXPECT_TRUE(eos);
  EXPECT_EQ(batch.num_rows(), output_row_batch.num_rows());
}

TEST_F(TupleFileReadWriteTest, TestDeletedFile) {
  string path = Path("missing-file");
  filesystem::remove(path);
  TupleFileWriter writer(path, tracker(), profile());

  Status status = writer.Open(runtime_state());
  ASSERT_OK(status);

  unique_ptr<RowBatch> batch = CreateBatch();
  status = writer.Write(runtime_state(), batch.get());
  ASSERT_OK(status);

  filesystem::remove(TempPath(writer));
  status = writer.Commit(runtime_state());
  ASSERT_FALSE(status.ok());
  ASSERT_FALSE(filesystem::exists(TempPath(writer)));
  ASSERT_FALSE(filesystem::exists(path));

  // Even if the writer is in a bad state, it can still call Abort().
  writer.Abort();
}

TEST_F(TupleFileReadWriteTest, TestDestructorNoOpen) {
  // Simple test to make sure we can destroy a TupleFileWriter that we never opened.
  string path = Path("no-open-file");
  filesystem::remove(path);
  TupleFileWriter writer(path, tracker(), profile());
}

TEST_F(TupleFileReadWriteTest, TestDestructorAbort) {
  // If neither Abort() nor Commit() are called explicitly, then the destructor for
  // TupleFileWriter will run Abort().
  string path = Path("destructor-file");
  filesystem::remove(path);
  unique_ptr<TupleFileWriter> writer =
    make_unique<TupleFileWriter>(path, tracker(), profile());

  Status status = writer->Open(runtime_state());
  ASSERT_OK(status);

  string temp_path = TempPath(*writer);
  ASSERT_TRUE(filesystem::exists(temp_path));

  writer.reset();
  ASSERT_FALSE(filesystem::exists(temp_path));
  ASSERT_FALSE(filesystem::exists(path));
}

TEST_F(TupleFileReadWriteTest, TestExceedMaxFileSize) {
  string path = Path("exceed-max-size-file");
  filesystem::remove(path);
  // Limit the file to 20 bytes
  size_t max_size = 20;

  TupleFileWriter writer(path, tracker(), profile(), max_size);

  Status status = writer.Open(runtime_state());
  ASSERT_OK(status);

  unique_ptr<RowBatch> batch = CreateBatch();
  status = writer.Write(runtime_state(), batch.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find("exceed the maximum file size") != -1);
}

TEST_F(TupleFileReadWriteTest, TestExactMaxFileSize) {
  // This tests that reaching exactly equal to the max file size is allowed.

  // Run without a limit first to get the exact size that we will write.
  string path = Path("no-limit-file");
  filesystem::remove(path);
  TupleFileWriter writer(path, tracker(), profile());

  Status status = writer.Open(runtime_state());
  ASSERT_OK(status);

  unique_ptr<RowBatch> batch = CreateBatch();
  status = writer.Write(runtime_state(), batch.get());
  ASSERT_OK(status);

  // Store the number of bytes written
  size_t max_size = writer.BytesWritten();
  writer.Abort();

  // Now, run the same thing with the max size set to the number of bytes written.
  string path2 = Path("exact-max-size-file");
  filesystem::remove(path2);
  TupleFileWriter limited_writer(path2, tracker(), profile(), max_size);

  status = limited_writer.Open(runtime_state());
  ASSERT_OK(status);

  status = limited_writer.Write(runtime_state(), batch.get());
  ASSERT_OK(status);

  // We reached exactly the max size, and everything is ok.
  EXPECT_EQ(limited_writer.BytesWritten(), max_size);
  status = limited_writer.Commit(runtime_state());
  ASSERT_OK(status);
}

} // namespace impala
