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

#include <mutex>

#include "runtime/io/disk-io-mgr-stress.h"

#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/exec-env.h"
#include "runtime/io/request-context.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;

constexpr float DiskIoMgrStress::ABORT_CHANCE;
const int DiskIoMgrStress::MIN_READ_LEN;
const int DiskIoMgrStress::MAX_READ_LEN;

const int DiskIoMgrStress::MIN_FILE_LEN;
const int DiskIoMgrStress::MAX_FILE_LEN;

// Make sure this is between MIN/MAX FILE_LEN to test more cases
const int DiskIoMgrStress::MIN_READ_BUFFER_SIZE;
const int DiskIoMgrStress::MAX_READ_BUFFER_SIZE;

const int DiskIoMgrStress::MAX_BUFFER_BYTES_PER_SCAN_RANGE;

const int DiskIoMgrStress::CANCEL_READER_PERIOD_MS;

static void CreateTempFile(const char* filename, const char* data) {
  FILE* file = fopen(filename, "w");
  CHECK(file != NULL);
  fwrite(data, 1, strlen(data), file);
  fclose(file);
}

string DiskIoMgrStress::GenerateRandomData() {
  int rand_len = rand() % (MAX_FILE_LEN - MIN_FILE_LEN) + MIN_FILE_LEN;
  stringstream ss;
  for (int i = 0; i < rand_len; ++i) {
    char c = rand() % 26 + 'a';
    ss << c;
  }
  return ss.str();
}

struct DiskIoMgrStress::Client {
  std::mutex lock;
  /// Pool for objects that is cleared when the client is (re-)initialized in NewClient().
  ObjectPool obj_pool;
  unique_ptr<RequestContext> reader;
  int file_idx;
  vector<ScanRange*> scan_ranges;
  int abort_at_byte;
  int files_processed;
};

DiskIoMgrStress::DiskIoMgrStress(int num_disks, int num_threads_per_disk,
     int num_clients, bool includes_cancellation) :
    num_clients_(num_clients),
    includes_cancellation_(includes_cancellation) {

  time_t rand_seed = time(NULL);
  LOG(INFO) << "Running with rand seed: " << rand_seed;
  srand(rand_seed);

  io_mgr_.reset(new DiskIoMgr(num_disks, num_threads_per_disk, num_threads_per_disk,
      MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE));
  Status status = io_mgr_->Init();
  CHECK(status.ok());

  // Initialize some data files.  It doesn't really matter how many there are.
  files_.resize(num_clients * 2);
  for (int i = 0; i < files_.size(); ++i) {
    stringstream ss;
    ss << "/tmp/disk_io_mgr_stress_file" << i;
    files_[i].filename = ss.str();
    files_[i].data = GenerateRandomData();
    CreateTempFile(files_[i].filename.c_str(), files_[i].data.c_str());
  }

  clients_.reset(new Client[num_clients_]);
  client_mem_trackers_.resize(num_clients_);
  buffer_pool_clients_.reset(new BufferPool::ClientHandle[num_clients_]);
  for (int i = 0; i < num_clients_; ++i) {
    NewClient(i);
  }
}

DiskIoMgrStress::~DiskIoMgrStress() { }

void DiskIoMgrStress::ClientThread(int client_id) {
  Client* client = &clients_[client_id];
  Status status;
  char read_buffer[MAX_FILE_LEN];

  while (!shutdown_.Load()) {
    bool eos = false;
    int bytes_read = 0;

    const string& expected = files_[client->file_idx].data;

    while (!eos) {
      ScanRange* range;
      int64_t scan_range_offset = 0;
      bool needs_buffers;
      Status status = client->reader->GetNextUnstartedRange(&range, &needs_buffers);
      CHECK(status.ok() || status.IsCancelled());
      if (range == NULL) break;
      if (needs_buffers) {
        status = io_mgr_->AllocateBuffersForRange(
            &buffer_pool_clients_[client_id], range, MAX_BUFFER_BYTES_PER_SCAN_RANGE);
        CHECK(status.ok()) << status.GetDetail();
      }

      while (true) {
        unique_ptr<BufferDescriptor> buffer;
        status = range->GetNext(&buffer);
        CHECK(status.ok() || status.IsCancelled());
        if (buffer == NULL) break;

        int len = buffer->len();
        CHECK_GE(scan_range_offset, 0);
        CHECK_LT(scan_range_offset, expected.size());
        CHECK_GT(len, 0);

        // We get scan ranges back in arbitrary order so the scan range to the file
        // offset.
        int64_t file_offset = scan_range_offset + range->offset();

        // Validate the bytes read
        CHECK_LE(file_offset + len, expected.size());
        CHECK_EQ(strncmp(reinterpret_cast<char*>(buffer->buffer()),
                     &expected.c_str()[file_offset], len), 0);

        // Copy the bytes from this read into the result buffer.
        memcpy(read_buffer + file_offset, buffer->buffer(), buffer->len());
        range->ReturnBuffer(move(buffer));
        bytes_read += len;
        scan_range_offset += len;

        CHECK_GE(bytes_read, 0);
        CHECK_LE(bytes_read, expected.size());

        if (bytes_read > client->abort_at_byte) {
          eos = true;
          break;
        }
      } // End of buffer
    } // End of scan range

    if (bytes_read == expected.size()) {
      // This entire file was read without being cancelled, validate the entire result
      CHECK(status.ok());
      CHECK_EQ(strncmp(read_buffer, expected.c_str(), bytes_read), 0);
    }

    // Unregister the old client and get a new one
    unique_lock<mutex> lock(client->lock);
    io_mgr_->UnregisterContext(client->reader.get());
    client->reader.reset();
    NewClient(client_id);
  }

  unique_lock<mutex> lock(client->lock);
  io_mgr_->UnregisterContext(client->reader.get());
  client->reader = NULL;
}

// Cancel a random reader
void DiskIoMgrStress::CancelRandomReader() {
  if (!includes_cancellation_) return;
  Client* rand_client = &clients_[rand() % num_clients_];
  unique_lock<mutex> lock(rand_client->lock);
  rand_client->reader->Cancel();
}

void DiskIoMgrStress::Run(int sec) {
  shutdown_.Store(false);
  for (int i = 0; i < num_clients_; ++i) {
    readers_.add_thread(
        new thread(&DiskIoMgrStress::ClientThread, this, i));
  }

  // Sleep and let the clients do their thing for 'sec'
  for (int loop_count = 1; sec == 0 || loop_count <= sec; ++loop_count) {
    int iter = (1000) / CANCEL_READER_PERIOD_MS;
    for (int i = 0; i < iter; ++i) {
      SleepForMs(CANCEL_READER_PERIOD_MS);
      CancelRandomReader();
    }
    LOG(ERROR) << "Finished iteration: " << loop_count;
  }

  // Signal shutdown for the client threads
  shutdown_.Store(true);

  for (int i = 0; i < num_clients_; ++i) {
    unique_lock<mutex> lock(clients_[i].lock);
    if (clients_[i].reader != NULL) clients_[i].reader->Cancel();
  }
  readers_.join_all();

  for (int i = 0; i < num_clients_; ++i) {
    if (clients_[i].reader != nullptr) {
      io_mgr_->UnregisterContext(clients_[i].reader.get());
    }
    ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&buffer_pool_clients_[i]);
    client_mem_trackers_[i]->Close();
  }
  mem_tracker_.Close();
}

// Initialize a client to read one of the files at random.  The scan ranges are
// assigned randomly.
void DiskIoMgrStress::NewClient(int i) {
  Client& client = clients_[i];
  ++client.files_processed;
  client.file_idx = rand() % files_.size();
  int file_len = files_[client.file_idx].data.size();

  client.abort_at_byte = file_len;

  if (includes_cancellation_) {
    float rand_value = rand() / (float)RAND_MAX;
    if (rand_value < ABORT_CHANCE) {
      // Abort at a random byte inside the file
      client.abort_at_byte = rand() % file_len;
    }
  }

  // Clean up leftover state from the previous client (if any).
  client.scan_ranges.clear();
  ExecEnv* exec_env = ExecEnv::GetInstance();
  if (client.reader != nullptr) io_mgr_->UnregisterContext(client.reader.get());

  exec_env->buffer_pool()->DeregisterClient(&buffer_pool_clients_[i]);
  if (client_mem_trackers_[i] != nullptr) client_mem_trackers_[i]->Close();
  client.obj_pool.Clear();

  int assigned_len = 0;
  while (assigned_len < file_len) {
    int range_len = rand() % (MAX_READ_LEN - MIN_READ_LEN) + MIN_READ_LEN;
    range_len = min(range_len, file_len - assigned_len);

    ScanRange* range = client.obj_pool.Add(new ScanRange);
    range->Reset(ScanRange::FileInfo{files_[client.file_idx].filename.c_str()},
        range_len, assigned_len, 0, false, BufferOpts::Uncached());
    client.scan_ranges.push_back(range);
    assigned_len += range_len;
  }

  string client_name = Substitute("Client $0", i);
  client_mem_trackers_[i].reset(new MemTracker(-1, client_name, &mem_tracker_));
  Status status = exec_env->buffer_pool()->RegisterClient(client_name, nullptr,
      exec_env->buffer_reservation(), client_mem_trackers_[i].get(),
      numeric_limits<int64_t>::max(), RuntimeProfile::Create(&client.obj_pool, client_name),
      &buffer_pool_clients_[i]);
  CHECK(status.ok());
  // Reserve enough memory for 3 buffers per range, which should be enough to guarantee
  // progress.
  CHECK(buffer_pool_clients_[i].IncreaseReservationToFit(
      MAX_BUFFER_BYTES_PER_SCAN_RANGE * client.scan_ranges.size()))
      << buffer_pool_clients_[i].DebugString() << "\n"
      << exec_env->buffer_pool()->DebugString() << "\n"
      << exec_env->buffer_reservation()->DebugString();

  client.reader = io_mgr_->RegisterContext();
  status = client.reader->AddScanRanges(client.scan_ranges, EnqueueLocation::TAIL);
  CHECK(status.ok());
}
