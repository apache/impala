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

#include "runtime/disk-io-mgr-stress.h"

#include "util/time.h"

#include "common/names.h"

using namespace impala;

static const float ABORT_CHANCE = .10f;
static const int MIN_READ_LEN = 1;
static const int MAX_READ_LEN = 20;

static const int MIN_FILE_LEN = 10;
static const int MAX_FILE_LEN = 1024;

// Make sure this is between MIN/MAX FILE_LEN to test more cases
static const int MIN_READ_BUFFER_SIZE = 64;
static const int MAX_READ_BUFFER_SIZE = 128;

static const int CANCEL_READER_PERIOD_MS = 20;  // in ms

static void CreateTempFile(const char* filename, const char* data) {
  FILE* file = fopen(filename, "w");
  CHECK(file != NULL);
  fwrite(data, 1, strlen(data), file);
  fclose(file);
}

string GenerateRandomData() {
  int rand_len = rand() % (MAX_FILE_LEN - MIN_FILE_LEN) + MIN_FILE_LEN;
  stringstream ss;
  for (int i = 0; i < rand_len; ++i) {
    char c = rand() % 26 + 'a';
    ss << c;
  }
  return ss.str();
}

struct DiskIoMgrStress::Client {
  boost::mutex lock;
  DiskIoMgr::RequestContext* reader;
  int file_idx;
  vector<DiskIoMgr::ScanRange*> scan_ranges;
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

  io_mgr_.reset(new DiskIoMgr(
      num_disks, num_threads_per_disk, MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE));
  Status status = io_mgr_->Init(&dummy_tracker_);
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

  clients_ = new Client[num_clients_];
  for (int i = 0; i < num_clients_; ++i) {
    NewClient(i);
  }
}

void DiskIoMgrStress::ClientThread(int client_id) {
  Client* client = &clients_[client_id];
  Status status;
  char read_buffer[MAX_FILE_LEN];

  while (!shutdown_) {
    bool eos = false;
    int bytes_read = 0;

    const string& expected = files_[client->file_idx].data;

    while (!eos) {
      DiskIoMgr::ScanRange* range;
      Status status = io_mgr_->GetNextRange(client->reader, &range);
      CHECK(status.ok() || status.IsCancelled());
      if (range == NULL) break;

      while (true) {
        DiskIoMgr::BufferDescriptor* buffer;
        status = range->GetNext(&buffer);
        CHECK(status.ok() || status.IsCancelled());
        if (buffer == NULL) break;

        int64_t scan_range_offset = buffer->scan_range_offset();
        int len = buffer->len();
        CHECK_GE(scan_range_offset, 0);
        CHECK_LT(scan_range_offset, expected.size());
        CHECK_GT(len, 0);

        // We get scan ranges back in arbitrary order so the scan range to the file
        // offset.
        int64_t file_offset = scan_range_offset + range->offset();

        // Validate the bytes read
        CHECK_LE(file_offset + len, expected.size());
        CHECK_EQ(strncmp(buffer->buffer(), &expected.c_str()[file_offset], len), 0);

        // Copy the bytes from this read into the result buffer.
        memcpy(read_buffer + file_offset, buffer->buffer(), buffer->len());
        buffer->Return();
        buffer = NULL;
        bytes_read += len;

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
    io_mgr_->UnregisterContext(client->reader);
    NewClient(client_id);
  }

  unique_lock<mutex> lock(client->lock);
  io_mgr_->UnregisterContext(client->reader);
  client->reader = NULL;
}

// Cancel a random reader
void DiskIoMgrStress::CancelRandomReader() {
  if (!includes_cancellation_) return;

  int rand_client = rand() % num_clients_;

  unique_lock<mutex> lock(clients_[rand_client].lock);
  io_mgr_->CancelContext(clients_[rand_client].reader);
}

void DiskIoMgrStress::Run(int sec) {
  shutdown_ = false;
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
  shutdown_ = true;

  for (int i = 0; i < num_clients_; ++i) {
    unique_lock<mutex> lock(clients_[i].lock);
    if (clients_[i].reader != NULL) io_mgr_->CancelContext(clients_[i].reader);
  }

  readers_.join_all();
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

  for (int i = 0; i < client.scan_ranges.size(); ++i) {
    delete client.scan_ranges[i];
  }
  client.scan_ranges.clear();

  int assigned_len = 0;
  while (assigned_len < file_len) {
    int range_len = rand() % (MAX_READ_LEN - MIN_READ_LEN) + MIN_READ_LEN;
    range_len = min(range_len, file_len - assigned_len);

    DiskIoMgr::ScanRange* range = new DiskIoMgr::ScanRange();;
    range->Reset(NULL, files_[client.file_idx].filename.c_str(), range_len,
        assigned_len, 0, false, false, DiskIoMgr::ScanRange::NEVER_CACHE);
    client.scan_ranges.push_back(range);
    assigned_len += range_len;
  }
  Status status = io_mgr_->RegisterContext(&client.reader, NULL);
  CHECK(status.ok());
  status = io_mgr_->AddScanRanges(client.reader, client.scan_ranges);
  CHECK(status.ok());
}
