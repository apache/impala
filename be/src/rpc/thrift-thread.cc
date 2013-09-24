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

#include "rpc/thrift-thread.h"

#include <boost/bind.hpp>
#include <sstream>

using namespace impala;
using namespace boost;
using namespace std;

// Can't import the whole namespace, since impala::Thread will clash with atc::Thread
namespace atc = apache::thrift::concurrency;

void ThriftThread::start() {
  Promise<atc::Thread::id_t> promise;
  impala_thread_.reset(new impala::Thread(group_, name_,
      bind(&ThriftThread::RunRunnable, this, runnable(), &promise)));

  // Blocks until the thread id has been set
  tid_ = promise.Get();
}

atc::Thread::id_t ThriftThread::getId() {
  return tid_;
}

void ThriftThread::join() {
  impala_thread_->Join();
}

shared_ptr<atc::Thread> ThriftThreadFactory::newThread(
    shared_ptr<atc::Runnable> runnable) const {
  stringstream name;
  name << prefix_ << "-" << count_++;
  shared_ptr<ThriftThread> result =
      shared_ptr<ThriftThread>(new ThriftThread(group_, name.str(), runnable));
  runnable->thread(result);
  return result;
}

void ThriftThread::RunRunnable(shared_ptr<atc::Runnable> runnable,
    Promise<atc::Thread::id_t>* promise) {
  promise->Set(get_current());
  // Passing runnable in to this method (rather than reading from this->runnable())
  // ensures that it will live as long as this method, otherwise the ThriftThread could be
  // destroyed between the previous statement and this one (according to my reading of
  // PosixThread)
  runnable->run();
}

atc::Thread::id_t ThriftThreadFactory::getCurrentThreadId() const {
  return atc::Thread::get_current();
}

ThriftThread::ThriftThread(const string& group, const string& name,
    shared_ptr<atc::Runnable> runnable)
    : group_(group), name_(name) {
  // Sets this::runnable (and no, I don't know why it's not protected in atc::Thread)
  this->Thread::runnable(runnable);
}
