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

#include "util/thread.h"

#include <set>
#include <map>
#include <boost/foreach.hpp>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "util/metrics.h"
#include "util/webserver.h"
#include "util/url-coding.h"
#include "util/proc-util.h"

using namespace boost;
using namespace std;

namespace impala {

class ThreadMgr;

// Singleton instance of ThreadMgr. Only visible in this file, used only by Thread.
// The Thread class adds a reference to thread_manager while it is supervising a thread so
// that a race between the end of the process's main thread (and therefore the destruction
// of thread_manager) and the end of a thread that tries to remove itself from the
// manager after the destruction can be avoided.
shared_ptr<ThreadMgr> thread_manager;

// A singleton class that tracks all live threads, and groups them together for easy
// auditing. Used only by Thread.
class ThreadMgr {
 public:
  ThreadMgr() : metrics_enabled_(false) { }

  Status StartInstrumentation(Metrics* metrics, Webserver* webserver);

  // Registers a thread to the supplied category. The key is a boost::thread::id, used
  // instead of the system TID since boost::thread::id is always available, unlike
  // gettid() which might fail.
  void AddThread(const thread::id& thread, const string& name, const string& category,
      int64_t tid);

  // Removes a thread from the supplied category. If the thread has
  // already been removed, this is a no-op.
  void RemoveThread(const thread::id& boost_id, const string& category);

 private:
  // Container class for any details we want to capture about a thread
  // TODO: Add start-time.
  // TODO: Track fragment ID.
  class ThreadDescriptor {
   public:
    ThreadDescriptor() { }
    ThreadDescriptor(const string& category, const string& name, int64_t thread_id)
        : name_(name), category_(category), thread_id_(thread_id) {
    }

    const string& name() const { return name_; }
    const string& category() const { return category_; }
    int64_t thread_id() const { return thread_id_; }

   private:
    string name_;
    string category_;
    int64_t thread_id_;
  };

  // A ThreadCategory is a set of threads that are logically related.
  // TODO: unordered_map is incompatible with boost::thread::id, but would be more
  // efficient here.
  typedef map<const thread::id, ThreadDescriptor> ThreadCategory;

  // All thread categorys, keyed on the category name.
  typedef map<string, ThreadCategory> ThreadCategoryMap;

  // Protects thread_categories_ and metrics_enabled_
  mutex lock_;

  // All thread categorys that ever contained a thread, even if empty
  ThreadCategoryMap thread_categories_;

  // True after StartInstrumentation(..) returns
  bool metrics_enabled_;

  // Metrics to track all-time total number of threads, and the
  // current number of running threads.
  Metrics::IntMetric* total_threads_metric_;
  Metrics::IntMetric* current_num_threads_metric_;

  // Webpage callback; prints all threads by category
  void ThreadPathHandler(const Webserver::ArgumentMap& args, stringstream* output);
  void PrintThreadCategoryRows(const ThreadCategory& category, stringstream* output);
};

Status ThreadMgr::StartInstrumentation(Metrics* metrics, Webserver* webserver) {
  DCHECK(metrics != NULL);
  DCHECK(webserver != NULL);
  lock_guard<mutex> l(lock_);
  metrics_enabled_ = true;
  total_threads_metric_ = metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
      "thread-manager.total-threads-created", 0L);
  current_num_threads_metric_ = metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
      "thread-manager.running-threads", 0L);

  Webserver::PathHandlerCallback thread_callback =
      bind<void>(mem_fn(&ThreadMgr::ThreadPathHandler), this, _1, _2);
  webserver->RegisterPathHandler("/threadz", thread_callback);
  return Status::OK;
}

void ThreadMgr::AddThread(const thread::id& thread, const string& name,
    const string& category, int64_t tid) {
  lock_guard<mutex> l(lock_);
  thread_categories_[category][thread] = ThreadDescriptor(category, name, tid);
  if (metrics_enabled_) {
    current_num_threads_metric_->Increment(1L);
    total_threads_metric_->Increment(1L);
  }
}

void ThreadMgr::RemoveThread(const thread::id& boost_id, const string& category) {
  lock_guard<mutex> l(lock_);
  ThreadCategoryMap::iterator category_it = thread_categories_.find(category);
  DCHECK(category_it != thread_categories_.end());
  category_it->second.erase(boost_id);
  if (metrics_enabled_) current_num_threads_metric_->Increment(-1L);
}

void ThreadMgr::PrintThreadCategoryRows(const ThreadCategory& category,
    stringstream* output) {
  BOOST_FOREACH(const ThreadCategory::value_type& thread, category) {
    ThreadStats stats;
    Status status = GetThreadStats(thread.second.thread_id(), &stats);
    if (!status.ok()) {
      LOG_EVERY_N(INFO, 100) << "Could not get per-thread statistics: "
                             << status.GetErrorMsg();
    }
    (*output) << "<tr><td>" << thread.second.name() << "</td><td>"
              << (static_cast<double>(stats.user_ns) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.kernel_ns) / 1e9) << "</td><td>"
              << (static_cast<double>(stats.iowait_ns) / 1e9) << "</td></tr>";
  }
}

void ThreadMgr::ThreadPathHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  lock_guard<mutex> l(lock_);
  vector<const ThreadCategory*> categories_to_print;
  Webserver::ArgumentMap::const_iterator category_name = args.find("group");
  if (category_name != args.end()) {
    (*output) << "<h2>Thread Group: " << category_name->second << "</h2>" << endl;
    if (category_name->second != "all") {
      ThreadCategoryMap::const_iterator category =
          thread_categories_.find(category_name->second);
      if (category == thread_categories_.end()) {
        (*output) << "Thread group '" << category_name->second << "' not found" << endl;
        return;
      }
      categories_to_print.push_back(&category->second);
      (*output) << "<h3>" << category->first << " : " << category->second.size()
                << "</h3>";
    } else {
      BOOST_FOREACH(const ThreadCategoryMap::value_type& category, thread_categories_) {
        categories_to_print.push_back(&category.second);
      }
      (*output) << "<h3>All Threads : </h3>";
    }

    (*output) << "<table class='table table-hover table-border'>";
    (*output) << "<tr><th>Thread name</th><th>Cumulative User CPU(s)</th>"
              << "<th>Cumulative Kernel CPU(s)</th>"
              << "<th>Cumulative IO-wait(s)</th></tr>";

    BOOST_FOREACH(const ThreadCategory* category, categories_to_print) {
      PrintThreadCategoryRows(*category, output);
    }
    (*output) << "</table>";
  } else {
    (*output) << "<h2>Thread Groups</h2>";
    if (metrics_enabled_) {
      (*output) << "<h4>" << current_num_threads_metric_->value() << " thread(s) running";
    }
    (*output) << "<a href='/threadz?group=all'><h3>All Threads</h3>";

    BOOST_FOREACH(const ThreadCategoryMap::value_type& category, thread_categories_) {
      string category_arg;
      UrlEncode(category.first, &category_arg);
      (*output) << "<a href='/threadz?group=" << category_arg << "'><h3>"
                << category.first << " : " << category.second.size() << "</h3></a>";
    }
  }
}

void InitThreading() {
  DCHECK(thread_manager.get() == NULL);
  thread_manager.reset(new ThreadMgr());
}

Status StartThreadInstrumentation(Metrics* metrics, Webserver* webserver) {
  return thread_manager->StartInstrumentation(metrics, webserver);
}

void Thread::StartThread(const ThreadFunctor& functor) {
  DCHECK(thread_manager.get() != NULL)
      << "Thread created before InitThreading called";
  DCHECK(tid_ == UNINITIALISED_THREAD_ID) << "StartThread called twice";

  Promise<int64_t> thread_started;
  thread_.reset(
      new thread(&Thread::SuperviseThread, name_, category_, functor, &thread_started));

  // TODO: This slows down thread creation although not enormously. To make this faster,
  // consider delaying thread_started.Get() until the first call to tid(), but bear in
  // mind that some coordination is required between SuperviseThread() and this to make
  // sure that the thread is still available to have its tid set.
  tid_ = thread_started.Get();

  VLOG(2) << "Started thread " << tid_ << " - " << category_ << ":" << name_;
}

void Thread::SuperviseThread(const string& name, const string& category,
    Thread::ThreadFunctor functor, Promise<int64_t>* thread_started) {
  int64_t system_tid = syscall(SYS_gettid);
  if (system_tid == -1) {
    LOG_EVERY_N(INFO, 100) << "Could not determine thread ID: "
                           << string(strerror(errno));
  }
  // Make a copy, since we want to refer to these variables after the unsafe point below.
  string category_copy = category;
  shared_ptr<ThreadMgr> thread_mgr_ref = thread_manager;
  stringstream ss;
  ss << (name.empty() ? "thread" : name) << "-" << system_tid;
  string name_copy = ss.str();

  if (category_copy.empty()) category_copy = "no-category";

  // Use boost's get_id rather than the system thread ID as the unique key for this thread
  // since the latter is more prone to being recycled.
  thread_mgr_ref->AddThread(this_thread::get_id(), name_copy, category_copy, system_tid);
  thread_started->Set(system_tid);

  // Any reference to any parameter not copied in by value may no longer be valid after
  // this point, since the caller that is waiting on *tid != 0 may wake, take the lock and
  // destroy the enclosing Thread object.

  functor();
  thread_mgr_ref->RemoveThread(this_thread::get_id(), category_copy);
}

void ThreadGroup::AddThread(Thread* thread) {
  threads_.push_back(thread);
}

void ThreadGroup::JoinAll() {
  BOOST_FOREACH(const Thread& thread, threads_) {
    thread.Join();
  }
}

}
