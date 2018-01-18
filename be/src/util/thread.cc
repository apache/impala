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

#include "util/thread.h"

#include <set>
#include <map>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "common/thread-debug-info.h"
#include "util/coding-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/jni-util.h"
#include "util/metrics.h"
#include "util/webserver.h"
#include "util/os-util.h"

#include "common/names.h"

#ifndef NDEBUG
DECLARE_bool(thread_creation_fault_injection);
#endif

namespace this_thread = boost::this_thread;
using namespace rapidjson;

namespace impala {

static const string JVM_THREADS_WEB_PAGE = "/jvm-threadz";
static const string JVM_THREADS_TEMPLATE = "jvm-threadz.tmpl";
static const string THREADS_WEB_PAGE = "/threadz";
static const string THREADS_TEMPLATE = "threadz.tmpl";
static const string THREAD_GROUP_WEB_PAGE = "/thread-group";
static const string THREAD_GROUP_TEMPLATE = "/thread-group.tmpl";

class ThreadMgr;

// Singleton instance of ThreadMgr. Only visible in this file, used only by Thread.
// The Thread class adds a reference to thread_manager while it is supervising a thread so
// that a race between the end of the process's main thread (and therefore the destruction
// of thread_manager) and the end of a thread that tries to remove itself from the
// manager after the destruction can be avoided.
shared_ptr<ThreadMgr> thread_manager;

namespace {

// Example output:
// "overview" : {
//   "thread_count" : 30,
//   "daemon_count" : 4,
//   "peak_count" : 40
// }
// "threads": [
//   {
//     "summary" : "main ID:1 RUNNABLE",
//     "cpu_time_sec" : 1.303,
//     "user_time_sec" : 2.323,
//     "blocked_time_ms" : -1,
//     "blocked_count" : 20,
//     "is_native" : false
//   },
//   { ... }
// ]
void JvmThreadsUrlCallback(const Webserver::ArgumentMap& args, Document* doc);

void ThreadOverviewUrlCallback(bool include_jvm_threads,
    const Webserver::ArgumentMap& args, Document* document);

}

// A singleton class that tracks all live threads, and groups them together for easy
// auditing. Used only by Thread.
class ThreadMgr {
 public:
  ThreadMgr() : metrics_enabled_(false) { }

  Status StartInstrumentation(MetricGroup* metrics);

  // Registers a thread to the supplied category. The key is a boost::thread::id, used
  // instead of the system TID since boost::thread::id is always available, unlike
  // gettid() which might fail.
  void AddThread(const thread::id& thread, const string& name, const string& category,
      int64_t tid);

  // Removes a thread from the supplied category. If the thread has
  // already been removed, this is a no-op.
  void RemoveThread(const thread::id& boost_id, const string& category);

  // Example output:
  // "total_threads": 144,
  //   "thread-groups": [
  //       {
  //         "name": "common",
  //             "size": 1
  //             },
  //       {
  //         "name": "disk-io-mgr",
  //             "size": 2
  //             },
  //       {
  //         "name": "hdfs-worker-pool",
  //             "size": 16
  //             },
  //             ... etc ...
  //      ]
  void GetThreadOverview(Document* document);

  // Example output:
  // "thread-group": {
  //   "category": "disk-io-mgr",
  //       "size": 2
  //       },
  //   "threads": [
  //       {
  //         "name": "work-loop(Disk: 0, Thread: 0)-17049",
  //             "user_ns": 0,
  //             "kernel_ns": 0,
  //             "iowait_ns": 0
  //             },
  //       {
  //         "name": "work-loop(Disk: 1, Thread: 0)-17050",
  //             "user_ns": 0,
  //             "kernel_ns": 0,
  //             "iowait_ns": 0
  //             }
  //        ]
  void ThreadGroupUrlCallback(const Webserver::ArgumentMap& args, Document* output);

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
  struct ThreadCategory {
    int64_t num_threads_created;
    map<const thread::id, ThreadDescriptor> threads_by_id;
  };

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
  IntGauge* total_threads_metric_;
  IntGauge* current_num_threads_metric_;
};

Status ThreadMgr::StartInstrumentation(MetricGroup* metrics) {
  DCHECK(metrics != NULL);
  lock_guard<mutex> l(lock_);
  metrics_enabled_ = true;
  total_threads_metric_ = metrics->AddGauge("thread-manager.total-threads-created", 0L);
  current_num_threads_metric_ = metrics->AddGauge("thread-manager.running-threads", 0L);
  return Status::OK();
}

void ThreadMgr::AddThread(const thread::id& thread, const string& name,
    const string& category, int64_t tid) {
  lock_guard<mutex> l(lock_);
  ThreadCategory& thread_category = thread_categories_[category];
  thread_category.threads_by_id[thread] = ThreadDescriptor(category, name, tid);
  ++thread_category.num_threads_created;
  if (metrics_enabled_) {
    current_num_threads_metric_->Increment(1L);
    total_threads_metric_->Increment(1L);
  }
}

void ThreadMgr::RemoveThread(const thread::id& boost_id, const string& category) {
  lock_guard<mutex> l(lock_);
  ThreadCategoryMap::iterator category_it = thread_categories_.find(category);
  DCHECK(category_it != thread_categories_.end());
  category_it->second.threads_by_id.erase(boost_id);
  if (metrics_enabled_) current_num_threads_metric_->Increment(-1L);
}

void ThreadMgr::GetThreadOverview(Document* document) {
  lock_guard<mutex> l(lock_);
  if (metrics_enabled_) {
    document->AddMember("total_threads", current_num_threads_metric_->GetValue(),
        document->GetAllocator());
  }
  Value lst(kArrayType);
  for (const ThreadCategoryMap::value_type& category: thread_categories_) {
    Value val(kObjectType);
    val.AddMember("name", category.first.c_str(), document->GetAllocator());
    val.AddMember("size", static_cast<uint64_t>(category.second.threads_by_id.size()),
        document->GetAllocator());
    val.AddMember("num_created",
        static_cast<uint64_t>(category.second.num_threads_created),
        document->GetAllocator());
    // TODO: URLEncode() name?
    lst.PushBack(val, document->GetAllocator());
  }
  document->AddMember("thread-groups", lst, document->GetAllocator());
}

void ThreadMgr::ThreadGroupUrlCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(lock_);
  vector<const ThreadCategory*> categories_to_print;
  Webserver::ArgumentMap::const_iterator category_it = args.find("group");
  string category_name = (category_it == args.end()) ? "all" : category_it->second;
  if (category_name != "all") {
    ThreadCategoryMap::const_iterator category =
        thread_categories_.find(category_name);
    if (category == thread_categories_.end()) {
      return;
    }
    categories_to_print.push_back(&category->second);
    Value val(kObjectType);
    val.AddMember("category", category->first.c_str(), document->GetAllocator());
    val.AddMember("size", static_cast<uint64_t>(category->second.threads_by_id.size()),
        document->GetAllocator());
    document->AddMember("thread-group", val, document->GetAllocator());
  } else {
    for (const ThreadCategoryMap::value_type& category: thread_categories_) {
      categories_to_print.push_back(&category.second);
    }
  }

  Value lst(kArrayType);
  for (const ThreadCategory* category: categories_to_print) {
    for (const auto& thread : category->threads_by_id) {
      Value val(kObjectType);
      val.AddMember("name", thread.second.name().c_str(), document->GetAllocator());
      val.AddMember("id", thread.second.thread_id(), document->GetAllocator());
      ThreadStats stats;
      Status status = GetThreadStats(thread.second.thread_id(), &stats);
      if (!status.ok()) {
        LOG_EVERY_N(INFO, 100) << "Could not get per-thread statistics: "
                               << status.GetDetail();
      } else {
        val.AddMember("user_ns", static_cast<double>(stats.user_ns) / 1e9,
            document->GetAllocator());
        val.AddMember("kernel_ns", static_cast<double>(stats.kernel_ns) / 1e9,
            document->GetAllocator());
        val.AddMember("iowait_ns", static_cast<double>(stats.iowait_ns) / 1e9,
            document->GetAllocator());
      }
      lst.PushBack(val, document->GetAllocator());
    }
  }
  document->AddMember("threads", lst, document->GetAllocator());
}

Status Thread::StartThread(const std::string& category, const std::string& name,
    const ThreadFunctor& functor, unique_ptr<Thread>* thread,
    bool fault_injection_eligible) {
  DCHECK(thread_manager.get() != nullptr)
      << "Thread created before InitThreading called";
  DCHECK(thread->get() == nullptr);

#ifndef NDEBUG
  if (fault_injection_eligible && FLAGS_thread_creation_fault_injection) {
    // Fail roughly 1% of the time on eligible codepaths.
    if ((rand() % 100) == 1) {
      return Status(Substitute("Fake thread creation failure (category: $0, name: $1)",
          category, name));
    }
  }
#endif

  unique_ptr<Thread> t(new Thread(category, name));
  Promise<int64_t> thread_started;
  try {
    t->thread_.reset(
        new boost::thread(&Thread::SuperviseThread, t->name_, t->category_, functor,
            GetThreadDebugInfo(), &thread_started));
  } catch (boost::thread_resource_error& e) {
    return Status(TErrorCode::THREAD_CREATION_FAILED, name, category, e.what());
  }
  // TODO: This slows down thread creation although not enormously. To make this faster,
  // consider delaying thread_started.Get() until the first call to tid(), but bear in
  // mind that some coordination is required between SuperviseThread() and this to make
  // sure that the thread is still available to have its tid set.
  t->tid_ = thread_started.Get();

  VLOG(2) << "Started thread " << t->tid() << " - " << category << ":" << name;
  *thread = move(t);
  return Status::OK();
}

void Thread::SuperviseThread(const string& name, const string& category,
    Thread::ThreadFunctor functor, const ThreadDebugInfo* parent_thread_info,
    Promise<int64_t>* thread_started) {
  int64_t system_tid = syscall(SYS_gettid);
  if (system_tid == -1) {
    string error_msg = GetStrErrMsg();
    LOG_EVERY_N(INFO, 100) << "Could not determine thread ID: " << error_msg;
  }
  // Make a copy, since we want to refer to these variables after the unsafe point below.
  string category_copy = category.empty() ? "no-category" : category;;
  shared_ptr<ThreadMgr> thread_mgr_ref = thread_manager;
  string name_copy = name.empty() ? Substitute("thread-$0", system_tid) : name;

  // Use boost's get_id rather than the system thread ID as the unique key for this thread
  // since the latter is more prone to being recycled.
  thread_mgr_ref->AddThread(this_thread::get_id(), name_copy, category_copy, system_tid);

  ThreadDebugInfo thread_debug_info;
  thread_debug_info.SetThreadName(name_copy);
  thread_debug_info.SetParentInfo(parent_thread_info);

  thread_started->Set(system_tid);

  // Any reference to any parameter not copied in by value may no longer be valid after
  // this point, since the caller that is waiting on *tid != 0 may wake, take the lock and
  // destroy the enclosing Thread object.

  functor();
  thread_mgr_ref->RemoveThread(this_thread::get_id(), category_copy);
}

void ThreadGroup::AddThread(unique_ptr<Thread>&& thread) {
  threads_.emplace_back(move(thread));
}

void ThreadGroup::JoinAll() {
  for (auto& thread : threads_) thread->Join();
}

int ThreadGroup::Size() const {
  return threads_.size();
}

namespace {

void RegisterUrlCallbacks(bool include_jvm_threads, Webserver* webserver) {
  DCHECK(webserver != nullptr);
  auto overview_callback = [include_jvm_threads]
      (const Webserver::ArgumentMap& args, Document* doc) {
    ThreadOverviewUrlCallback(include_jvm_threads, args, doc);
  };
  webserver->RegisterUrlCallback(THREADS_WEB_PAGE, THREADS_TEMPLATE, overview_callback);

  auto group_callback = [] (const Webserver::ArgumentMap& args, Document* doc) {
    thread_manager->ThreadGroupUrlCallback(args, doc);
  };
  webserver->RegisterUrlCallback(THREAD_GROUP_WEB_PAGE, THREAD_GROUP_TEMPLATE,
      group_callback, false);

  if (include_jvm_threads) {
    auto jvm_threads_callback = [] (const Webserver::ArgumentMap& args, Document* doc) {
      JvmThreadsUrlCallback(args, doc);
    };
    webserver->RegisterUrlCallback(JVM_THREADS_WEB_PAGE, JVM_THREADS_TEMPLATE,
        jvm_threads_callback, false);
  }
}

void ThreadOverviewUrlCallback(bool include_jvm_threads,
    const Webserver::ArgumentMap& args, Document* document) {
  thread_manager->GetThreadOverview(document);
  if (!include_jvm_threads) return;

  // Add information about the JVM threads
  TGetJvmThreadsInfoRequest request;
  request.get_complete_info = false;
  TGetJvmThreadsInfoResponse response;
  Status status = JniUtil::GetJvmThreadsInfo(request, &response);
  if (!status.ok()) {
    Value error(Substitute("Couldn't retrieve information about JVM threads: $0",
        status.GetDetail()).c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    return;
  }
  Value jvm_threads_val(kObjectType);
  jvm_threads_val.AddMember("name", "jvm", document->GetAllocator());
  jvm_threads_val.AddMember("total", response.total_thread_count,
      document->GetAllocator());
  jvm_threads_val.AddMember("daemon", response.daemon_thread_count,
      document->GetAllocator());
  document->AddMember("jvm-threads", jvm_threads_val, document->GetAllocator());
}

void JvmThreadsUrlCallback(const Webserver::ArgumentMap& args, Document* doc) {
  DCHECK(doc != NULL);
  TGetJvmThreadsInfoRequest request;
  request.get_complete_info = true;
  TGetJvmThreadsInfoResponse response;
  Status status = JniUtil::GetJvmThreadsInfo(request, &response);
  if (!status.ok()) {
    Value error(Substitute("Couldn't retrieve information about JVM threads: $0",
        status.GetDetail()).c_str(), doc->GetAllocator());
    doc->AddMember("error", error, doc->GetAllocator());
    return;
  }
  Value overview(kObjectType);
  overview.AddMember("thread_count", response.total_thread_count, doc->GetAllocator());
  overview.AddMember("daemon_count", response.daemon_thread_count, doc->GetAllocator());
  overview.AddMember("peak_count", response.peak_thread_count, doc->GetAllocator());
  doc->AddMember("overview", overview, doc->GetAllocator());

  Value lst(kArrayType);
  for (const TJvmThreadInfo& thread: response.threads) {
    Value val(kObjectType);
    Value summary(thread.summary.c_str(), doc->GetAllocator());
    val.AddMember("summary", summary, doc->GetAllocator());
    val.AddMember("cpu_time_sec", static_cast<double>(thread.cpu_time_in_ns) / 1e9,
        doc->GetAllocator());
    val.AddMember("user_time_sec", static_cast<double>(thread.user_time_in_ns) / 1e9,
        doc->GetAllocator());
    val.AddMember("blocked_time_ms", thread.blocked_time_in_ms, doc->GetAllocator());
    val.AddMember("blocked_count", thread.blocked_count, doc->GetAllocator());
    val.AddMember("is_native", thread.is_in_native, doc->GetAllocator());
    lst.PushBack(val, doc->GetAllocator());
  }
  doc->AddMember("jvm-threads", lst, doc->GetAllocator());
}

}

void InitThreading() {
  DCHECK(thread_manager.get() == nullptr);
  thread_manager.reset(new ThreadMgr());
}

Status StartThreadInstrumentation(MetricGroup* metrics, Webserver* webserver,
    bool include_jvm_threads) {
  DCHECK(metrics != nullptr);
  DCHECK(webserver != nullptr);
  RETURN_IF_ERROR(thread_manager->StartInstrumentation(metrics));
  RegisterUrlCallbacks(include_jvm_threads, webserver);
  return Status::OK();
}

}
