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
#include "kudu/util/test_util.h"

#include <map>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gtest/gtest-spi.h>

#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/random.h"
#include "kudu/util/spinlock_profiling.h"

DEFINE_string_hidden(test_leave_files, "on_failure",
              "Whether to leave test files around after the test run. "
              " Valid values are 'always', 'on_failure', or 'never'");

DEFINE_int32_hidden(test_random_seed, 0, "Random seed to use for randomized tests");

using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {

const char* kInvalidPath = "/dev/invalid-path-for-kudu-tests";
static const char* const kSlowTestsEnvVariable = "KUDU_ALLOW_SLOW_TESTS";

static const uint64 kTestBeganAtMicros = Env::Default()->NowMicros();

// Global which production code can check to see if it is running
// in a GTest environment (assuming the test binary links in this module,
// which is typically a good assumption).
//
// This can be checked using the 'IsGTest()' function from test_util_prod.cc.
bool g_is_gtest = true;

///////////////////////////////////////////////////
// KuduTest
///////////////////////////////////////////////////

KuduTest::KuduTest()
  : env_(Env::Default()),
    test_dir_(GetTestDataDirectory()) {
  std::map<const char*, const char*> flags_for_tests = {
    // Disabling fsync() speeds up tests dramatically, and it's safe to do as no
    // tests rely on cutting power to a machine or equivalent.
    {"never_fsync", "true"},
    // Disable log redaction.
    {"redact", "flag"},
    // Reduce default RSA key length for faster tests. We are using strong/high
    // TLS v1.2 cipher suites, so minimum possible for TLS-related RSA keys is
    // 768 bits. However, for the external mini cluster we use 1024 bits because
    // Java default security policies require at least 1024 bits for RSA keys
    // used in certificates. For uniformity, here 1024 RSA bit keys are used
    // as well. As for the TSK keys, 512 bits is the minimum since the SHA256
    // digest is used for token signing/verification.
    {"ipki_server_key_size", "1024"},
    {"ipki_ca_key_size", "1024"},
    {"tsk_num_rsa_bits", "512"},
  };
  for (const auto& e : flags_for_tests) {
    // We don't check for errors here, because we have some default flags that
    // only apply to certain tests.
    google::SetCommandLineOptionWithMode(e.first, e.second, google::SET_FLAGS_DEFAULT);
  }
}

KuduTest::~KuduTest() {
  // Clean up the test directory in the destructor instead of a TearDown
  // method. This is better because it ensures that the child-class
  // dtor runs first -- so, if the child class is using a minicluster, etc,
  // we will shut that down before we remove files underneath.
  if (FLAGS_test_leave_files == "always") {
    LOG(INFO) << "-----------------------------------------------";
    LOG(INFO) << "--test_leave_files specified, leaving files in " << test_dir_;
  } else if (FLAGS_test_leave_files == "on_failure" && HasFatalFailure()) {
    LOG(INFO) << "-----------------------------------------------";
    LOG(INFO) << "Had fatal failures, leaving test files at " << test_dir_;
  } else {
    VLOG(1) << "Cleaning up temporary test files...";
    WARN_NOT_OK(env_->DeleteRecursively(test_dir_),
                "Couldn't remove test files");
  }
}

void KuduTest::SetUp() {
  InitSpinLockContentionProfiling();
  OverrideKrb5Environment();
}

string KuduTest::GetTestPath(const string& relative_path) {
  return JoinPathSegments(test_dir_, relative_path);
}

void KuduTest::OverrideKrb5Environment() {
  // Set these variables to paths that definitely do not exist and
  // couldn't be accidentally created.
  //
  // Note that if we were to set these to /dev/null, we end up triggering a leak in krb5
  // when it tries to read an empty file as a ticket cache, whereas non-existent files
  // don't have this issue. See MIT krb5 bug #8509.
  //
  // NOTE: we don't simply *unset* the variables, because then we'd still pick up
  // the user's /etc/krb5.conf and other default locations.
  setenv("KRB5_CONFIG", kInvalidPath, 1);
  setenv("KRB5_KTNAME", kInvalidPath, 1);
  setenv("KRB5CCNAME", kInvalidPath, 1);
  // Enable the workaround for MIT krb5 1.10 bugs from krb5_realm_override.cc.
  setenv("KUDU_ENABLE_KRB5_REALM_FIX", "yes", 1);
}

///////////////////////////////////////////////////
// Test utility functions
///////////////////////////////////////////////////

bool AllowSlowTests() {
  char *e = getenv(kSlowTestsEnvVariable);
  if ((e == nullptr) ||
      (strlen(e) == 0) ||
      (strcasecmp(e, "false") == 0) ||
      (strcasecmp(e, "0") == 0) ||
      (strcasecmp(e, "no") == 0)) {
    return false;
  }
  if ((strcasecmp(e, "true") == 0) ||
      (strcasecmp(e, "1") == 0) ||
      (strcasecmp(e, "yes") == 0)) {
    return true;
  }
  LOG(FATAL) << "Unrecognized value for " << kSlowTestsEnvVariable << ": " << e;
  return false;
}

void OverrideFlagForSlowTests(const std::string& flag_name,
                              const std::string& new_value) {
  // Ensure that the flag is valid.
  google::GetCommandLineFlagInfoOrDie(flag_name.c_str());

  // If we're not running slow tests, don't override it.
  if (!AllowSlowTests()) {
    return;
  }
  google::SetCommandLineOptionWithMode(flag_name.c_str(), new_value.c_str(),
                                       google::SET_FLAG_IF_DEFAULT);
}

int SeedRandom() {
  int seed;
  // Initialize random seed
  if (FLAGS_test_random_seed == 0) {
    // Not specified by user
    seed = static_cast<int>(GetCurrentTimeMicros());
  } else {
    seed = FLAGS_test_random_seed;
  }
  LOG(INFO) << "Using random seed: " << seed;
  srand(seed);
  return seed;
}

string GetTestDataDirectory() {
  const ::testing::TestInfo* const test_info =
    ::testing::UnitTest::GetInstance()->current_test_info();
  CHECK(test_info) << "Must be running in a gtest unit test to call this function";
  string dir;
  CHECK_OK(Env::Default()->GetTestDirectory(&dir));

  // The directory name includes some strings for specific reasons:
  // - program name: identifies the directory to the test invoker
  // - timestamp and pid: disambiguates with prior runs of the same test
  //
  // e.g. "env-test.TestEnv.TestReadFully.1409169025392361-23600"
  dir += Substitute("/$0.$1.$2.$3-$4",
    StringReplace(google::ProgramInvocationShortName(), "/", "_", true),
    StringReplace(test_info->test_case_name(), "/", "_", true),
    StringReplace(test_info->name(), "/", "_", true),
    kTestBeganAtMicros,
    getpid());
  Status s = Env::Default()->CreateDir(dir);
  CHECK(s.IsAlreadyPresent() || s.ok())
    << "Could not create directory " << dir << ": " << s.ToString();
  if (s.ok()) {
    string metadata;

    StrAppend(&metadata, Substitute("PID=$0\n", getpid()));

    StrAppend(&metadata, Substitute("PPID=$0\n", getppid()));

    char* jenkins_build_id = getenv("BUILD_ID");
    if (jenkins_build_id) {
      StrAppend(&metadata, Substitute("BUILD_ID=$0\n", jenkins_build_id));
    }

    CHECK_OK(WriteStringToFile(Env::Default(), metadata,
                               Substitute("$0/test_metadata", dir)));
  }
  return dir;
}

void AssertEventually(const std::function<void(void)>& f,
                      const MonoDelta& timeout) {
  const MonoTime deadline = MonoTime::Now() + timeout;

  for (int attempts = 0; MonoTime::Now() < deadline; attempts++) {
    // Capture any assertion failures within this scope (i.e. from their function)
    // into 'results'
    testing::TestPartResultArray results;
    testing::ScopedFakeTestPartResultReporter reporter(
        testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
        &results);
    f();

    // Determine whether their function produced any new test failure results.
    bool has_failures = false;
    for (int i = 0; i < results.size(); i++) {
      has_failures |= results.GetTestPartResult(i).failed();
    }
    if (!has_failures) {
      return;
    }

    // If they had failures, sleep and try again.
    int sleep_ms = (attempts < 10) ? (1 << attempts) : 1000;
    SleepFor(MonoDelta::FromMilliseconds(sleep_ms));
  }

  // If we ran out of time looping, run their function one more time
  // without capturing its assertions. This way the assertions will
  // propagate back out to the normal test reporter. Of course it's
  // possible that it will pass on this last attempt, but that's OK
  // too, since we aren't trying to be that strict about the deadline.
  f();
  if (testing::Test::HasFatalFailure()) {
    ADD_FAILURE() << "Timed out waiting for assertion to pass.";
  }
}

int CountOpenFds(Env* env) {
  static const char* kProcSelfFd =
#if defined(__APPLE__)
    "/dev/fd";
#else
    "/proc/self/fd";
#endif // defined(__APPLE__)

  vector<string> children;
  CHECK_OK(env->GetChildren(kProcSelfFd, &children));
  int num_fds = 0;
  for (const auto& c : children) {
    // Skip '.' and '..'.
    if (c == "." || c == "..") {
      continue;
    }
    num_fds++;
  }

  // Exclude the fd opened to iterate over kProcSelfFd.
  return num_fds - 1;
}

} // namespace kudu
