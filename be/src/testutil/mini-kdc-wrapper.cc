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

#include "testutil/mini-kdc-wrapper.h"

#include <string>

#include "common/names.h"
#include "exec/kudu/kudu-util.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "rpc/auth-provider.h"
#include "util/filesystem-util.h"

using namespace impala;
namespace filesystem = boost::filesystem;
using filesystem::path;

DECLARE_string(keytab_file);
DECLARE_string(krb5_conf);
DECLARE_string(krb5_ccname);

Status MiniKdcWrapper::StartKdc(string keytab_dir) {
  kudu::MiniKdcOptions options;
  options.realm = realm_;
  options.data_root = move(keytab_dir);
  options.ticket_lifetime = ticket_lifetime_;
  options.renew_lifetime = renew_lifetime_;
  options.port = kdc_port_;

  DCHECK(kdc_.get() == nullptr);
  kdc_.reset(new kudu::MiniKdc(options));
  DCHECK(kdc_.get() != nullptr);
  KUDU_RETURN_IF_ERROR(kdc_->Start(), "Failed to start KDC.");
  KUDU_RETURN_IF_ERROR(kdc_->SetKrb5Environment(), "Failed to set Kerberos environment.");
  return Status::OK();
}

Status MiniKdcWrapper::StopKdc() {
  KUDU_RETURN_IF_ERROR(kdc_->Stop(), "Failed to stop KDC.");
  return Status::OK();
}

Status MiniKdcWrapper::Kinit(const string& username) {
  KUDU_RETURN_IF_ERROR(kdc_->Kinit(username), "Failed to kinit.");
  return Status::OK();
}

Status MiniKdcWrapper::CreateUserPrincipal(const string& username) {
  KUDU_RETURN_IF_ERROR(kdc_->CreateUserPrincipal(username),
      "Failed to create user principal.");
  return Status::OK();
}

Status MiniKdcWrapper::CreateServiceKeytab(const string& spn, string* kt_path) {
  KUDU_RETURN_IF_ERROR(kdc_->CreateServiceKeytab(spn, kt_path),
      "Failed to create service keytab.");
  return Status::OK();
}

Status MiniKdcWrapper::SetupAndStartMiniKDC(string realm,
    string ticket_lifetime, string renew_lifetime,
    int kdc_port, unique_ptr<MiniKdcWrapper>* kdc_ptr) {
  unique_ptr<MiniKdcWrapper> kdc(new MiniKdcWrapper(
      move(realm), move(ticket_lifetime), move(renew_lifetime), kdc_port));
  DCHECK(kdc.get() != nullptr);

  // Enable the workaround for MIT krb5 1.10 bugs from krb5_realm_override.cc.
  setenv("KUDU_ENABLE_KRB5_REALM_FIX", "true", 0);

  // Check if the unique directory already exists, and create it if it doesn't.
  RETURN_IF_ERROR(
      FileSystemUtil::RemoveAndCreateDirectory(kdc->unique_test_dir_.string()));
  string keytab_dir = kdc->unique_test_dir_.string() + "/krb5kdc";

  RETURN_IF_ERROR(kdc->StartKdc(keytab_dir));

  // Set the appropriate flags based on how we've set up the kerberos environment.
  FLAGS_krb5_conf = strings::Substitute("$0/$1", keytab_dir, "krb5.conf");

  *kdc_ptr = std::move(kdc);
  return Status::OK();
}

Status MiniKdcWrapper::TearDownMiniKDC() {
  RETURN_IF_ERROR(StopKdc());

  // Clear the flags so we don't step on other tests that may run in the same process.
  FLAGS_krb5_conf.clear();

  // Remove test directory.
  RETURN_IF_ERROR(FileSystemUtil::RemovePaths({unique_test_dir_.string()}));
  return Status::OK();
}
