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

#include "rpc/rpc-mgr.inline.h"

#include "common/init.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/auth-provider.h"
#include "testutil/gtest-util.h"
#include "testutil/mini-kdc-wrapper.h"
#include "util/counting-barrier.h"
#include "util/network-util.h"
#include "util/test-info.h"

#include "gen-cpp/rpc_test.proxy.h"
#include "gen-cpp/rpc_test.service.h"

#include "common/names.h"

using kudu::rpc::ErrorStatusPB;
using kudu::rpc::ServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::RpcSidecar;
using kudu::MonoDelta;
using kudu::Slice;

using namespace std;

DECLARE_int32(num_reactor_threads);
DECLARE_int32(num_acceptor_threads);
DECLARE_string(hostname);


// The path of the current executable file that is required for passing into the SASL
// library as the 'application name'.
static string CURRENT_EXECUTABLE_PATH;

namespace impala {

static int32_t SERVICE_PORT = FindUnusedEphemeralPort(nullptr);

int GetServerPort() {
  int port = FindUnusedEphemeralPort(nullptr);
  EXPECT_FALSE(port == -1);
  return port;
}

static int kdc_port = GetServerPort();

#define PAYLOAD_SIZE (4096)

template <class T> class RpcMgrTestBase : public T {
 protected:
  TNetworkAddress krpc_address_;
  RpcMgr rpc_mgr_;

  virtual void SetUp() {
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    krpc_address_ = MakeNetworkAddress(ip, SERVICE_PORT);
    ASSERT_OK(rpc_mgr_.Init());
  }

  virtual void TearDown() {
    rpc_mgr_.Shutdown();
  }

  // Utility function to initialize the parameter for ScanMem RPC.
  // Picks a random value and fills 'payload_' with it. Adds 'payload_' as a sidecar
  // to 'controller'. Also sets up 'request' with the random value and index of the
  // sidecar.
  void SetupScanMemRequest(ScanMemRequestPB* request, RpcController* controller) {
    int32_t pattern = random();
    for (int i = 0; i < PAYLOAD_SIZE / sizeof(int32_t); ++i) payload_[i] = pattern;
    int idx;
    Slice slice(reinterpret_cast<const uint8_t*>(payload_), PAYLOAD_SIZE);
    controller->AddOutboundSidecar(RpcSidecar::FromSlice(slice), &idx);
    request->set_pattern(pattern);
    request->set_sidecar_idx(idx);
  }

 private:
  int32_t payload_[PAYLOAD_SIZE];
};

// For tests that do not require kerberized testing, we use RpcTest.
class RpcMgrTest : public RpcMgrTestBase<testing::Test> {
  virtual void SetUp() {
    RpcMgrTestBase::SetUp();
  }

  virtual void TearDown() {
    RpcMgrTestBase::TearDown();
  }
};

class RpcMgrKerberizedTest :
    public RpcMgrTestBase<testing::TestWithParam<KerberosSwitch> > {
  virtual void SetUp() {
    IpAddr ip;
    ASSERT_OK(HostnameToIpAddr(FLAGS_hostname, &ip));
    string spn = Substitute("impala-test/$0", ip);

    kdc_wrapper_.reset(new MiniKdcWrapper(
        std::move(spn), "KRBTEST.COM", "24h", "7d", kdc_port));
    DCHECK(kdc_wrapper_.get() != nullptr);

    ASSERT_OK(kdc_wrapper_->SetupAndStartMiniKDC(GetParam()));
    ASSERT_OK(InitAuth(CURRENT_EXECUTABLE_PATH));

    RpcMgrTestBase::SetUp();
  }

  virtual void TearDown() {
    ASSERT_OK(kdc_wrapper_->TearDownMiniKDC(GetParam()));
    RpcMgrTestBase::TearDown();
  }

 private:
  boost::scoped_ptr<MiniKdcWrapper> kdc_wrapper_;
};

typedef std::function<void(RpcContext*)> ServiceCB;

class PingServiceImpl : public PingServiceIf {
 public:
  // 'cb' is a callback used by tests to inject custom behaviour into the RPC handler.
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker,
      ServiceCB cb = [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(entity, tracker), cb_(cb) {}

  virtual void Ping(
      const PingRequestPB* request, PingResponsePB* response, RpcContext* context) {
    response->set_int_response(42);
    cb_(context);
  }

 private:
  ServiceCB cb_;
};

class ScanMemServiceImpl : public ScanMemServiceIf {
 public:
  ScanMemServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker)
    : ScanMemServiceIf(entity, tracker) {
  }

  // The request comes with an int 'pattern' and a payload of int array sent with
  // sidecar. Scan the array to make sure every element matches 'pattern'.
  virtual void ScanMem(const ScanMemRequestPB* request, ScanMemResponsePB* response,
      RpcContext* context) {
    int32_t pattern = request->pattern();
    Slice payload;
    ASSERT_OK(
        FromKuduStatus(context->GetInboundSidecar(request->sidecar_idx(), &payload)));
    ASSERT_EQ(payload.size() % sizeof(int32_t), 0);

    const int32_t* v = reinterpret_cast<const int32_t*>(payload.data());
    for (int i = 0; i < payload.size() / sizeof(int32_t); ++i) {
      int32_t val = v[i];
      if (val != pattern) {
        context->RespondFailure(kudu::Status::Corruption(
            Substitute("Expecting $1; Found $2", pattern, val)));
        return;
      }
    }
    context->RespondSuccess();
  }
};

// TODO: Disabled 'USE_KUDU_KERBEROS' and 'USE_IMPALA_KERBEROS' due to IMPALA-6268.
// Reenable after fixing.
INSTANTIATE_TEST_CASE_P(KerberosOnAndOff,
                        RpcMgrKerberizedTest,
                        ::testing::Values(KERBEROS_OFF));

TEST_P(RpcMgrKerberizedTest, MultipleServices) {
  // Test that a service can be started, and will respond to requests.
  unique_ptr<ServiceIf> ping_impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 10, move(ping_impl)));

  // Test that a second service, that verifies the RPC payload is not corrupted,
  // can be started.
  unique_ptr<ServiceIf> scan_mem_impl(
      new ScanMemServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 10, move(scan_mem_impl)));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  unique_ptr<PingServiceProxy> ping_proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(krpc_address_, &ping_proxy));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<ScanMemServiceProxy>(krpc_address_, &scan_mem_proxy));

  RpcController controller;
  srand(0);
  // Randomly invoke either services to make sure a RpcMgr can host multiple
  // services at the same time.
  for (int i = 0; i < 100; ++i) {
    controller.Reset();
    if (random() % 2 == 0) {
      PingRequestPB request;
      PingResponsePB response;
      kudu::Status status = ping_proxy->Ping(request, &response, &controller);
      ASSERT_TRUE(status.ok());
      ASSERT_EQ(response.int_response(), 42);
    } else {
      ScanMemRequestPB request;
      ScanMemResponsePB response;
      SetupScanMemRequest(&request, &controller);
      kudu::Status status = scan_mem_proxy->ScanMem(request, &response, &controller);
      ASSERT_TRUE(status.ok());
    }
  }
}

TEST_F(RpcMgrTest, SlowCallback) {

  // Use a callback which is slow to respond.
  auto slow_cb = [](RpcContext* ctx) {
    SleepForMs(300);
    ctx->RespondSuccess();
  };

  // Test a service which is slow to respond and has a short queue.
  // Set a timeout on the client side. Expect either a client timeout
  // or the service queue filling up.
  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker(), slow_cb));
  const int num_service_threads = 1;
  const int queue_size = 3;
  ASSERT_OK(rpc_mgr_.RegisterService(num_service_threads, queue_size, move(impl)));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(krpc_address_, &proxy));

  PingRequestPB request;
  PingResponsePB response;
  RpcController controller;
  for (int i = 0; i < 100; ++i) {
    controller.Reset();
    controller.set_timeout(MonoDelta::FromMilliseconds(50));
    kudu::Status status = proxy->Ping(request, &response, &controller);
    ASSERT_TRUE(status.IsTimedOut() || RpcMgr::IsServerTooBusy(controller));
  }
}

TEST_F(RpcMgrTest, AsyncCall) {
  unique_ptr<ServiceIf> scan_mem_impl(
      new ScanMemServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 10, move(scan_mem_impl)));

  unique_ptr<ScanMemServiceProxy> scan_mem_proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<ScanMemServiceProxy>(krpc_address_, &scan_mem_proxy));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 10;
  ASSERT_OK(rpc_mgr_.StartServices(krpc_address_));

  RpcController controller;
  srand(0);
  for (int i = 0; i < 10; ++i) {
    controller.Reset();
    ScanMemRequestPB request;
    ScanMemResponsePB response;
    SetupScanMemRequest(&request, &controller);
    CountingBarrier barrier(1);
    scan_mem_proxy->ScanMemAsync(request, &response, &controller,
        [barrier_ptr = &barrier]() { barrier_ptr->Notify(); });
    // TODO: Inject random cancellation here.
    barrier.Wait();
    ASSERT_TRUE(controller.status().ok()) << controller.status().ToString();
  }
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, false, impala::TestInfo::BE_TEST);

  // Fill in the path of the current binary for use by the tests.
  CURRENT_EXECUTABLE_PATH = argv[0];
  return RUN_ALL_TESTS();
}
