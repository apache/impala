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

#include "rpc/rpc-trace.h"

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "rpc/rpc-mgr.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;
using namespace strings;

// Metric key format for rpc call duration metrics.
const string RPC_PROCESSING_TIME_DISTRIBUTION_METRIC_KEY = "rpc-method.$0.call_duration";

// Singleton class to keep track of all RpcEventHandlers, and to render them to a
// web-based summary page.
class RpcEventHandlerManager {
 public:
  RpcEventHandlerManager(RpcMgr* rpc_mgr) : rpc_mgr_(rpc_mgr) {}

  // Adds an event handler to the list of those tracked
  void RegisterEventHandler(RpcEventHandler* event_handler);

  // Produces Json for /rpcz with summary information for all Rpc methods.
  // { "servers": [
  //  .. list of output from RpcEventHandler::ToJson()
  //  ] }
  void JsonCallback(const Webserver::ArgumentMap& args, Document* document);

  // Resets method statistics. Takes two optional arguments: 'server' and 'method'. If
  // neither are specified, all server statistics are reset. If only the former is
  // specified, all statistics for that server are reset. If both arguments are present,
  // resets the statistics for a single method only. Produces no JSON output.
  void ResetCallback(const Webserver::ArgumentMap& args, Document* document);

 private:
  // Protects event_handlers_
  mutex lock_;

  // List of all event handlers in the system - once an event handler is registered, it
  // should never be deleted. Event handlers are owned by the TProcessor which calls them,
  // which are themselves owned by a ThriftServer. Since we do not terminate ThriftServers
  // after they are started, event handlers have a lifetime equivalent to the length of
  // the process.
  vector<RpcEventHandler*> event_handlers_;

  // Points to an RpcMgr. If this is not null, then its metrics will be included in the
  // output of JsonCallback. Not owned, but the object must be guaranteed to live as long
  // as the process lives.
  RpcMgr* rpc_mgr_ = nullptr;
};

// Only instance of RpcEventHandlerManager
scoped_ptr<RpcEventHandlerManager> handler_manager;

void impala::InitRpcEventTracing(Webserver* webserver, RpcMgr* rpc_mgr) {
  handler_manager.reset(new RpcEventHandlerManager(rpc_mgr));
  if (webserver != nullptr) {
    Webserver::UrlCallback json = bind<void>(
        mem_fn(&RpcEventHandlerManager::JsonCallback), handler_manager.get(), _1, _2);
    webserver->RegisterUrlCallback("/rpcz", "rpcz.tmpl", json);

    Webserver::UrlCallback reset = bind<void>(
        mem_fn(&RpcEventHandlerManager::ResetCallback), handler_manager.get(), _1, _2);
    webserver->RegisterUrlCallback("/rpcz_reset", "", reset, false);
  }
}

void RpcEventHandlerManager::RegisterEventHandler(RpcEventHandler* event_handler) {
  DCHECK(event_handler != nullptr);
  lock_guard<mutex> l(lock_);
  event_handlers_.push_back(event_handler);
}

void RpcEventHandlerManager::JsonCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(lock_);
  Value servers(kArrayType);
  for (RpcEventHandler* handler: event_handlers_) {
    Value server(kObjectType);
    handler->ToJson(&server, document);
    servers.PushBack(server, document->GetAllocator());
  }
  document->AddMember("servers", servers, document->GetAllocator());
  if (rpc_mgr_ != nullptr) rpc_mgr_->ToJson(document);
}

void RpcEventHandlerManager::ResetCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  Webserver::ArgumentMap::const_iterator server_it = args.find("server");
  bool reset_all_servers = (server_it == args.end());
  Webserver::ArgumentMap::const_iterator method_it = args.find("method");
  bool reset_all_in_server = (method_it == args.end());
  lock_guard<mutex> l(lock_);
  for (RpcEventHandler* handler: event_handlers_) {
    if (reset_all_servers || handler->server_name() == server_it->second) {
      if (reset_all_in_server) {
        handler->ResetAll();
      } else {
        handler->Reset(method_it->second);
      }
      if (!reset_all_servers) return;
    }
  }
}

void RpcEventHandler::Reset(const string& method_name) {
  lock_guard<mutex> l(method_map_lock_);
  MethodMap::iterator it = method_map_.find(method_name);
  if (it == method_map_.end()) return;
  it->second->processing_time_distribution->Reset();
  it->second->num_in_flight.Store(0L);
}

void RpcEventHandler::ResetAll() {
  lock_guard<mutex> l(method_map_lock_);
  for (const MethodMap::value_type& method: method_map_) {
    method.second->processing_time_distribution->Reset();
    method.second->num_in_flight.Store(0L);
  }
}

RpcEventHandler::RpcEventHandler(const string& server_name, MetricGroup* metrics) :
    server_name_(server_name), metrics_(metrics) {
  if (handler_manager.get() != nullptr) handler_manager->RegisterEventHandler(this);
}

void RpcEventHandler::ToJson(Value* server, Document* document) {
  lock_guard<mutex> l(method_map_lock_);
  Value name(server_name_.c_str(), document->GetAllocator());
  server->AddMember("name", name, document->GetAllocator());
  Value methods(kArrayType);
  for (const MethodMap::value_type& rpc: method_map_) {
    Value method(kObjectType);
    Value method_name(rpc.first.c_str(), document->GetAllocator());
    method.AddMember("name", method_name, document->GetAllocator());
    const string& human_readable =
        rpc.second->processing_time_distribution->ToHumanReadable();
    Value summary(human_readable.c_str(), document->GetAllocator());
    method.AddMember("summary", summary, document->GetAllocator());
    method.AddMember("in_flight", rpc.second->num_in_flight.Load(),
        document->GetAllocator());
    Value server_name(server_name_.c_str(), document->GetAllocator());
    method.AddMember("server_name", server_name, document->GetAllocator());
    methods.PushBack(method, document->GetAllocator());
  }
  server->AddMember("methods", methods, document->GetAllocator());
}

void* RpcEventHandler::getContext(const char* fn_name, void* server_context) {
  ThriftServer::ConnectionContext* cnxn_ctx =
      reinterpret_cast<ThriftServer::ConnectionContext*>(server_context);
  MethodMap::iterator it;
  {
    lock_guard<mutex> l(method_map_lock_);
    it = method_map_.find(fn_name);
    if (it == method_map_.end()) {
      MethodDescriptor* descriptor = new MethodDescriptor();
      descriptor->name = fn_name;
      const string& rpc_name = Substitute(RPC_PROCESSING_TIME_DISTRIBUTION_METRIC_KEY,
          Substitute("$0.$1", server_name_, descriptor->name));
      const TMetricDef& def =
          MakeTMetricDef(rpc_name, TMetricKind::HISTOGRAM, TUnit::TIME_MS);
      constexpr int32_t SIXTY_MINUTES_IN_MS = 60 * 1000 * 60;
      // Store processing times of up to 60 minutes with 3 sig. fig.
      descriptor->processing_time_distribution =
          metrics_->RegisterMetric(new HistogramMetric(def, SIXTY_MINUTES_IN_MS, 3));
      it = method_map_.insert(make_pair(descriptor->name, descriptor)).first;
    }
  }
  it->second->num_in_flight.Add(1);
  // TODO: Consider pooling these
  InvocationContext* ctxt_ptr =
      new InvocationContext(MonotonicMillis(), cnxn_ctx, it->second);
  VLOG_RPC << "RPC call: " << string(fn_name) << "(from "
           << ctxt_ptr->cnxn_ctx->network_address << ")";
  return reinterpret_cast<void*>(ctxt_ptr);
}

void RpcEventHandler::postWrite(void* ctx, const char* fn_name, uint32_t bytes) {
  InvocationContext* rpc_ctx = reinterpret_cast<InvocationContext*>(ctx);
  int64_t elapsed_time = MonotonicMillis() - rpc_ctx->start_time_ms;
  const string& call_name = string(fn_name);
  // TODO: bytes is always 0, how come?
  VLOG_RPC << "RPC call: " << server_name_ << ":" << call_name << " from "
           << rpc_ctx->cnxn_ctx->network_address << " took "
           << PrettyPrinter::Print(elapsed_time * 1000L * 1000L, TUnit::TIME_NS);
  MethodDescriptor* descriptor = rpc_ctx->method_descriptor;
  delete rpc_ctx;
  descriptor->num_in_flight.Add(-1);
  descriptor->processing_time_distribution->Update(elapsed_time);
}
