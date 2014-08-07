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

#include "rpc/rpc-trace.h"

#include <boost/bind.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/foreach.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "util/webserver.h"

using namespace impala;
using namespace boost;
using namespace rapidjson;
using namespace std;
using namespace strings;

// Singleton class to keep track of all RpcEventHandlers, and to render them to a
// web-based summary page.
class RpcEventHandlerManager {
 public:
  // Adds an event handler to the list of those tracked
  void RegisterEventHandler(RpcEventHandler* event_handler);

  // Produces Json for /rpcz with summary information for all Rpc methods.
  // { "servers": [
  //  .. list of output from RpcEventHandler::ToJson()
  //  ] }
  void JsonCallback(const Webserver::ArgumentMap& args, Document* document);

 private:
  // Protects event_handlers_
  mutex lock_;

  // List of all event handlers in the system - once an event handler is registered, it
  // should never be deleted. Event handlers are owned by the TProcessor which calls them,
  // which are themselves owned by a ThriftServer. Since we do not terminate ThriftServers
  // after they are started, event handlers have a lifetime equivalent to the length of
  // the process.
  vector<RpcEventHandler*> event_handlers_;
};

// Only instance of RpcEventHandlerManager
scoped_ptr<RpcEventHandlerManager> handler_manager;

void impala::InitRpcEventTracing(Webserver* webserver) {
  handler_manager.reset(new RpcEventHandlerManager());
  if (webserver != NULL) {
    Webserver::UrlCallback json = bind<void>(
        mem_fn(&RpcEventHandlerManager::JsonCallback), handler_manager.get(), _1, _2);
    webserver->RegisterUrlCallback("/rpcz", "rpcz.tmpl", json);
  }
}

void RpcEventHandlerManager::RegisterEventHandler(RpcEventHandler* event_handler) {
  DCHECK(event_handler != NULL);
  lock_guard<mutex> l(lock_);
  event_handlers_.push_back(event_handler);
}

void RpcEventHandlerManager::JsonCallback(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(lock_);
  Value servers(kArrayType);
  BOOST_FOREACH(RpcEventHandler* handler, event_handlers_) {
    Value server(kObjectType);
    handler->ToJson(&server, document);
    servers.PushBack(server, document->GetAllocator());
  }
  document->AddMember("servers", servers, document->GetAllocator());
}

RpcEventHandler::RpcEventHandler(const string& server_name, Metrics* metrics) :
    server_name_(server_name), metrics_(metrics) {
  if (handler_manager.get() != NULL) handler_manager->RegisterEventHandler(this);
}

void RpcEventHandler::ToJson(Value* server, Document* document) {
  lock_guard<mutex> l(method_map_lock_);
  Value name(server_name_.c_str(), document->GetAllocator());
  server->AddMember("name", name, document->GetAllocator());
  Value methods(kArrayType);
  BOOST_FOREACH(const MethodMap::value_type& rpc, method_map_) {
    Value method(kObjectType);
    Value method_name(rpc.first.c_str(), document->GetAllocator());
    method.AddMember("name", method_name, document->GetAllocator());
    stringstream ss;
    rpc.second->time_stats->PrintValue(&ss);
    Value summary(ss.str().c_str(), document->GetAllocator());
    method.AddMember("summary", summary, document->GetAllocator());
    method.AddMember("in_flight", rpc.second->num_in_flight, document->GetAllocator());
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
      const string& time_metric_name =
          Substitute("rpc-method.$0.$1.call_duration", server_name_, descriptor->name);
      descriptor->time_stats =
          metrics_->RegisterMetric(new StatsMetric<double>(time_metric_name));
      it = method_map_.insert(make_pair(descriptor->name, descriptor)).first;
    }
  }
  ++(it->second->num_in_flight);
  // TODO: Consider pooling these
  InvocationContext* ctxt_ptr =
      new InvocationContext(ms_since_epoch(), cnxn_ctx, it->second);
  VLOG_RPC << "RPC call: " << string(fn_name) << "(from "
           << ctxt_ptr->cnxn_ctx->network_address << ")";
  return reinterpret_cast<void*>(ctxt_ptr);
}

void RpcEventHandler::postWrite(void* ctx, const char* fn_name, uint32_t bytes) {
  InvocationContext* rpc_ctx = reinterpret_cast<InvocationContext*>(ctx);
  int64_t elapsed_time = ms_since_epoch() - rpc_ctx->start_time_ms;
  const string& call_name = string(fn_name);
  // TODO: bytes is always 0, how come?
  VLOG_RPC << "RPC call: " << server_name_ << ":" << call_name << " from "
           << rpc_ctx->cnxn_ctx->network_address << " took "
           << PrettyPrinter::Print(elapsed_time * 1000L * 1000L, TCounterType::TIME_NS);
  MethodDescriptor* descriptor = rpc_ctx->method_descriptor;
  delete rpc_ctx;
  --descriptor->num_in_flight;
  descriptor->time_stats->Update(elapsed_time);
}
