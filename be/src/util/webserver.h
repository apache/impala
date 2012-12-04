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


#ifndef IMPALA_UTIL_WEBSERVER_H
#define IMPALA_UTIL_WEBSERVER_H

#include <mongoose/mongoose.h>
#include <string>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"

namespace impala {

// Wrapper class for the Mongoose web server library. Clients may register callback
// methods which produce output for a given URL path
class Webserver {
 public:
  typedef std::map<std::string, std::string> ArgumentMap;
  typedef boost::function<void (const ArgumentMap& args, std::stringstream* output)> 
      PathHandlerCallback;
  
  // If interface is set to the empty string the socket will bind to all available
  // interfaces.
  Webserver(const std::string& interface, const int port);

  // Uses FLAGS_webserver_{port, interface}
  Webserver();

  ~Webserver();

  // Starts a webserver on the port passed to the constructor. The webserver runs in a
  // separate thread, so this call is non-blocking.
  Status Start();

  // Stops the webserver synchronously.
  void Stop();

  // Register a callback for a URL path. Path should not include the
  // http://hostname/ prefix.
  void RegisterPathHandler(const std::string& path, const PathHandlerCallback& callback);

 private:
  // Renders a common Bootstrap-styled header 
  void BootstrapPageHeader(std::stringstream* output);

  // Renders a common Bootstrap-styled footer. Must be used in conjunction with
  // BootstrapPageHeader.
  void BootstrapPageFooter(std::stringstream* output);

  // Static so that it can act as a function pointer, and then call the next method
  static void* MongooseCallbackStatic(enum mg_event event, 
      struct mg_connection* connection);

  // Dispatch point for all incoming requests.
  void* MongooseCallback(enum mg_event event, struct mg_connection* connection,
      const struct mg_request_info* request_info);

  // Registered to handle "/", and prints a list of available URIs
  void RootHandler(const ArgumentMap& args, std::stringstream* output);

  // Builds a map of argument name to argument value from a typical URL argument
  // string (that is, "key1=value1&key2=value2.."). If no value is given for a
  // key, it is entered into the map as (key, "").
  void BuildArgumentMap(const std::string& args, ArgumentMap* output);

  // Lock guarding the path_handlers_ map
  boost::mutex path_handlers_lock_;

  // Map of path to a list of handlers for that path. More than one handler may
  // register itself with a path so that many components may contribute to a
  // single page.
  typedef std::map<std::string, std::vector<PathHandlerCallback> > PathHandlerMap;
  PathHandlerMap path_handlers_;

  const int port_;
  // If empty, webserver will bind to all interfaces.
  const std::string& interface_;

  // Handle to Mongoose context; owned and freed by Mongoose internally
  struct mg_context* context_;
};

}

#endif // IMPALA_UTIL_WEBSERVER_H
