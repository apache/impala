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

#include "util/service-util.h"

#include <sstream>
#include <errno.h>
#include <string.h>

#include <boost/algorithm/string.hpp> 
#include <google/malloc_extension.h>

using namespace boost;
using namespace std;

namespace impala {

void ServiceUtil::RenderMemUsage(const Webserver::ArgumentMap& args,
      stringstream* output) {
#ifdef ADDRESS_SANITIZER
  (*output) << "Memory tracking is not available with address sanitizer builds.";
#else
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  // Replace new lines with <br> for html
  string tmp(buf);
  replace_all(tmp, "\n", "<br>");
  (*output) << tmp;
#endif
}

}

