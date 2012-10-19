// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#ifndef IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H
#define IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H

namespace impala {

class Webserver;

// Adds a set of default path handlers to the webserver to display
// logs and configuration flags
void AddDefaultPathHandlers(Webserver* webserver);
}

#endif // IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H
