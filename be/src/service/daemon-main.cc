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

#include <iostream>
#include <string>
#include <boost/filesystem.hpp>

using namespace boost::filesystem;
using namespace std;

int StatestoredMain(int, char**);
int CatalogdMain(int, char**);
int ImpaladMain(int, char**);
int AdmissiondMain(int, char**);

int main(int argc, char** argv) {
  path cmd_line_path(argv[0]);
  string daemon = cmd_line_path.filename().string();
  if (daemon == "statestored") {
    return StatestoredMain(argc, argv);
  }

  if (daemon == "impalad") {
    return ImpaladMain(argc, argv);
  }

  if (daemon == "catalogd") {
    return CatalogdMain(argc, argv);
  }

  if (daemon == "admissiond") {
    return AdmissiondMain(argc, argv);
  }

  cerr << "Unknown daemon name: " << daemon
       << " (valid options: impalad, catalogd, statestored)" << endl;
  exit(1);
}
