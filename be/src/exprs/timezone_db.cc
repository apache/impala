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

#include "exprs/timezone_db.h"

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/local_time/posix_time_zone.hpp>
#include <boost/filesystem.hpp>

#include "exprs/timestamp-functions.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using boost::local_time::tz_database;
using boost::local_time::time_zone_ptr;
using boost::local_time::posix_time_zone;

DECLARE_string(local_library_dir);

namespace impala {

tz_database TimezoneDatabase::tz_database_;
vector<string> TimezoneDatabase::tz_region_list_;

const time_zone_ptr TimezoneDatabase::TIMEZONE_MSK_PRE_2011_DST(time_zone_ptr(
    new posix_time_zone(string("MSK+03MSK+01,M3.5.0/02:00:00,M10.5.0/03:00:00"))));
const time_zone_ptr TimezoneDatabase::TIMEZONE_MSK_PRE_2014(time_zone_ptr(
    new posix_time_zone(string("MSK+04"))));

const char* TimezoneDatabase::TIMEZONE_DATABASE_STR = "\"ID\",\"STD ABBR\",\"STD NAME\",\"DST ABBR\",\"DST NAME\",\"GMT offset\",\"DST adjustment\",\"DST Start Date rule\",\"Start time\",\"DST End date rule\",\"End time\"\n\
\"ACT\",\"ACST\",\"Australian Central Standard Time (Northern Territory)\",\"\",\"\",\"+09:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"AET\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"AGT\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"ART\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;4;4\",\"+00:00:00\",\"-1;4;9\",\"+24:00:00\"\n\
\"AST\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Africa/Abidjan\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Accra\",\"GMT\",\"Ghana Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Addis_Ababa\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Algiers\",\"CET\",\"Central European Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Asmara\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Asmera\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Bamako\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Bangui\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Banjul\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Bissau\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Blantyre\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Brazzaville\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Bujumbura\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Cairo\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;4;4\",\"+00:00:00\",\"-1;4;9\",\"+24:00:00\"\n\
\"Africa/Casablanca\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+02:00:00\",\"-1;0;10\",\"+03:00:00\"\n\
\"Africa/Ceuta\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Africa/Conakry\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Dakar\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Dar_es_Salaam\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Djibouti\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Douala\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/El_Aaiun\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+02:00:00\",\"-1;0;10\",\"+03:00:00\"\n\
\"Africa/Freetown\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Gaborone\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Harare\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Johannesburg\",\"SAST\",\"South Africa Standard Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Juba\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Kampala\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Khartoum\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Kigali\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Kinshasa\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Lagos\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Libreville\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Lome\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Luanda\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Lubumbashi\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Lusaka\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Malabo\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Maputo\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Maseru\",\"SAST\",\"South Africa Standard Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Mbabane\",\"SAST\",\"South Africa Standard Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Mogadishu\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Monrovia\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Nairobi\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Ndjamena\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Niamey\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Nouakchott\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Ouagadougou\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Porto-Novo\",\"WAT\",\"Western African Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Sao_Tome\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Timbuktu\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Tripoli\",\"EET\",\"Eastern European Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Tunis\",\"CET\",\"Central European Time\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Africa/Windhoek\",\"WAT\",\"Western African Time\",\"WAST\",\"Western African Summer Time\",\"+01:00:00\",\"+01:00:00\",\"1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"America/Adak\",\"HAST\",\"Hawaii-Aleutian Standard Time\",\"HADT\",\"Hawaii-Aleutian Daylight Time\",\"-10:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Anchorage\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Anguilla\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Antigua\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Araguaina\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Buenos_Aires\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Catamarca\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/ComodRivadavia\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Cordoba\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Jujuy\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/La_Rioja\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Mendoza\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Rio_Gallegos\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Salta\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/San_Juan\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/San_Luis\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Tucuman\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Argentina/Ushuaia\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Aruba\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Asuncion\",\"PYT\",\"Paraguay Time\",\"PYST\",\"Paraguay Summer Time\",\"-04:00:00\",\"+01:00:00\",\"1;0;10\",\"+00:00:00\",\"4;0;3\",\"+00:00:00\"\n\
\"America/Atikokan\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Atka\",\"HAST\",\"Hawaii-Aleutian Standard Time\",\"HADT\",\"Hawaii-Aleutian Daylight Time\",\"-10:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Bahia\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Bahia_Banderas\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Barbados\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Belem\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Belize\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Blanc-Sablon\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Boa_Vista\",\"AMT\",\"Amazon Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Bogota\",\"COT\",\"Colombia Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Boise\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Buenos_Aires\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Cambridge_Bay\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Campo_Grande\",\"AMT\",\"Amazon Time\",\"AMST\",\"Amazon Summer Time\",\"-04:00:00\",\"+01:00:00\",\"3;0;10\",\"+00:00:00\",\"3;0;2\",\"+00:00:00\"\n\
\"America/Cancun\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Caracas\",\"VET\",\"Venezuela Time\",\"\",\"\",\"-04:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Catamarca\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Cayenne\",\"GFT\",\"French Guiana Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Cayman\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Chicago\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Chihuahua\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Coral_Harbour\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Cordoba\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Costa_Rica\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Creston\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Cuiaba\",\"AMT\",\"Amazon Time\",\"AMST\",\"Amazon Summer Time\",\"-04:00:00\",\"+01:00:00\",\"3;0;10\",\"+00:00:00\",\"3;0;2\",\"+00:00:00\"\n\
\"America/Curacao\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Danmarkshavn\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Dawson\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Dawson_Creek\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Denver\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Detroit\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Dominica\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Edmonton\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Eirunepe\",\"ACT\",\"Acre Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/El_Salvador\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Ensenada\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Fort_Wayne\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Fortaleza\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Glace_Bay\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Godthab\",\"WGT\",\"Western Greenland Time\",\"WGST\",\"Western Greenland Summer Time\",\"-03:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"America/Goose_Bay\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Grand_Turk\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Grenada\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Guadeloupe\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Guatemala\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Guayaquil\",\"ECT\",\"Ecuador Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Guyana\",\"GYT\",\"Guyana Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Halifax\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Havana\",\"CST\",\"Cuba Standard Time\",\"CDT\",\"Cuba Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+00:00:00\",\"1;0;11\",\"+00:00:00\"\n\
\"America/Hermosillo\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Indiana/Indianapolis\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Knox\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Marengo\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Petersburg\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Tell_City\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Vevay\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Vincennes\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indiana/Winamac\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Indianapolis\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Inuvik\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Iqaluit\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Jamaica\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Jujuy\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Juneau\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Kentucky/Louisville\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Kentucky/Monticello\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Knox_IN\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Kralendijk\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/La_Paz\",\"BOT\",\"Bolivia Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Lima\",\"PET\",\"Peru Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Los_Angeles\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Louisville\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Lower_Princes\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Maceio\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Managua\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Manaus\",\"AMT\",\"Amazon Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Marigot\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Martinique\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Matamoros\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Mazatlan\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Mendoza\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Menominee\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Merida\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Metlakatla\",\"PST\",\"Pacific Standard Time\",\"\",\"\",\"-08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Mexico_City\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Miquelon\",\"PMST\",\"Pierre & Miquelon Standard Time\",\"PMDT\",\"Pierre & Miquelon Daylight Time\",\"-03:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Moncton\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Monterrey\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Montevideo\",\"UYT\",\"Uruguay Time\",\"UYST\",\"Uruguay Summer Time\",\"-03:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"2;0;3\",\"+02:00:00\"\n\
\"America/Montreal\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Montserrat\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Nassau\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/New_York\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Nipigon\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Nome\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Noronha\",\"FNT\",\"Fernando de Noronha Time\",\"\",\"\",\"-02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/North_Dakota/Beulah\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/North_Dakota/Center\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/North_Dakota/New_Salem\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Ojinaga\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Panama\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Pangnirtung\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Paramaribo\",\"SRT\",\"Suriname Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Phoenix\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Port-au-Prince\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Port_of_Spain\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Porto_Acre\",\"ACT\",\"Acre Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Porto_Velho\",\"AMT\",\"Amazon Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Puerto_Rico\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Rainy_River\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Rankin_Inlet\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Recife\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Regina\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Resolute\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Rio_Branco\",\"ACT\",\"Acre Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Rosario\",\"ART\",\"Argentine Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Santa_Isabel\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"America/Santarem\",\"BRT\",\"Brasilia Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Santiago\",\"CLT\",\"Chile Time\",\"CLST\",\"Chile Summer Time\",\"-04:00:00\",\"+01:00:00\",\"1;0;9\",\"+04:00:00\",\"4;0;4\",\"+03:00:00\"\n\
\"America/Santo_Domingo\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Sao_Paulo\",\"BRT\",\"Brasilia Time\",\"BRST\",\"Brasilia Summer Time\",\"-03:00:00\",\"+01:00:00\",\"3;0;10\",\"+00:00:00\",\"3;0;2\",\"+00:00:00\"\n\
\"America/Scoresbysund\",\"EGT\",\"Eastern Greenland Time\",\"EGST\",\"Eastern Greenland Summer Time\",\"-01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"America/Shiprock\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Sitka\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/St_Barthelemy\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/St_Johns\",\"NST\",\"Newfoundland Standard Time\",\"NDT\",\"Newfoundland Daylight Time\",\"-03:30:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/St_Kitts\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/St_Lucia\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/St_Thomas\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/St_Vincent\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Swift_Current\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Tegucigalpa\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Thule\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Thunder_Bay\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Tijuana\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Toronto\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Tortola\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Vancouver\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Virgin\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"America/Whitehorse\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Winnipeg\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Yakutat\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"America/Yellowknife\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Antarctica/Casey\",\"AWST\",\"Australian Western Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/Davis\",\"DAVT\",\"Davis Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/DumontDUrville\",\"DDUT\",\"Dumont-d'Urville Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/Macquarie\",\"MIST\",\"Macquarie Island Standard Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/Mawson\",\"MAWT\",\"Mawson Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/McMurdo\",\"NZST\",\"New Zealand Standard Time\",\"NZDT\",\"New Zealand Daylight Time\",\"+12:00:00\",\"+01:00:00\",\"-1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Antarctica/Palmer\",\"CLT\",\"Chile Time\",\"CLST\",\"Chile Summer Time\",\"-04:00:00\",\"+01:00:00\",\"1;0;9\",\"+04:00:00\",\"4;0;4\",\"+03:00:00\"\n\
\"Antarctica/Rothera\",\"ROTT\",\"Rothera Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/South_Pole\",\"NZST\",\"New Zealand Standard Time\",\"NZDT\",\"New Zealand Daylight Time\",\"+12:00:00\",\"+01:00:00\",\"-1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Antarctica/Syowa\",\"SYOT\",\"Syowa Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Antarctica/Troll\",\"UTC\",\"Coordinated Universal Time\",\"CEST\",\"Central European Summer Time\",\"+00:00:00\",\"+02:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Antarctica/Vostok\",\"VOST\",\"Vostok Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Arctic/Longyearbyen\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Asia/Aden\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Almaty\",\"ALMT\",\"Alma-Ata Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Amman\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;5;3\",\"+24:00:00\",\"-1;5;10\",\"+00:00:00\"\n\
\"Asia/Anadyr\",\"ANAT\",\"Anadyr Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Aqtau\",\"AQTT\",\"Aqtau Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Aqtobe\",\"AQTT\",\"Aqtobe Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ashgabat\",\"TMT\",\"Turkmenistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ashkhabad\",\"TMT\",\"Turkmenistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Baghdad\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Bahrain\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Baku\",\"AZT\",\"Azerbaijan Time\",\"AZST\",\"Azerbaijan Summer Time\",\"+04:00:00\",\"+01:00:00\",\"-1;0;3\",\"+04:00:00\",\"-1;0;10\",\"+05:00:00\"\n\
\"Asia/Bangkok\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Beirut\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+00:00:00\",\"-1;0;10\",\"+00:00:00\"\n\
\"Asia/Bishkek\",\"KGT\",\"Kirgizstan Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Brunei\",\"BNT\",\"Brunei Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Calcutta\",\"IST\",\"India Standard Time\",\"\",\"\",\"+05:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Chita\",\"IRKT\",\"Irkutsk Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Choibalsan\",\"CHOT\",\"Choibalsan Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Chongqing\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Chungking\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Colombo\",\"IST\",\"India Standard Time\",\"\",\"\",\"+05:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Dacca\",\"BDT\",\"Bangladesh Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Damascus\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;5;3\",\"+00:00:00\",\"-1;5;10\",\"+00:00:00\"\n\
\"Asia/Dhaka\",\"BDT\",\"Bangladesh Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Dili\",\"TLT\",\"Timor-Leste Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Dubai\",\"GST\",\"Gulf Standard Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Dushanbe\",\"TJT\",\"Tajikistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Gaza\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;5;3\",\"+24:00:00\",\"4;5;9\",\"+00:00:00\"\n\
\"Asia/Harbin\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Hebron\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;5;3\",\"+24:00:00\",\"4;5;9\",\"+00:00:00\"\n\
\"Asia/Ho_Chi_Minh\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Hong_Kong\",\"HKT\",\"Hong Kong Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Hovd\",\"HOVT\",\"Hovd Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Irkutsk\",\"IRKT\",\"Irkutsk Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Istanbul\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Asia/Jakarta\",\"WIB\",\"West Indonesia Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Jayapura\",\"WIT\",\"East Indonesia Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Jerusalem\",\"IST\",\"Israel Standard Time\",\"IDT\",\"Israel Daylight Time\",\"+02:00:00\",\"+01:00:00\",\"4;5;3\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"Asia/Kabul\",\"AFT\",\"Afghanistan Time\",\"\",\"\",\"+04:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kamchatka\",\"PETT\",\"Petropavlovsk-Kamchatski Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Karachi\",\"PKT\",\"Pakistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kashgar\",\"XJT\",\"Xinjiang Standard Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kathmandu\",\"NPT\",\"Nepal Time\",\"\",\"\",\"+05:45:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Katmandu\",\"NPT\",\"Nepal Time\",\"\",\"\",\"+05:45:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Khandyga\",\"YAKT\",\"Khandyga Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kolkata\",\"IST\",\"India Standard Time\",\"\",\"\",\"+05:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Krasnoyarsk\",\"KRAT\",\"Krasnoyarsk Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kuala_Lumpur\",\"MYT\",\"Malaysia Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kuching\",\"MYT\",\"Malaysia Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Kuwait\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Macao\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Macau\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Magadan\",\"MAGT\",\"Magadan Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Makassar\",\"WITA\",\"Central Indonesia Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Manila\",\"PHT\",\"Philippines Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Muscat\",\"GST\",\"Gulf Standard Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Nicosia\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Asia/Novokuznetsk\",\"KRAT\",\"Krasnoyarsk Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Novosibirsk\",\"NOVT\",\"Novosibirsk Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Omsk\",\"OMST\",\"Omsk Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Oral\",\"ORAT\",\"Oral Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Phnom_Penh\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Pontianak\",\"WIB\",\"West Indonesia Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Pyongyang\",\"KST\",\"Korea Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Qatar\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Qyzylorda\",\"QYZT\",\"Qyzylorda Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Rangoon\",\"MMT\",\"Myanmar Time\",\"\",\"\",\"+06:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Riyadh\",\"AST\",\"Arabia Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Riyadh87\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Riyadh88\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Riyadh89\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Saigon\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Sakhalin\",\"SAKT\",\"Sakhalin Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Samarkand\",\"UZT\",\"Uzbekistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Seoul\",\"KST\",\"Korea Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Shanghai\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Singapore\",\"SGT\",\"Singapore Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Srednekolymsk\",\"SRET\",\"Srednekolymsk Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Taipei\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Tashkent\",\"UZT\",\"Uzbekistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Tbilisi\",\"GET\",\"Georgia Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Tehran\",\"IRST\",\"Iran Standard Time\",\"IRDT\",\"Iran Daylight Time\",\"+03:30:00\",\"+01:00:00\",\"4;0;3\",\"+00:00:00\",\"4;0;9\",\"+00:00:00\"\n\
\"Asia/Tel_Aviv\",\"IST\",\"Israel Standard Time\",\"IDT\",\"Israel Daylight Time\",\"+02:00:00\",\"+01:00:00\",\"4;5;3\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"Asia/Thimbu\",\"BTT\",\"Bhutan Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Thimphu\",\"BTT\",\"Bhutan Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Tokyo\",\"JST\",\"Japan Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ujung_Pandang\",\"WITA\",\"Central Indonesia Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ulaanbaatar\",\"ULAT\",\"Ulaanbaatar Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ulan_Bator\",\"ULAT\",\"Ulaanbaatar Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Urumqi\",\"XJT\",\"Xinjiang Standard Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Ust-Nera\",\"VLAT\",\"Ust-Nera Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Vientiane\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Vladivostok\",\"VLAT\",\"Vladivostok Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Yakutsk\",\"YAKT\",\"Yakutsk Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Yekaterinburg\",\"YEKT\",\"Yekaterinburg Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Asia/Yerevan\",\"AMT\",\"Armenia Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Atlantic/Azores\",\"AZOT\",\"Azores Time\",\"AZOST\",\"Azores Summer Time\",\"-01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Bermuda\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Atlantic/Canary\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Cape_Verde\",\"CVT\",\"Cape Verde Time\",\"\",\"\",\"-01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Atlantic/Faeroe\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Faroe\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Jan_Mayen\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Madeira\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Atlantic/Reykjavik\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Atlantic/South_Georgia\",\"GST\",\"South Georgia Standard Time\",\"\",\"\",\"-02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Atlantic/St_Helena\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Atlantic/Stanley\",\"FKT\",\"Falkland Is. Time\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/ACT\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Adelaide\",\"ACST\",\"Australian Central Standard Time (South Australia)\",\"ACDT\",\"Australian Central Daylight Time (South Australia)\",\"+09:30:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Brisbane\",\"AEST\",\"Australian Eastern Standard Time (Queensland)\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Broken_Hill\",\"ACST\",\"Australian Central Standard Time (South Australia/New South Wales)\",\"ACDT\",\"Australian Central Daylight Time (South Australia/New South Wales)\",\"+09:30:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Canberra\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Currie\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Darwin\",\"ACST\",\"Australian Central Standard Time (Northern Territory)\",\"\",\"\",\"+09:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Eucla\",\"ACWST\",\"Australian Central Western Standard Time\",\"\",\"\",\"+08:45:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Hobart\",\"AEST\",\"Australian Eastern Standard Time (Tasmania)\",\"AEDT\",\"Australian Eastern Daylight Time (Tasmania)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/LHI\",\"LHST\",\"Lord Howe Standard Time\",\"LHDT\",\"Lord Howe Daylight Time\",\"+10:30:00\",\"+00:30:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Lindeman\",\"AEST\",\"Australian Eastern Standard Time (Queensland)\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Lord_Howe\",\"LHST\",\"Lord Howe Standard Time\",\"LHDT\",\"Lord Howe Daylight Time\",\"+10:30:00\",\"+00:30:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Melbourne\",\"AEST\",\"Australian Eastern Standard Time (Victoria)\",\"AEDT\",\"Australian Eastern Daylight Time (Victoria)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/NSW\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/North\",\"ACST\",\"Australian Central Standard Time (Northern Territory)\",\"\",\"\",\"+09:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Perth\",\"AWST\",\"Australian Western Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Queensland\",\"AEST\",\"Australian Eastern Standard Time (Queensland)\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/South\",\"ACST\",\"Australian Central Standard Time (South Australia)\",\"ACDT\",\"Australian Central Daylight Time (South Australia)\",\"+09:30:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Sydney\",\"AEST\",\"Australian Eastern Standard Time (New South Wales)\",\"AEDT\",\"Australian Eastern Daylight Time (New South Wales)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Tasmania\",\"AEST\",\"Australian Eastern Standard Time (Tasmania)\",\"AEDT\",\"Australian Eastern Daylight Time (Tasmania)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/Victoria\",\"AEST\",\"Australian Eastern Standard Time (Victoria)\",\"AEDT\",\"Australian Eastern Daylight Time (Victoria)\",\"+10:00:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Australia/West\",\"AWST\",\"Australian Western Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Australia/Yancowinna\",\"ACST\",\"Australian Central Standard Time (South Australia/New South Wales)\",\"ACDT\",\"Australian Central Daylight Time (South Australia/New South Wales)\",\"+09:30:00\",\"+01:00:00\",\"1;0;10\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"BET\",\"BRT\",\"Brasilia Time\",\"BRST\",\"Brasilia Summer Time\",\"-03:00:00\",\"+01:00:00\",\"3;0;10\",\"+00:00:00\",\"3;0;2\",\"+00:00:00\"\n\
\"BST\",\"BDT\",\"Bangladesh Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Brazil/Acre\",\"ACT\",\"Acre Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Brazil/DeNoronha\",\"FNT\",\"Fernando de Noronha Time\",\"\",\"\",\"-02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Brazil/East\",\"BRT\",\"Brasilia Time\",\"BRST\",\"Brasilia Summer Time\",\"-03:00:00\",\"+01:00:00\",\"3;0;10\",\"+00:00:00\",\"3;0;2\",\"+00:00:00\"\n\
\"Brazil/West\",\"AMT\",\"Amazon Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"CAT\",\"CAT\",\"Central African Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"CET\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"CNT\",\"NST\",\"Newfoundland Standard Time\",\"NDT\",\"Newfoundland Daylight Time\",\"-03:30:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"CST\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"CST6CDT\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"CTT\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Canada/Atlantic\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/Central\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/East-Saskatchewan\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Canada/Eastern\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/Mountain\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/Newfoundland\",\"NST\",\"Newfoundland Standard Time\",\"NDT\",\"Newfoundland Daylight Time\",\"-03:30:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/Pacific\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Canada/Saskatchewan\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Canada/Yukon\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Chile/Continental\",\"CLT\",\"Chile Time\",\"CLST\",\"Chile Summer Time\",\"-04:00:00\",\"+01:00:00\",\"1;0;9\",\"+04:00:00\",\"4;0;4\",\"+03:00:00\"\n\
\"Chile/EasterIsland\",\"EAST\",\"Easter Is. Time\",\"EASST\",\"Easter Is. Summer Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;9\",\"+04:00:00\",\"4;0;4\",\"+03:00:00\"\n\
\"Cuba\",\"CST\",\"Cuba Standard Time\",\"CDT\",\"Cuba Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+00:00:00\",\"1;0;11\",\"+00:00:00\"\n\
\"EAT\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"ECT\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"EET\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"EST\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"EST5EDT\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Egypt\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;4;4\",\"+00:00:00\",\"-1;4;9\",\"+24:00:00\"\n\
\"Eire\",\"GMT\",\"Greenwich Mean Time\",\"IST\",\"Irish Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Etc/GMT\",\"GMT+00:00\",\"GMT+00:00\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+0\",\"GMT+00:00\",\"GMT+00:00\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+1\",\"GMT-01:00\",\"GMT-01:00\",\"\",\"\",\"-01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+10\",\"GMT-10:00\",\"GMT-10:00\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+11\",\"GMT-11:00\",\"GMT-11:00\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+12\",\"GMT-12:00\",\"GMT-12:00\",\"\",\"\",\"-12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+2\",\"GMT-02:00\",\"GMT-02:00\",\"\",\"\",\"-02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+3\",\"GMT-03:00\",\"GMT-03:00\",\"\",\"\",\"-03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+4\",\"GMT-04:00\",\"GMT-04:00\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+5\",\"GMT-05:00\",\"GMT-05:00\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+6\",\"GMT-06:00\",\"GMT-06:00\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+7\",\"GMT-07:00\",\"GMT-07:00\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+8\",\"GMT-08:00\",\"GMT-08:00\",\"\",\"\",\"-08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT+9\",\"GMT-09:00\",\"GMT-09:00\",\"\",\"\",\"-09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-0\",\"GMT+00:00\",\"GMT+00:00\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-1\",\"GMT+01:00\",\"GMT+01:00\",\"\",\"\",\"+01:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-10\",\"GMT+10:00\",\"GMT+10:00\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-11\",\"GMT+11:00\",\"GMT+11:00\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-12\",\"GMT+12:00\",\"GMT+12:00\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-13\",\"GMT+13:00\",\"GMT+13:00\",\"\",\"\",\"+13:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-14\",\"GMT+14:00\",\"GMT+14:00\",\"\",\"\",\"+14:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-2\",\"GMT+02:00\",\"GMT+02:00\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-3\",\"GMT+03:00\",\"GMT+03:00\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-4\",\"GMT+04:00\",\"GMT+04:00\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-5\",\"GMT+05:00\",\"GMT+05:00\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-6\",\"GMT+06:00\",\"GMT+06:00\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-7\",\"GMT+07:00\",\"GMT+07:00\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-8\",\"GMT+08:00\",\"GMT+08:00\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT-9\",\"GMT+09:00\",\"GMT+09:00\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/GMT0\",\"GMT+00:00\",\"GMT+00:00\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/Greenwich\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/UCT\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/UTC\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/Universal\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Etc/Zulu\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Amsterdam\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Andorra\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Athens\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Belfast\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Belgrade\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Berlin\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Bratislava\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Brussels\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Bucharest\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Budapest\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Busingen\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Chisinau\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Copenhagen\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Dublin\",\"GMT\",\"Greenwich Mean Time\",\"IST\",\"Irish Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Gibraltar\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Guernsey\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Helsinki\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Isle_of_Man\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Istanbul\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Jersey\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Kaliningrad\",\"EET\",\"Eastern European Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Kiev\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Lisbon\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Ljubljana\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/London\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Luxembourg\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Madrid\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Malta\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Mariehamn\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Minsk\",\"MSK\",\"Moscow Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Monaco\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Moscow\",\"MSK\",\"Moscow Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Nicosia\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Oslo\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Paris\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Podgorica\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Prague\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Riga\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Rome\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Samara\",\"SAMT\",\"Samara Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/San_Marino\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Sarajevo\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Simferopol\",\"MSK\",\"Moscow Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Skopje\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Sofia\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Stockholm\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Tallinn\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Tirane\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Tiraspol\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Uzhgorod\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Vaduz\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Vatican\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Vienna\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Vilnius\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Volgograd\",\"MSK\",\"Moscow Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Europe/Warsaw\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Zagreb\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Zaporozhye\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Europe/Zurich\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"GB\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"GB-Eire\",\"GMT\",\"Greenwich Mean Time\",\"BST\",\"British Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"GMT\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"GMT0\",\"GMT+00:00\",\"GMT+00:00\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Greenwich\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"HST\",\"HST\",\"Hawaii Standard Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Hongkong\",\"HKT\",\"Hong Kong Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"IET\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"IST\",\"IST\",\"India Standard Time\",\"\",\"\",\"+05:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Iceland\",\"GMT\",\"Greenwich Mean Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Antananarivo\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Chagos\",\"IOT\",\"Indian Ocean Territory Time\",\"\",\"\",\"+06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Christmas\",\"CXT\",\"Christmas Island Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Cocos\",\"CCT\",\"Cocos Islands Time\",\"\",\"\",\"+06:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Comoro\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Kerguelen\",\"TFT\",\"French Southern & Antarctic Lands Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Mahe\",\"SCT\",\"Seychelles Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Maldives\",\"MVT\",\"Maldives Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Mauritius\",\"MUT\",\"Mauritius Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Mayotte\",\"EAT\",\"Eastern African Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Indian/Reunion\",\"RET\",\"Reunion Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Iran\",\"IRST\",\"Iran Standard Time\",\"IRDT\",\"Iran Daylight Time\",\"+03:30:00\",\"+01:00:00\",\"4;0;3\",\"+00:00:00\",\"4;0;9\",\"+00:00:00\"\n\
\"Israel\",\"IST\",\"Israel Standard Time\",\"IDT\",\"Israel Daylight Time\",\"+02:00:00\",\"+01:00:00\",\"4;5;3\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"JST\",\"JST\",\"Japan Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Jamaica\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Japan\",\"JST\",\"Japan Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Kwajalein\",\"MHT\",\"Marshall Islands Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Libya\",\"EET\",\"Eastern European Time\",\"\",\"\",\"+02:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"MET\",\"MET\",\"Middle Europe Time\",\"MEST\",\"Middle Europe Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"MIT\",\"WSST\",\"West Samoa Standard Time\",\"WSDT\",\"West Samoa Daylight Time\",\"+13:00:00\",\"+01:00:00\",\"-1;0;9\",\"+03:00:00\",\"1;0;4\",\"+04:00:00\"\n\
\"MST\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"MST7MDT\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Mexico/BajaNorte\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Mexico/BajaSur\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"Mexico/General\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"Mideast/Riyadh87\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Mideast/Riyadh88\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Mideast/Riyadh89\",\"GMT+03:07\",\"GMT+03:07\",\"\",\"\",\"+03:07:04\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"NET\",\"AMT\",\"Armenia Time\",\"\",\"\",\"+04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"NST\",\"NZST\",\"New Zealand Standard Time\",\"NZDT\",\"New Zealand Daylight Time\",\"+12:00:00\",\"+01:00:00\",\"-1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"NZ\",\"NZST\",\"New Zealand Standard Time\",\"NZDT\",\"New Zealand Daylight Time\",\"+12:00:00\",\"+01:00:00\",\"-1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"NZ-CHAT\",\"CHAST\",\"Chatham Standard Time\",\"CHADT\",\"Chatham Daylight Time\",\"+12:45:00\",\"+01:00:00\",\"-1;0;9\",\"+02:45:00\",\"1;0;4\",\"+02:45:00\"\n\
\"Navajo\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"PLT\",\"PKT\",\"Pakistan Time\",\"\",\"\",\"+05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"PNT\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"PRC\",\"CST\",\"China Standard Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"PRT\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"PST\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"PST8PDT\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"Pacific/Apia\",\"WSST\",\"West Samoa Standard Time\",\"WSDT\",\"West Samoa Daylight Time\",\"+13:00:00\",\"+01:00:00\",\"-1;0;9\",\"+03:00:00\",\"1;0;4\",\"+04:00:00\"\n\
\"Pacific/Auckland\",\"NZST\",\"New Zealand Standard Time\",\"NZDT\",\"New Zealand Daylight Time\",\"+12:00:00\",\"+01:00:00\",\"-1;0;9\",\"+02:00:00\",\"1;0;4\",\"+02:00:00\"\n\
\"Pacific/Bougainville\",\"BST\",\"Bougainville Standard Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Chatham\",\"CHAST\",\"Chatham Standard Time\",\"CHADT\",\"Chatham Daylight Time\",\"+12:45:00\",\"+01:00:00\",\"-1;0;9\",\"+02:45:00\",\"1;0;4\",\"+02:45:00\"\n\
\"Pacific/Chuuk\",\"CHUT\",\"Chuuk Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Easter\",\"EAST\",\"Easter Is. Time\",\"EASST\",\"Easter Is. Summer Time\",\"-06:00:00\",\"+01:00:00\",\"1;0;9\",\"+04:00:00\",\"4;0;4\",\"+03:00:00\"\n\
\"Pacific/Efate\",\"VUT\",\"Vanuatu Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Enderbury\",\"PHOT\",\"Phoenix Is. Time\",\"\",\"\",\"+13:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Fakaofo\",\"TKT\",\"Tokelau Time\",\"\",\"\",\"+13:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Fiji\",\"FJT\",\"Fiji Time\",\"FJST\",\"Fiji Summer Time\",\"+12:00:00\",\"+01:00:00\",\"1;0;11\",\"+02:00:00\",\"3;0;1\",\"+03:00:00\"\n\
\"Pacific/Funafuti\",\"TVT\",\"Tuvalu Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Galapagos\",\"GALT\",\"Galapagos Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Gambier\",\"GAMT\",\"Gambier Time\",\"\",\"\",\"-09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Guadalcanal\",\"SBT\",\"Solomon Is. Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Guam\",\"ChST\",\"Chamorro Standard Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Honolulu\",\"HST\",\"Hawaii Standard Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Johnston\",\"HST\",\"Hawaii Standard Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Kiritimati\",\"LINT\",\"Line Is. Time\",\"\",\"\",\"+14:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Kosrae\",\"KOST\",\"Kosrae Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Kwajalein\",\"MHT\",\"Marshall Islands Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Majuro\",\"MHT\",\"Marshall Islands Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Marquesas\",\"MART\",\"Marquesas Time\",\"\",\"\",\"-09:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Midway\",\"SST\",\"Samoa Standard Time\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Nauru\",\"NRT\",\"Nauru Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Niue\",\"NUT\",\"Niue Time\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Norfolk\",\"NFT\",\"Norfolk Time\",\"\",\"\",\"+11:30:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Noumea\",\"NCT\",\"New Caledonia Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Pago_Pago\",\"SST\",\"Samoa Standard Time\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Palau\",\"PWT\",\"Palau Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Pitcairn\",\"PST\",\"Pitcairn Standard Time\",\"\",\"\",\"-08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Pohnpei\",\"PONT\",\"Pohnpei Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Ponape\",\"PONT\",\"Pohnpei Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Port_Moresby\",\"PGT\",\"Papua New Guinea Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Rarotonga\",\"CKT\",\"Cook Is. Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Saipan\",\"ChST\",\"Chamorro Standard Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Samoa\",\"SST\",\"Samoa Standard Time\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Tahiti\",\"TAHT\",\"Tahiti Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Tarawa\",\"GILT\",\"Gilbert Is. Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Tongatapu\",\"TOT\",\"Tonga Time\",\"\",\"\",\"+13:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Truk\",\"CHUT\",\"Chuuk Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Wake\",\"WAKT\",\"Wake Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Wallis\",\"WFT\",\"Wallis & Futuna Time\",\"\",\"\",\"+12:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Pacific/Yap\",\"CHUT\",\"Chuuk Time\",\"\",\"\",\"+10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Poland\",\"CET\",\"Central European Time\",\"CEST\",\"Central European Summer Time\",\"+01:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Portugal\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"ROK\",\"KST\",\"Korea Standard Time\",\"\",\"\",\"+09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SST\",\"SBT\",\"Solomon Is. Time\",\"\",\"\",\"+11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Singapore\",\"SGT\",\"Singapore Time\",\"\",\"\",\"+08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/AST4\",\"AST\",\"Atlantic Standard Time\",\"\",\"\",\"-04:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/AST4ADT\",\"AST\",\"Atlantic Standard Time\",\"ADT\",\"Atlantic Daylight Time\",\"-04:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"SystemV/CST6\",\"CST\",\"Central Standard Time\",\"\",\"\",\"-06:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/CST6CDT\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"SystemV/EST5\",\"EST\",\"Eastern Standard Time\",\"\",\"\",\"-05:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/EST5EDT\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"SystemV/HST10\",\"HST\",\"Hawaii Standard Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/MST7\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/MST7MDT\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"SystemV/PST8\",\"PST\",\"Pacific Standard Time\",\"\",\"\",\"-08:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/PST8PDT\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"SystemV/YST9\",\"AKST\",\"Alaska Standard Time\",\"\",\"\",\"-09:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"SystemV/YST9YDT\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"-1;0;4\",\"+02:00:00\",\"-1;0;10\",\"+02:00:00\"\n\
\"Turkey\",\"EET\",\"Eastern European Time\",\"EEST\",\"Eastern European Summer Time\",\"+02:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"UCT\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"US/Alaska\",\"AKST\",\"Alaska Standard Time\",\"AKDT\",\"Alaska Daylight Time\",\"-09:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Aleutian\",\"HAST\",\"Hawaii-Aleutian Standard Time\",\"HADT\",\"Hawaii-Aleutian Daylight Time\",\"-10:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Arizona\",\"MST\",\"Mountain Standard Time\",\"\",\"\",\"-07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"US/Central\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/East-Indiana\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Eastern\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Hawaii\",\"HST\",\"Hawaii Standard Time\",\"\",\"\",\"-10:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"US/Indiana-Starke\",\"CST\",\"Central Standard Time\",\"CDT\",\"Central Daylight Time\",\"-06:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Michigan\",\"EST\",\"Eastern Standard Time\",\"EDT\",\"Eastern Daylight Time\",\"-05:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Mountain\",\"MST\",\"Mountain Standard Time\",\"MDT\",\"Mountain Daylight Time\",\"-07:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Pacific\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Pacific-New\",\"PST\",\"Pacific Standard Time\",\"PDT\",\"Pacific Daylight Time\",\"-08:00:00\",\"+01:00:00\",\"2;0;3\",\"+02:00:00\",\"1;0;11\",\"+02:00:00\"\n\
\"US/Samoa\",\"SST\",\"Samoa Standard Time\",\"\",\"\",\"-11:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"UTC\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"Universal\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"VST\",\"ICT\",\"Indochina Time\",\"\",\"\",\"+07:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"W-SU\",\"MSK\",\"Moscow Standard Time\",\"\",\"\",\"+03:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"\n\
\"WET\",\"WET\",\"Western European Time\",\"WEST\",\"Western European Summer Time\",\"+00:00:00\",\"+01:00:00\",\"-1;0;3\",\"+01:00:00\",\"-1;0;10\",\"+01:00:00\"\n\
\"Zulu\",\"UTC\",\"Coordinated Universal Time\",\"\",\"\",\"+00:00:00\",\"+00:00:00\",\"\",\"\",\"\",\"\"";

Status TimezoneDatabase::Initialize() {
  // Create a temporary file and write the timezone information.  The boost
  // interface only loads this format from a file. We abort the startup if
  // this initialization fails for some reason.
  string pathname = (boost::filesystem::path(FLAGS_local_library_dir) /
      string("impala.tzdb.XXXXXXX")).string();
  // mkstemp operates in place, so we need a mutable array.
  std::vector<char> filestr(pathname.c_str(), pathname.c_str() + pathname.size() + 1);
  FILE* file;
  int fd;
  if ((fd = mkstemp(filestr.data())) == -1) {
    return Status(Substitute("Could not create temporary timezone file: $0. Check that "
        "the directory $1 is writable by the user running Impala.", filestr.data(),
        FLAGS_local_library_dir));
  }
  if ((file = fopen(filestr.data(), "w")) == NULL) {
    unlink(filestr.data());
    close(fd);
    return Status(Substitute("Could not open temporary timezone file: $0",
      filestr.data()));
  }
  if (fputs(TIMEZONE_DATABASE_STR, file) == EOF) {
    unlink(filestr.data());
    close(fd);
    fclose(file);
    return Status(Substitute("Could not load temporary timezone file: $0",
      filestr.data()));
  }
  fclose(file);
  tz_database_.load_from_file(string(filestr.data()));
  tz_region_list_ = tz_database_.region_list();
  unlink(filestr.data());
  close(fd);
  return Status::OK();
}

}
