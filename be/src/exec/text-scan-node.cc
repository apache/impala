// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <stdint.h>
#include "exec/text-scan-node.h"

using namespace std;
using namespace boost;
using namespace impala::exec;

//using boost::lexical_cast;
//using boost::bad_lexical_cast;

unsigned TextScanNode::Parse(ifstream& in) {
	unsigned num_records = 0;
	typedef tokenizer<escaped_list_separator<char> > Tokenizer;
	flen_out_runner_ = flen_out_buf_;
	vlen_out_runner_ = vlen_out_buf_;
	string line;
	while (getline(in, line, row_delim)) {
		Tokenizer tok(line, *els);
		unsigned field_ix = 0;
		vector<unsigned>::const_iterator offset = target_offs_.begin();
		for (Tokenizer::iterator token = tok.begin(); token != tok.end(); ++token) {
			cout << *token << endl;
			WriteField(field_ix++, *token, flen_out_runner_ + *offset++);
		}
		flen_out_runner_ += out_record_size_;
		++num_records;
	}
	return num_records;
}

void TextScanNode::WriteField(unsigned field_ix, const string& str,
		char* __restrict__ flen_target) {
	switch (schema_[field_ix]) {
		case BOOLEAN: {
			// bool b = lexical_cast<bool> (str);
			// boost lexical cast does not work for 'true' or 'false' strings
			std::istringstream is(str);
			is >> std::boolalpha >> *reinterpret_cast<char*> (flen_target);
			break;
		}
		case TINYINT: {
			*reinterpret_cast<char*> (flen_target) = lexical_cast<char> (str);
			break;
		}
		case SMALLINT: {
			*reinterpret_cast<short*> (flen_target) = lexical_cast<short> (str);
			break;
		}
		case INT: {
			*reinterpret_cast<int*> (flen_target) = lexical_cast<int> (str);
			break;
		}
		case BIGINT: {
			*reinterpret_cast<long*> (flen_target) = lexical_cast<long> (str);
			break;
		}
		case FLOAT: {
			*reinterpret_cast<float*> (flen_target) = lexical_cast<float> (str);
			break;
		}
		case DOUBLE: {
			*reinterpret_cast<double*> (flen_target) = lexical_cast<double> (str);
			break;
		}
		case STRING: {
			// add pointer as fixed-length field
			*reinterpret_cast<void**> (flen_target) = vlen_out_runner_;
			// copy string to vlen buffer
			int len = str.length();
			// length indicator
			*reinterpret_cast<int*> (vlen_out_runner_) = len;
			vlen_out_runner_ += sizeof(int);
			// string data, omitting quotes
			memcpy(vlen_out_runner_, str.data() + 1, len - 2);
			vlen_out_runner_ += len;
			break;
		}
	}
}

void TextScanNode::Print(unsigned num_records) {
	flen_out_runner_ = flen_out_buf_;
	vlen_out_runner_ = vlen_out_buf_;
	unsigned num_fields = target_offs_.size();
	for (unsigned i = 0; i < num_records; ++i) {
		vector<unsigned>::const_iterator offset = target_offs_.begin();
		for (unsigned j = 0; j < num_fields; ++j) {
			PrintField(j, flen_out_runner_ + *offset++);
		}
		flen_out_runner_ += out_record_size_;
		cout << "----------" << endl;
	}
}

void TextScanNode::PrintField(unsigned field_ix, void* __restrict__ field_start) {
	switch (schema_[field_ix]) {
		case BOOLEAN: {
			cout << "BOOLEAN: " << *reinterpret_cast<bool*> (field_start) << endl;
			break;
		}
		case TINYINT: {
			cout << "TINYINT: " << (int) (*reinterpret_cast<char*> (field_start))
					<< endl;
			break;
		}
		case SMALLINT: {
			cout << "SMALLINT: " << (short) (*reinterpret_cast<short*> (field_start))
					<< endl;
			break;
		}
		case INT: {
			cout << "SMALLINT: " << (int) (*reinterpret_cast<int*> (field_start))
					<< endl;
			break;
		}
		case BIGINT: {
			cout << "BIGINT: " << (long) (*reinterpret_cast<long*> (field_start))
					<< endl;
			break;
		}
		case FLOAT: {
			cout << "FLOAT: " << (float) (*reinterpret_cast<float*> (field_start))
					<< endl;
			break;
		}
		case DOUBLE: {
			cout << "FLOAT: " << (double) (*reinterpret_cast<double*> (field_start))
					<< endl;
			break;
		}
		case STRING: {
			void* vlen_str_loc = *reinterpret_cast<void**> (field_start);
			int len = *reinterpret_cast<int*> (vlen_str_loc);
			string s;
			s.assign(reinterpret_cast<char*> (vlen_str_loc) + sizeof(int), len);
			cout << "STRING: " << "'" << s << "'" << endl;
			break;
		}
	}
}
