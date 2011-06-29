// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "text-scan-node.h"
#include <iostream>

using namespace std;

int main() {
	vector<ColumnType> schema;
	schema.push_back(INT);
	schema.push_back(BOOLEAN);
	schema.push_back(INT);
	schema.push_back(INT);
	schema.push_back(INT);
	schema.push_back(INT);
	schema.push_back(FLOAT);
	schema.push_back(FLOAT);
	schema.push_back(STRING);
	schema.push_back(STRING);

	vector<unsigned> targetOffsets;
	targetOffsets.push_back(0);
	targetOffsets.push_back(4);
	targetOffsets.push_back(8);
	targetOffsets.push_back(12);
	targetOffsets.push_back(16);
	targetOffsets.push_back(20);
	targetOffsets.push_back(24);
	targetOffsets.push_back(28);
	targetOffsets.push_back(36);
	targetOffsets.push_back(44);

	unsigned outRecordSize = 52;

	unsigned fixedLenOutBufSize = 65536;
	unsigned varLenOutBufSize = 65536;
	char* fixedLenOutBuf = new char[fixedLenOutBufSize];
	char* varLenOutBuf = new char[varLenOutBufSize];

	std::string data = "data.csv";
	ifstream in(data.c_str());
	if (!in.is_open())
		return 1;

	TextScanNode parser(schema, targetOffsets, outRecordSize);
	parser.SetOutput(fixedLenOutBuf, fixedLenOutBufSize, varLenOutBuf,
			varLenOutBufSize);

	unsigned recordsParsed = parser.Parse(in);
	cout << "RECORDS PARSED: " << recordsParsed << endl;
	parser.Print(recordsParsed);

	in.close();

	delete[] fixedLenOutBuf;
	delete[] varLenOutBuf;

	return 0;
}
