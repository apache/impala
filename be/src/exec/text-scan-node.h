// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_TEXT_SCAN_NODE_H_
#define IMPALA_EXEC_TEXT_SCAN_NODE_H_

#include <vector>
#include <fstream>
#include <boost/tokenizer.hpp>
#include "exec-node.h"
#include "runtime/columntype.h"
#include "runtime/row-batch.h"

namespace impala {
namespace exec {

class TextScanNode : public ExecNode {
public:
  TextScanNode(const std::vector<ColumnType>& schema, 
		 const std::vector<unsigned>& target_offsets,
		 size_t out_record_size,
		 char row_delim = '\n', 
		 char field_delim = ',', 
		 char coll_item_delim = ';')
    : schema_(schema), 
      target_offs_(target_offsets),
      out_record_size_(out_record_size),
      row_delim(row_delim), 
      field_delim_(field_delim), 
      coll_item_delim_(coll_item_delim), 
      flen_out_buf_(NULL), 
      flen_out_buf_size_(0),      
      vlen_out_buf_(NULL),
      vlen_out_buf_size_(0),
      flen_out_runner_(NULL),
      vlen_out_runner_(NULL),
      els(NULL)
  {}
  
  Status Prepare(RuntimeState* state) {
		std::string escape_indicator("\\");
		std::string field_delims;
		field_delims.append(1, field_delim_);
		field_delims.append(1, coll_item_delim_);
		std::string quote_chars("\"\'");
		els = new boost::escaped_list_separator<char>(escape_indicator, field_delims, quote_chars);

		// TODO: need to call SetOutput from here?
		return 0;
	}

  void SetOutput(char* __restrict__ flen_out_buf, unsigned flen_out_buf_size, char* __restrict__ vlen_out_buf, unsigned vlen_out_buf_size) {
    flen_out_buf_ = flen_out_buf;
    flen_out_buf_size_ = flen_out_buf_size;
    vlen_out_buf_ = vlen_out_buf;
    vlen_out_buf_size_ = vlen_out_buf_size;
  }
  
  Status GetNext(RuntimeState* state, RowBatch* row_batch);

  unsigned Parse(std::ifstream& in);
  void Print(unsigned num_records);

  Status Close(RuntimeState* state) {
		delete els;
		return 0;
	}

private:
	const std::vector<ColumnType>& schema_;
	const std::vector<unsigned>& target_offs_;
	const size_t out_record_size_;
	const char row_delim;
	const char field_delim_;
	const char coll_item_delim_;

	char* __restrict__ flen_out_buf_;
	unsigned flen_out_buf_size_;
	char* __restrict__ vlen_out_buf_;
	unsigned vlen_out_buf_size_;

	char* flen_out_runner_;
	char* vlen_out_runner_;

	boost::escaped_list_separator<char>* els;

	void WriteField(unsigned field_ix, const std::string& str,
			char* __restrict__ fixed_len_target);
	void PrintField(unsigned field_ix, void* __restrict__ tuple_off);
};

} // namespace exec
} // namespace impala

#endif
