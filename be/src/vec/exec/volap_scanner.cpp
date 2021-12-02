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

#include "vec/exec/volap_scanner.h"

#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/block_reader.h"

namespace doris::vectorized {

VOlapScanner::VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                           bool need_agg_finalize, const TPaloScanRange& scan_range)
        : OlapScanner(runtime_state, parent, aggregation, need_agg_finalize, scan_range) {
    _reader.reset(new BlockReader);
}

Status VOlapScanner::get_block(RuntimeState* state, vectorized::Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
        }
    }

    do {
        // Read one block from block reader
        auto res = _reader->next_block_with_aggregation(block, nullptr, nullptr, eof);
        if (res != OLAP_SUCCESS) {
            std::stringstream ss;
            ss << "Internal Error: read storage fail. res=" << res
               << ", tablet=" << _tablet->full_name()
               << ", backend=" << BackendOptions::get_localhost();
            return Status::InternalError(ss.str());
        }
        _num_rows_read += block->rows();
        _update_realtime_counter();

        // If we reach end of this scanner, break
        if (UNLIKELY(*eof)) {
            break;
        }

        VLOG_ROW << "VOlapScanner input row: " << _read_row_cursor.to_string();

        RETURN_IF_ERROR(
                VExprContext::filter_block(_vconjunct_ctx, block, _tuple_desc->slots().size()));

    } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold);

    return Status::OK();
}

} // namespace doris::vectorized
