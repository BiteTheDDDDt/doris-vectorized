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

#pragma once

#include <gen_cpp/PaloInternalService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "exprs/bloomfilter_predicate.h"
#include "olap/collect_iterator.h"
#include "olap/column_predicate.h"
#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/common/cow.h"

namespace doris {

class Tablet;
class RowCursor;
class RowBlock;
class RuntimeState;

namespace vectorized {
class BlockReader final : public Reader {
public:
    using TempRowRef = std::pair<const Block*, uint32_t>;

    BlockReader();

    ~BlockReader();

    // Initialize BlockReader with tablet, data version and fetch range.
    OLAPStatus init(const ReaderParams& read_params) override;

    OLAPStatus next_row_with_aggregation(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) override {
        return OLAP_ERR_READER_INITIALIZE_ERROR;
    }

    OLAPStatus next_block_with_aggregation(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                           bool* eof) override {
        return (this->*_next_block_func)(block, mem_pool, agg_pool, eof);
    }

private:
    friend class VCollectIterator;
    friend class DeleteHandler;

    // Direcly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    OLAPStatus _direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof);
    // Just same as _direct_next_block, but this is only for AGGREGATE KEY tables.
    // And this is an optimization for AGGR tables.
    // When there is only one rowset and is not overlapping, we can read it directly without aggregation.
    OLAPStatus _direct_agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                          bool* eof);
    // For normal AGGREGATE KEY tables, read data by a merge heap.
    OLAPStatus _agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                   bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    OLAPStatus _unique_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                      bool* eof);

    OLAPStatus _init_collect_iter(const ReaderParams& read_params,
                                  std::vector<RowsetReaderSharedPtr>* valid_rs_readers,
                                  bool is_agg);

    void _insert_data(MutableColumns& columns);

    void _update_value_in_column();

    void _append_agg_data_in_column();

private:
    std::unique_ptr<VCollectIterator> _collect_iter;

    std::vector<AggregateFunctionPtr> _agg_functions;
    std::vector<AggregateDataPtr> _agg_places;

    std::vector<uint32_t> _normal_columns_idx;
    std::vector<uint32_t> _agg_columns_idx;
    std::vector<uint32_t> _return_columns_loc;

    TempRowRef _next_row {nullptr, 0};
    std::vector<TempRowRef> _stored_row_ref;

    std::unique_ptr<Block> _unique_key_tmp_block;
    MutableColumns _unique_row_columns;

    int _agg_data_num = 0; // number of stored agg data
    std::unique_ptr<Block> _stored_value_block;
    MutableColumns _stored_value_columns;

    int _batch_size;
    int _key_num; // number of key columns
    bool _need_compare = true;
    bool _eof = false;

    OLAPStatus (BlockReader::*_next_block_func)(Block* block, MemPool* mem_pool,
                                                ObjectPool* agg_pool, bool* eof) = nullptr;
};

} // namespace vectorized
} // namespace doris
