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

#include "vec/olap/block_reader.h"

#include <parallel_hashmap/phmap.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_set>

#include "olap/row_block.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/date_func.h"
#include "vec/olap/vcollect_iterator.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris::vectorized {

BlockReader::BlockReader() : _collect_iter(new VCollectIterator()) {}

BlockReader::~BlockReader() {
    for (int i = 0; i < _agg_functions.size(); ++i) {
        AggregateFunctionPtr function = _agg_functions[i];
        AggregateDataPtr place = _agg_places[i];
        function->destroy(place);
        delete[] place;
    }
}

OLAPStatus BlockReader::_init_collect_iter(const ReaderParams& read_params,
                                           std::vector<RowsetReaderSharedPtr>* valid_rs_readers,
                                           bool is_agg) {
    _collect_iter->init(this);
    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto res = _capture_rs_readers(read_params, &rs_readers);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    for (auto& rs_reader : rs_readers) {
        RETURN_NOT_OK(rs_reader->init(&_reader_context));
        OLAPStatus res = _collect_iter->add_child(rs_reader);
        if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to add child to iterator, err=" << res;
            return res;
        }
        if (res == OLAP_SUCCESS) {
            valid_rs_readers->push_back(rs_reader);
        }
    }

    _collect_iter->build_heap(*valid_rs_readers);
    if (_collect_iter->is_merge()) {
        auto status = _collect_iter->current_row(&_next_row.first, &_next_row.second);
        _eof = status == OLAP_ERR_DATA_EOF;

        if (!_eof) {
            _unique_key_tmp_block = _next_row.first->create_same_struct_block(1);
            _unique_row_columns = _unique_key_tmp_block->mutate_columns();

            _stored_value_block = _next_row.first->create_same_struct_block(_batch_size);
            _stored_value_columns = _stored_value_block->mutate_columns();

            _replace_data_in_column();

            if (is_agg) {
                for (auto idx : _agg_columns_idx) {
                    FieldAggregationMethod agg_method =
                            tablet()->tablet_schema().column(idx).aggregation();
                    std::string agg_name = TabletColumn::get_string_by_aggregation_type(agg_method);
                    std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
                                   [](unsigned char c) { return std::tolower(c); });

                    if (agg_name == "sum") {
                        agg_name += "_pure";
                    }

                    DataTypes argument_types;
                    argument_types.push_back(_unique_key_tmp_block->get_data_types()[idx]);
                    Array params;

                    AggregateFunctionPtr function = AggregateFunctionSimpleFactory::instance().get(
                            agg_name, argument_types, params,
                            _unique_key_tmp_block->get_data_types()[idx]->is_nullable());
                    _agg_functions.push_back(function);
                    // TODO: maybe allocate once?
                    AggregateDataPtr place = new char[function->size_of_data()];
                    function->create(place);
                    _agg_places.push_back(place);
                }

                _append_agg_data_in_column();
            }
        }
    }
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::init(const ReaderParams& read_params) {
    Reader::init(read_params);

    _key_num = tablet()->num_key_columns();

    auto return_column_size =
            read_params.origin_return_columns->size() - (_sequence_col_idx != -1 ? 1 : 0);
    for (int i = 0; i < return_column_size; ++i) {
        auto cid = read_params.origin_return_columns->at(i);
        for (int j = 0; j < read_params.return_columns.size(); ++j) {
            if (read_params.return_columns[j] == cid) {
                if (j < _key_num || _tablet->keys_type() != AGG_KEYS) {
                    _normal_columns_idx.emplace_back(i);
                } else {
                    _agg_columns_idx.emplace_back(i);
                }
                _return_columns_loc.emplace_back(j);
                break;
            }
        }
    }

    _batch_size = read_params.runtime_state->batch_size();
    _need_compare = !read_params.single_version;

    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto status = _init_collect_iter(read_params, &rs_readers, _tablet->keys_type() == AGG_KEYS);
    if (status != OLAP_SUCCESS) {
        return status;
    }

    //  TODO: Support the logic in the future
    //    if (_optimize_for_single_rowset(rs_readers)) {
    //        _next_block_func = _tablet->keys_type() == AGG_KEYS ? &BlockReader::_direct_agg_key_next_block
    //                                                          : &BlockReader::_direct_next_block;
    //        return OLAP_SUCCESS;
    //    }

    switch (_tablet->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &BlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        _next_block_func = &BlockReader::_unique_key_next_block;
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &BlockReader::_agg_key_next_block;
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet->keys_type();
        break;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                           bool* eof) {
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_direct_agg_key_next_block(Block* block, MemPool* mem_pool,
                                                   ObjectPool* agg_pool, bool* eof) {
    auto res = _collect_iter->next(block);
    if (UNLIKELY(res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF)) {
        return res;
    }
    *eof = res == OLAP_ERR_DATA_EOF;
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                            bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return OLAP_SUCCESS;
    }

    int64_t merged_count = 0;
    auto target_block_row = 0;
    auto target_columns = block->mutate_columns();
    do {
        auto res = _collect_iter->next(&_next_row.first, &_next_row.second);
        if (UNLIKELY(res == OLAP_ERR_DATA_EOF)) {
            _update_value_in_column();
            _insert_tmp_block_to(target_columns);
            *eof = true;
            break;
        }

        if (UNLIKELY(res != OLAP_SUCCESS)) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }

        auto cmp_res =
                _need_compare && _unique_key_tmp_block->compare_at(0, _next_row.second, _key_num,
                                                                   *_next_row.first, -1) == 0;

        if (cmp_res) {
            merged_count++;
        } else {
            _update_value_in_column();
            _insert_tmp_block_to(target_columns);
            target_block_row++;
            _replace_data_in_column();
        }
        _append_agg_data_in_column();

    } while (target_block_row < _batch_size);

    _merged_rows += merged_count;
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_unique_key_next_block(Block* block, MemPool* mem_pool,
                                               ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return OLAP_SUCCESS;
    }

    int64_t merged_count = 0;
    auto target_block_row = 0;
    auto target_columns = block->mutate_columns();
    do {
        // the version is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        auto res = _collect_iter->next(&_next_row.first, &_next_row.second);
        if (UNLIKELY(res == OLAP_ERR_DATA_EOF)) {
            _insert_tmp_block_to(target_columns);
            *eof = true;
            break;
        }

        if (UNLIKELY(res != OLAP_SUCCESS)) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }

        auto cmp_res =
                _need_compare && _unique_key_tmp_block->compare_at(0, _next_row.second, _key_num,
                                                                   *_next_row.first, -1) == 0;

        if (cmp_res) {
            merged_count++;
        } else {
            _insert_tmp_block_to(target_columns);
            target_block_row++;
            _replace_data_in_column();
        }
    } while (target_block_row < _batch_size);

    _merged_rows += merged_count;
    return OLAP_SUCCESS;
}

void BlockReader::_insert_tmp_block_to(doris::vectorized::MutableColumns& columns) {
    for (auto idx : _normal_columns_idx) {
        columns[idx]->insert_from(*_unique_row_columns[_return_columns_loc[idx]], 0);
    }

    for (int i = 0; i < _agg_columns_idx.size(); i++) {
        AggregateFunctionPtr function = _agg_functions[i];
        AggregateDataPtr place = _agg_places[i];

        function->insert_result_into(place, *columns[_agg_columns_idx[i]]);

        // reset aggregate data
        function->create(place);
    }
}

void BlockReader::_update_value_in_column() {
    if (!_stored_row_ref.size()) {
        return;
    }

    for (int i = 0; i < _agg_columns_idx.size(); i++) {
        AggregateFunctionPtr function = _agg_functions[i];
        AggregateDataPtr place = _agg_places[i];

        int begin = 0;
        int end = 1;

        auto add_batch = [&]() {
            auto column_ptr =
                    _stored_row_ref[begin].first->get_by_position(_agg_columns_idx[i]).column_raw;
            function->add_batch_range(_stored_row_ref[begin].second,
                                      _stored_row_ref[end - 1].second, place,
                                      const_cast<const IColumn**>(&column_ptr), nullptr);
        };

        for (; end < _stored_row_ref.size(); end++) {
            if (_stored_row_ref[end].first != _stored_row_ref[begin].first) {
                add_batch();
                begin = end;
            }
        }
        add_batch();
    }

    _stored_row_ref.clear();
}

void BlockReader::_replace_data_in_column() {
    for (auto idx : _normal_columns_idx) {
        _unique_row_columns[idx]->replace_column_data(
                *(_next_row.first)->get_by_position(idx).column, _next_row.second);
    }
}

void BlockReader::_append_agg_data_in_column() {
    _stored_row_ref.emplace_back(_next_row);

    bool is_last = _next_row.first->rows() == _next_row.second + 1;
    if (_stored_row_ref.size() == _batch_size || is_last) {
        _update_value_in_column();

        if (is_last) {
            for (int i = 0; i < _agg_columns_idx.size(); i++) {
                AggregateFunctionPtr function = _agg_functions[i];
                function->erase_has_null(
                        _next_row.first->get_by_position(_agg_columns_idx[i]).column_raw);
            }
        }
    }
}

} // namespace doris::vectorized
