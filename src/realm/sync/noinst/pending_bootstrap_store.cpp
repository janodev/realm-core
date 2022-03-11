/*************************************************************************
 *
 * Copyright 2022 Realm, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "realm/sync/noinst/pending_bootstrap_store.hpp"

#include "realm/list.hpp"

namespace realm::sync {
namespace {
constexpr static std::string_view c_pending_bootstrap_table("flx_pending_bootstrap");
constexpr static std::string_view c_pending_changesets_table("flx_pending_bootstrap_changesets");
constexpr static std::string_view c_pending_bootstrap_changesets("changesets");
constexpr static std::string_view c_pending_bootstrap_total_size("total_size");
constexpr static std::string_view c_pending_changesets_server_version("server_version");
constexpr static std::string_view c_pending_changesets_client_version("client_version");
constexpr static std::string_view c_pending_changesets_origin_timestamp("origin_timestamp");
constexpr static std::string_view c_pending_changesets_origin_fileident("origin_file_ident");
constexpr static std::string_view c_pending_changesets_data("data");

} // namespace

PendingBootstrapStore::PendingBootstrapStore(DBRef db)
    : m_db(std::move(db))
{
    auto wr = m_db->start_write();

    if (m_table = wr->find_table(c_pending_bootstrap_table); m_table) {
        wr->remove_table(m_table);
    }

    if (m_changeset_table = wr->find_table(c_pending_changesets_table); m_changeset_table) {
        wr->remove_table(m_changeset_table);
    }

    auto table = wr->add_table(c_pending_bootstrap_table);
    m_table = table->get_key();

    auto changeset_table = wr->add_embedded_table(c_pending_changesets_table);
    m_changeset_table = changeset_table->get_key();

    m_changesets = table->add_column_list(*changeset_table, c_pending_bootstrap_changesets);
    m_total_size = table->add_column(type_Int, c_pending_bootstrap_total_size);

    m_changeset_server_version = changeset_table->add_column(type_Int, c_pending_changesets_server_version);
    m_changeset_client_version = changeset_table->add_column(type_Int, c_pending_changesets_client_version);
    m_changeset_origin_timestamp = changeset_table->add_column(type_Int, c_pending_changesets_origin_timestamp);
    m_changeset_origin_file_ident = changeset_table->add_column(type_Int, c_pending_changesets_origin_fileident);
    m_changeset_data = changeset_table->add_column(type_Binary, c_pending_changesets_data);

    wr->commit();
}

version_type PendingBootstrapStore::add_changeset_batch(AddMode mode,
                                                        const std::vector<Transformer::RemoteChangeset>& changesets)
{
    auto tr = m_db->start_write();
    auto table = tr->get_table(m_table);
    if (mode == ClearFirst) {
        table->clear();
    }

    auto msg_obj = table->create_object();

    auto changeset_list = msg_obj.get_linklist(m_changesets);
    size_t idx = 0;
    int64_t total_size = 0;
    for (const auto& changeset : changesets) {
        auto cur_changeset_obj = changeset_list.create_and_insert_linked_object(idx++);
        cur_changeset_obj.set(m_changeset_server_version, static_cast<int64_t>(changeset.remote_version));
        cur_changeset_obj.set(m_changeset_client_version,
                              static_cast<int64_t>(changeset.last_integrated_local_version));
        cur_changeset_obj.set(m_changeset_origin_timestamp, static_cast<int64_t>(changeset.origin_timestamp));
        cur_changeset_obj.set(m_changeset_origin_file_ident, static_cast<int64_t>(changeset.origin_file_ident));
        cur_changeset_obj.set(m_changeset_data, changeset.data.get_first_chunk());
        total_size += changeset.data.size();
    }
    msg_obj.set(m_total_size, total_size);
    auto committed_at = tr->commit();
    m_has_pending = true;
    return committed_at;
}

bool PendingBootstrapStore::has_pending()
{
    return m_has_pending;
}

PendingBootstrapStore::PendingBatch PendingBootstrapStore::get_next()
{
    auto tr = m_db->start_write();
    auto table = tr->get_table(m_table);
    if (table->is_empty()) {
        return {};
    }

    PendingBatch batch;
    auto batch_obj = table->get_object(0);
    batch.owned_data.resize(batch_obj.get<int64_t>(m_total_size));

    size_t owned_data_offset = 0;
    auto changeset_list = batch_obj.get_linklist(m_changesets);
    for (size_t idx = 0; idx < changeset_list.size(); ++idx) {
        auto cur_changeset_obj = changeset_list.get_object(idx);
        auto stored_data = cur_changeset_obj.get<BinaryData>(m_changeset_data);

        std::memcpy(&batch.owned_data.at(owned_data_offset), stored_data.data(), stored_data.size());

        Transformer::RemoteChangeset cur_changeset;
        cur_changeset.data = BinaryData(&batch.owned_data.at(owned_data_offset), stored_data.size());
        cur_changeset.remote_version = cur_changeset_obj.get<int64_t>(m_changeset_server_version);
        cur_changeset.last_integrated_local_version = cur_changeset_obj.get<int64_t>(m_changeset_client_version);
        cur_changeset.origin_file_ident = cur_changeset_obj.get<int64_t>(m_changeset_origin_file_ident);
        cur_changeset.origin_timestamp = cur_changeset_obj.get<int64_t>(m_changeset_origin_timestamp);
        cur_changeset.original_changeset_size = stored_data.size();

        owned_data_offset += stored_data.size();
    }

    batch_obj.remove();
    m_has_pending = (table->is_empty() == false);
    tr->commit();

    return batch;
}

} // namespace realm::sync
