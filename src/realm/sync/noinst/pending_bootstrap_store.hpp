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

#pragma once

#include "realm/db.hpp"
#include "realm/sync/transform.hpp"

namespace realm::sync {

class PendingBootstrapStore {
public:
    explicit PendingBootstrapStore(DBRef db);

    PendingBootstrapStore(const PendingBootstrapStore&) = delete;
    PendingBootstrapStore& operator=(const PendingBootstrapStore&) = delete;

    bool has_pending();

    struct PendingBatch {
        std::vector<Transformer::RemoteChangeset> changesets;
        std::vector<char> owned_data;
    };
    PendingBatch get_next();

    enum AddMode {
        Append,
        ClearFirst,
    };

    version_type add_changeset_batch(AddMode mode, const std::vector<Transformer::RemoteChangeset>& changesets);

private:
    DBRef m_db;

    TableKey m_table;
    ColKey m_changesets;
    ColKey m_total_size;

    TableKey m_changeset_table;
    ColKey m_changeset_server_version;
    ColKey m_changeset_client_version;
    ColKey m_changeset_origin_timestamp;
    ColKey m_changeset_origin_file_ident;
    ColKey m_changeset_data;

    bool m_has_pending = false;
    int64_t m_msg_num = 0;
};

} // namespace realm::sync
