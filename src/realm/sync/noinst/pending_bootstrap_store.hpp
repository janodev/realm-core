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
#include "realm/list.hpp"
#include "realm/obj.hpp"
#include "realm/sync/transform.hpp"

namespace realm::sync {

// The PendingBootstrapStore is used internally by the sync client to store changesets from FLX sync bootstraps
// that are sent across multiple download messages.
class PendingBootstrapStore {
public:
    // Constructs from a DBRef. Constructing is destructive - since pending bootstraps are only valid for the
    // session they occured in, this will drop/clear all data when the bootstrap store is constructed.
    //
    // Underneath this creates a table which stores each download message's changesets.
    explicit PendingBootstrapStore(DBRef db);

    PendingBootstrapStore(const PendingBootstrapStore&) = delete;
    PendingBootstrapStore& operator=(const PendingBootstrapStore&) = delete;

    // True if there are pending changesets to process.
    bool has_pending();

    struct PendingBatch {
        std::vector<Transformer::RemoteChangeset> changesets;
        std::vector<OwnedBinaryData> changeset_data;
    };

    // Returns the next batch (download message) of changesets if it exists. This will also remove the batch from
    // storage.
    PendingBatch get_next();

    enum AddMode {
        // Used for download messages in the middle of a bootstrap.
        Append,
        // Used for a download message where lastInBatch=false and the query version is different than the last
        // seen query version (i.e. the start of a new bootstrap).
        ClearFirst,
    };

    // Adds a set of changesets to the store and returns the local version produced by the transaction.
    version_type add_changeset_batch(AddMode mode, const std::vector<Transformer::RemoteChangeset>& changesets);

private:
    DBRef m_db;

    TableKey m_table;
    ColKey m_changesets;

    TableKey m_changeset_table;
    ColKey m_changeset_server_version;
    ColKey m_changeset_client_version;
    ColKey m_changeset_origin_timestamp;
    ColKey m_changeset_origin_file_ident;
    ColKey m_changeset_data;

    bool m_has_pending = false;
};

} // namespace realm::sync
