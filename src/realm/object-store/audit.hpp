////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_AUDIT_HPP
#define REALM_AUDIT_HPP

#include <realm/util/functional.hpp>
#include <realm/util/optional.hpp>

#include <memory>
#include <string>
#include <vector>

namespace realm {
class DB;
class AuditObjectSerializer;
class Obj;
class SyncUser;
class TableView;
class Timestamp;
struct ColKey;
struct RealmConfig;
struct VersionID;
namespace util {
class Logger;
}

struct AuditConfig {
    std::shared_ptr<SyncUser> audit_user;
    std::string partition_value_prefix = "audit";
    std::vector<std::pair<std::string, std::string>> metadata;
    std::shared_ptr<AuditObjectSerializer> serializer;
    std::shared_ptr<util::Logger> logger;
};

class AuditInterface {
public:
    virtual ~AuditInterface() = default;

    virtual void record_query(VersionID, const TableView&) = 0;
    virtual void record_read(VersionID, const Obj& obj, const Obj& parent, ColKey col) = 0;
    virtual void record_write(VersionID old_version, VersionID new_version) = 0;

    virtual void update_metadata(std::vector<std::pair<std::string, std::string>> new_metadata) = 0;

    virtual void begin_scope(std::string_view name) = 0;
    virtual void end_scope(util::UniqueFunction<void(std::exception_ptr)>&& completion = nullptr) = 0;
    // Record a custom audit event. Does not use the scope (and does not need to be inside a scope).
    virtual void record_event(std::string_view activity, util::Optional<std::string> event_type,
                              util::Optional<std::string> data,
                              util::UniqueFunction<void(std::exception_ptr)>&& completion) = 0;

    // Wait for all scopes to be written to disk. Does not wait for them to be
    // uploaded to the server.
    virtual void wait_for_completion() = 0;
    // Wait for there to be no more data to upload. This is not a precise check;
    // if more scopes are created while this is waiting they may or may not be
    // included in the wait.
    virtual void wait_for_uploads() = 0;
};

std::shared_ptr<AuditInterface> make_audit_context(std::shared_ptr<DB>, RealmConfig const& parent_config);

// Hooks for testing. Do not use outside of tests.
namespace audit_test_hooks {
void set_maximum_shard_size(int64_t max_size);
// Not thread-safe, so this must be called at a point when no audit contexts exist.
void set_clock(util::UniqueFunction<Timestamp()>&&);
} // namespace audit_test_hooks
} // namespace realm

#endif // REALM_AUDIT_HPP
