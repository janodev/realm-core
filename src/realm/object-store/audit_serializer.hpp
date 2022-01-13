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

#include <realm/keys.hpp>
#include <realm/mixed.hpp>
#include <realm/version_id.hpp>

#include <external/json/json.hpp>

namespace realm {
class AuditObjectSerializer {
public:
    virtual ~AuditObjectSerializer() = default;
    virtual void to_json(nlohmann::json&, const Obj&);

    void link_accessed(VersionID, TableKey, ObjKey, ColKey);
    void set_event_index(size_t index) noexcept
    {
        m_index = index;
    }
    void set_version(VersionID version) noexcept
    {
        m_version = version;
    }
    void sort_link_accesses() noexcept;

    virtual void scope_complete() {}

private:
    struct LinkAccess {
        uint_fast64_t version;
        TableKey table;
        ObjKey obj;
        ColKey col;
        size_t event_ndx;
    };
    std::vector<LinkAccess> m_accessed_links;
    VersionID m_version;
    size_t m_index = 0;

    bool get_field(nlohmann::json& field, const Obj& obj, ColKey col);
    bool get_field(nlohmann::json& field, const Obj& obj, ColKey col, Mixed const& value);
    bool accessed_link(uint_fast64_t, const Obj&, ColKey) const noexcept;
};

} // namespace realm
