///////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#include <realm/db.hpp>
#include <realm/dictionary.hpp>
#include <realm/set.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/client_reset.hpp>

#include <realm/util/flat_map.hpp>

#include <algorithm>
#include <vector>

using namespace realm;
using namespace _impl;
using namespace sync;

namespace {

struct EmbeddedObjectConverter {
    // If an embedded object is encountered, add it to a list of embedded objects to process.
    // This relies on the property that embedded objects only have one incoming link
    // otherwise there could be an infinite loop while discovering embedded objects.
    void track(Obj e_src, Obj e_dst)
    {
        embedded_pending.push_back({e_src, e_dst});
    }
    void process_pending();

private:
    struct EmbeddedToCheck {
        Obj embedded_in_src;
        Obj embedded_in_dst;
    };

    std::vector<EmbeddedToCheck> embedded_pending;
};

struct InterRealmValueConverter {
    InterRealmValueConverter(ConstTableRef src_table, ColKey src_col, ConstTableRef dst_table, ColKey dst_col,
                             EmbeddedObjectConverter& ec)
        : m_src_table(src_table)
        , m_dst_table(dst_table)
        , m_src_col(src_col)
        , m_dst_col(dst_col)
        , m_embedded_converter(ec)
        , m_is_embedded_link(false)
        , m_primitive_types_only(!(src_col.get_type() == col_type_TypedLink || src_col.get_type() == col_type_Link ||
                                   src_col.get_type() == col_type_LinkList || src_col.get_type() == col_type_Mixed))
    {
        if (!m_primitive_types_only) {
            REALM_ASSERT(src_table);
            m_opposite_of_src = src_table->get_opposite_table(src_col);
            m_opposite_of_dst = dst_table->get_opposite_table(dst_col);
            REALM_ASSERT(bool(m_opposite_of_src) == bool(m_opposite_of_dst));
            if (m_opposite_of_src) {
                m_is_embedded_link = m_opposite_of_src->is_embedded();
            }
        }
    }

    void track_new_embedded(Obj src, Obj dst)
    {
        m_embedded_converter.track(src, dst);
    }

    struct ConversionResult {
        Mixed converted_value;
        bool requires_new_embedded_object = false;
        Obj src_embedded_to_check;
    };

    // convert `src` to the destination Realm and compare that value with `dst`
    // If `converted_src_out` is provided, it will be set to the converted src value
    int cmp_src_to_dst(Mixed src, Mixed dst, ConversionResult* converted_src_out = nullptr,
                       bool* did_update_out = nullptr)
    {
        int cmp = 0;
        Mixed converted_src;
        if (m_primitive_types_only || !src.is_type(type_Link, type_TypedLink)) {
            converted_src = src;
            cmp = src.compare(dst);
        }
        else {
            if (m_opposite_of_src) {
                ObjKey src_link_key = src.get<ObjKey>();
                if (m_is_embedded_link) {
                    Obj src_embedded = m_opposite_of_src->get_object(src_link_key);
                    REALM_ASSERT_DEBUG(src_embedded.is_valid());
                    if (dst.is_type(type_Link, type_TypedLink)) {
                        cmp = 0; // no need to set this link, there is already an embedded object here
                        Obj dst_embedded = m_opposite_of_dst->get_object(dst.get<ObjKey>());
                        REALM_ASSERT_DEBUG(dst_embedded.is_valid());
                        converted_src = dst_embedded.get_key();
                        track_new_embedded(src_embedded, dst_embedded);
                    }
                    else {
                        cmp = src.compare(dst);
                        if (converted_src_out) {
                            converted_src_out->requires_new_embedded_object = true;
                            converted_src_out->src_embedded_to_check = src_embedded;
                        }
                    }
                }
                else {
                    Mixed src_link_pk = m_opposite_of_src->get_primary_key(src_link_key);
                    Obj dst_link = m_opposite_of_dst->create_object_with_primary_key(src_link_pk, did_update_out);
                    converted_src = dst_link.get_key();
                    if (dst.is_type(type_TypedLink)) {
                        cmp = converted_src.compare(dst.get<ObjKey>());
                    }
                    else {
                        cmp = converted_src.compare(dst);
                    }
                }
            }
            else {
                ObjLink src_link = src.get<ObjLink>();
                if (src_link.is_unresolved()) {
                    converted_src = Mixed{}; // no need to transfer over unresolved links
                    cmp = converted_src.compare(dst);
                }
                else {
                    TableRef src_link_table = m_src_table->get_parent_group()->get_table(src_link.get_table_key());
                    REALM_ASSERT_EX(src_link_table, src_link.get_table_key());
                    TableRef dst_link_table = m_dst_table->get_parent_group()->get_table(src_link_table->get_name());
                    REALM_ASSERT_EX(dst_link_table, src_link_table->get_name());
                    // embedded tables should always be covered by the m_opposite_of_src case above.
                    REALM_ASSERT_EX(!src_link_table->is_embedded(), src_link_table->get_name());
                    // regular table, convert by pk
                    Mixed src_pk = src_link_table->get_primary_key(src_link.get_obj_key());
                    Obj dst_link = dst_link_table->create_object_with_primary_key(src_pk, did_update_out);
                    converted_src = ObjLink{dst_link_table->get_key(), dst_link.get_key()};
                    cmp = converted_src.compare(dst);
                }
            }
        }
        if (converted_src_out) {
            converted_src_out->converted_value = converted_src;
        }
        if (did_update_out && cmp) {
            *did_update_out = true;
        }
        return cmp;
    }

    inline ColKey source_col() const
    {
        return m_src_col;
    }

    inline ColKey dest_col() const
    {
        return m_dst_col;
    }

private:
    TableRef m_dst_link_table;
    ConstTableRef m_src_table;
    ConstTableRef m_dst_table;
    ColKey m_src_col;
    ColKey m_dst_col;
    TableRef m_opposite_of_src;
    TableRef m_opposite_of_dst;
    EmbeddedObjectConverter& m_embedded_converter;
    bool m_is_embedded_link;
    const bool m_primitive_types_only;
};

// Takes two lists, src and dst, and makes dst equal src. src is unchanged.
void copy_list(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    // The two arrays are compared by finding the longest common prefix and
    // suffix.  The middle section differs between them and is made equal by
    // updating the middle section of dst.
    //
    // Example:
    // src = abcdefghi
    // dst = abcxyhi
    // The common prefix is abc. The common suffix is hi. xy is replaced by defg.
    LstBasePtr src = src_obj.get_listbase_ptr(convert.source_col());
    LstBasePtr dst = dst_obj.get_listbase_ptr(convert.dest_col());

    bool updated = false;
    size_t len_src = src->size();
    size_t len_dst = dst->size();
    size_t len_min = std::min(len_src, len_dst);

    size_t ndx = 0;
    size_t suffix_len = 0;

    while (ndx < len_min && convert.cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), nullptr, update_out) == 0) {
        ndx++;
    }

    size_t suffix_len_max = len_min - ndx;

    while (suffix_len < suffix_len_max &&
           convert.cmp_src_to_dst(src->get_any(len_src - 1 - suffix_len), dst->get_any(len_dst - 1 - suffix_len),
                                  nullptr, update_out) == 0) {
        suffix_len++;
    }

    len_min -= (ndx + suffix_len);

    for (size_t i = 0; i < len_min; i++) {
        InterRealmValueConverter::ConversionResult converted_src;
        if (convert.cmp_src_to_dst(src->get_any(ndx), dst->get_any(ndx), &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                auto lnklist = dynamic_cast<LnkLst*>(dst.get());
                REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
                Obj embedded = lnklist->create_and_set_linked_object(ndx);
                convert.track_new_embedded(converted_src.src_embedded_to_check, embedded);
            }
            else {
                dst->set_any(ndx, converted_src.converted_value);
            }
            updated = true;
        }
        ndx++;
    }

    // New elements must be inserted in dst.
    while (len_dst < len_src) {
        InterRealmValueConverter::ConversionResult converted_src;
        convert.cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        if (converted_src.requires_new_embedded_object) {
            auto lnklist = dynamic_cast<LnkLst*>(dst.get());
            REALM_ASSERT(lnklist); // this is the only type of list that supports embedded objects
            Obj embedded = lnklist->create_and_insert_linked_object(ndx);
            convert.track_new_embedded(converted_src.src_embedded_to_check, embedded);
        }
        else {
            dst->insert_any(ndx, converted_src.converted_value);
        }
        len_dst++;
        ndx++;
        updated = true;
    }
    // Excess elements must be removed from ll_dst.
    if (len_dst > len_src) {
        dst->remove(len_src - suffix_len, len_dst - suffix_len);
        updated = true;
    }

    REALM_ASSERT(dst->size() == len_src);
    if (updated && update_out) {
        *update_out = updated;
    }
}

void copy_set(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    SetBasePtr src = src_obj.get_setbase_ptr(convert.source_col());
    SetBasePtr dst = dst_obj.get_setbase_ptr(convert.dest_col());

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    // the implementation could be storing elements in sorted order, but
    // we don't assume that here.
    src->sort(sorted_src, ascending);
    dst->sort(sorted_dst, ascending);

    size_t dst_ndx = 0;
    size_t src_ndx = 0;
    while (src_ndx < sorted_src.size()) {
        if (dst_ndx == sorted_dst.size()) {
            // if we have reached the end of the dst items, all remaining
            // src items should be added
            while (src_ndx < sorted_src.size()) {
                to_insert.push_back(sorted_src[src_ndx++]);
            }
            break;
        }
        size_t ndx_in_src = sorted_src[src_ndx];
        Mixed src_val = src->get_any(ndx_in_src);
        while (dst_ndx < sorted_dst.size()) {
            size_t ndx_in_dst = sorted_dst[dst_ndx];

            int cmp = convert.cmp_src_to_dst(src_val, dst->get_any(ndx_in_dst), nullptr, update_out);
            if (cmp == 0) {
                // equal: advance both src and dst
                ++dst_ndx;
                ++src_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(ndx_in_src);
                ++src_ndx;
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(ndx_in_dst);
                ++dst_ndx;
                continue;
            }
        }
    }
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst->erase_any(dst->get_any(*it));
    }
    for (auto ndx : to_insert) {
        InterRealmValueConverter::ConversionResult converted_src;
        convert.cmp_src_to_dst(src->get_any(ndx), Mixed{}, &converted_src, update_out);
        // we do not support a set of embedded objects
        REALM_ASSERT(!converted_src.requires_new_embedded_object);
        dst->insert_any(converted_src.converted_value);
    }

    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void copy_dictionary(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    Dictionary src = src_obj.get_dictionary(convert.source_col());
    Dictionary dst = dst_obj.get_dictionary(convert.dest_col());

    std::vector<size_t> sorted_src, sorted_dst, to_insert, to_delete;
    constexpr bool ascending = true;
    src.sort_keys(sorted_src, ascending);
    dst.sort_keys(sorted_dst, ascending);

    size_t dst_ndx = 0;
    size_t src_ndx = 0;
    while (src_ndx < sorted_src.size()) {
        if (dst_ndx == sorted_dst.size()) {
            // if we have reached the end of the dst items, all remaining
            // src items should be added
            while (src_ndx < sorted_src.size()) {
                to_insert.push_back(sorted_src[src_ndx++]);
            }
            break;
        }

        auto src_val = src.get_pair(sorted_src[src_ndx]);
        while (dst_ndx < sorted_dst.size()) {
            auto dst_val = dst.get_pair(sorted_dst[dst_ndx]);
            int cmp = src_val.first.compare(dst_val.first);
            if (cmp == 0) {
                // Check if the values differ
                if (convert.cmp_src_to_dst(src_val.second, dst_val.second, nullptr, update_out)) {
                    // values are different - modify destination, advance both
                    to_insert.push_back(sorted_src[src_ndx]);
                }
                // keys and values equal: advance both src and dst
                ++dst_ndx;
                ++src_ndx;
                break;
            }
            else if (cmp < 0) {
                // src < dst: insert src, advance src only
                to_insert.push_back(sorted_src[src_ndx++]);
                break;
            }
            else {
                // src > dst: delete dst, advance only dst
                to_delete.push_back(sorted_dst[dst_ndx++]);
                continue;
            }
        }
    }
    // at this point, we've gone through all src items but still have dst items
    // oustanding; these should all be deleted because they are not in src
    while (dst_ndx < sorted_dst.size()) {
        to_delete.push_back(sorted_dst[dst_ndx++]);
    }

    std::sort(to_delete.begin(), to_delete.end());
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
        dst.erase(dst.begin() + *it);
    }
    for (auto ndx : to_insert) {
        auto pair = src.get_pair(ndx);
        InterRealmValueConverter::ConversionResult converted_val;
        convert.cmp_src_to_dst(pair.second, Mixed{}, &converted_val, update_out);
        if (converted_val.requires_new_embedded_object) {
            Obj new_embedded = dst.create_and_insert_linked_object(pair.first);
            convert.track_new_embedded(converted_val.src_embedded_to_check, new_embedded);
        }
        else {
            dst.insert(pair.first, converted_val.converted_value);
        }
    }
    if (update_out && (to_delete.size() || to_insert.size())) {
        *update_out = true;
    }
}

void copy_value(const Obj& src_obj, Obj& dst_obj, InterRealmValueConverter& convert, bool* update_out)
{
    if (convert.source_col().is_list()) {
        copy_list(src_obj, dst_obj, convert, update_out);
    }
    else if (convert.source_col().is_dictionary()) {
        copy_dictionary(src_obj, dst_obj, convert, update_out);
    }
    else if (convert.source_col().is_set()) {
        copy_set(src_obj, dst_obj, convert, update_out);
    }
    else {
        REALM_ASSERT(!convert.source_col().is_collection());
        InterRealmValueConverter::ConversionResult converted_src;
        if (convert.cmp_src_to_dst(src_obj.get_any(convert.source_col()), dst_obj.get_any(convert.dest_col()),
                                   &converted_src, update_out)) {
            if (converted_src.requires_new_embedded_object) {
                Obj new_embedded = dst_obj.create_and_set_linked_object(convert.dest_col());
                convert.track_new_embedded(converted_src.src_embedded_to_check, new_embedded);
            }
            else {
                dst_obj.set_any(convert.dest_col(), converted_src.converted_value);
            }
        }
    }
}

struct InterRealmObjectConverter {
    InterRealmObjectConverter(ConstTableRef table_src, TableRef table_dst, EmbeddedObjectConverter& embedded_tracker)
        : m_embedded_tracker(embedded_tracker)
    {
        populate_columns_from_table(table_src, table_dst);
    }

    void copy(const Obj& src, Obj& dst, bool* update_out)
    {
        for (auto& column : m_columns_cache) {
            copy_value(src, dst, column, update_out);
        }
    }

private:
    void populate_columns_from_table(ConstTableRef table_src, ConstTableRef table_dst)
    {
        m_columns_cache.clear();
        m_columns_cache.reserve(table_src->get_column_count());
        ColKey pk_col = table_src->get_primary_key_column();
        for (ColKey col_key_src : table_src->get_column_keys()) {
            if (col_key_src == pk_col)
                continue;
            StringData col_name = table_src->get_column_name(col_key_src);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            REALM_ASSERT(col_key_dst);
            m_columns_cache.emplace_back(
                InterRealmValueConverter(table_src, col_key_src, table_dst, col_key_dst, m_embedded_tracker));
        }
    }

    EmbeddedObjectConverter& m_embedded_tracker;
    std::vector<InterRealmValueConverter> m_columns_cache;
};

void EmbeddedObjectConverter::process_pending()
{
    // Conceptually this is a map, but doing a linear search through a vector is known
    // to be faster for small number of elements. Since the number of tables expected
    // to be processed here is assumed to be small < 20, use linear search instead of
    // hashing. N is the depth to which embedded objects are connected and the upper
    // bound is the total number of tables which is finite, and is usually small.

    std::vector<std::pair<TableKey, InterRealmObjectConverter>> converters; // FIXME: FlatMap
    auto get_converter = [this, &converters](ConstTableRef src_table,
                                             TableRef dst_table) -> InterRealmObjectConverter& {
        TableKey dst_table_key = dst_table->get_key();
        auto it = std::find_if(converters.begin(), converters.end(), [&dst_table_key](auto& val) {
            return val.first == dst_table_key;
        });
        if (it == converters.end()) {
            return converters.emplace_back(dst_table_key, InterRealmObjectConverter{src_table, dst_table, *this})
                .second;
        }
        return it->second;
    };

    while (!embedded_pending.empty()) {
        EmbeddedToCheck pending = embedded_pending.back();
        embedded_pending.pop_back();
        InterRealmObjectConverter& converter =
            get_converter(pending.embedded_in_src.get_table(), pending.embedded_in_dst.get_table());
        converter.copy(pending.embedded_in_src, pending.embedded_in_dst, nullptr);
    }
}

struct ListTracker {

    struct CrossListIndex {
        uint32_t local;
        uint32_t remote;
        size_t stable_id;
    };

    util::Optional<CrossListIndex> insert(uint32_t local_index, size_t remote_list_size)
    {
        if (m_requires_manual_copy) {
            return util::none;
        }
        uint32_t remote_index = local_index;
        if (remote_index > remote_list_size) {
            remote_index = static_cast<uint32_t>(remote_list_size);
        }
        for (auto& ndx : m_indices_allowed) {
            if (ndx.local >= local_index) {
                ++ndx.local;
                ++ndx.remote;
            }
        }
        CrossListIndex inserted{local_index, remote_index, ++m_stable_id_counter};
        m_indices_allowed.push_back(inserted);
        return inserted;
    }

    util::Optional<CrossListIndex> find_stable_id(size_t id)
    {
        if (m_requires_manual_copy) {
            return util::none;
        }
        for (auto& ndx : m_indices_allowed) {
            if (ndx.stable_id == id) {
                return ndx;
            }
        }
        return util::none;
    }

    util::Optional<CrossListIndex> update(uint32_t index)
    {
        if (m_requires_manual_copy) {
            return util::none;
        }
        for (auto& ndx : m_indices_allowed) {
            if (ndx.local == index) {
                return ndx;
            }
        }
        queue_for_manual_copy();
        return util::none;
    }

    void clear()
    {
        // FIXME: check for optimizations here
        // any local operations to a list after a clear are
        // strictly on locally added elements so no need to continue tracking
        m_requires_manual_copy = false;
        m_did_clear = true;
        m_indices_allowed.clear();
    }

    bool move(uint32_t from, uint32_t to, size_t lst_size, uint32_t& remote_from_out, uint32_t& remote_to_out)
    {
        if (m_requires_manual_copy) {
            return false;
        }
        remote_from_out = from;
        remote_to_out = to;

        // Only allow move operations that operate on known indices.
        // This requires that both local elements 'from' and 'to' are known.
        auto target_from = m_indices_allowed.end();
        auto target_to = m_indices_allowed.end();
        for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
            if (it->local == from) {
                REALM_ASSERT(target_from == m_indices_allowed.end());
                target_from = it;
            }
            else if (it->local == to) {
                REALM_ASSERT(target_to == m_indices_allowed.end());
                target_to = it;
            }
        }
        if (target_from == m_indices_allowed.end() || target_to == m_indices_allowed.end()) {
            queue_for_manual_copy();
            return false;
        }
        REALM_ASSERT_EX(target_from->remote <= lst_size, from, to, target_from->remote, target_to->remote, lst_size);
        REALM_ASSERT_EX(target_to->remote <= lst_size, from, to, target_from->remote, target_to->remote, lst_size);

        if (from == to) {
            // we shouldn't be generating an instruction for this case, but it is a no-op
            return true; // LCOV_EXCL_LINE
        }
        else if (from < to) {
            for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
                if (it->local > from && it->local <= to) {
                    REALM_ASSERT(it->local != 0);
                    REALM_ASSERT(it->remote != 0);
                    --it->local;
                    --it->remote;
                }
            }
            remote_from_out = target_from->remote;
            remote_to_out = target_to->remote + 1;
            target_from->local = target_to->local + 1;
            target_from->remote = target_to->remote + 1;
            return true;
        }
        else { // from > to
            for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end(); ++it) {
                if (it->local < from && it->local >= to) {
                    REALM_ASSERT_EX(it->remote + 1 < lst_size, it->remote, lst_size);
                    ++it->local;
                    ++it->remote;
                }
            }
            remote_from_out = target_from->remote;
            remote_to_out = target_to->remote - 1;
            target_from->local = target_to->local - 1;
            target_from->remote = target_to->remote - 1;
            return true;
        }
        REALM_UNREACHABLE();
        return false;
    }

    bool remove(uint32_t index, uint32_t& remote_index_out)
    {
        if (m_requires_manual_copy) {
            return false;
        }
        remote_index_out = index;
        bool found = false;
        for (auto it = m_indices_allowed.begin(); it != m_indices_allowed.end();) {
            if (it->local == index) {
                found = true;
                remote_index_out = it->remote;
                it = m_indices_allowed.erase(it);
                continue;
            }
            else if (it->local > index) {
                --it->local;
                --it->remote;
            }
            ++it;
        }
        if (!found) {
            queue_for_manual_copy();
            return false;
        }
        return true;
    }

    bool requires_manual_copy() const
    {
        return m_requires_manual_copy;
    }

    void queue_for_manual_copy()
    {
        m_requires_manual_copy = true;
        m_indices_allowed.clear();
    }

private:
    std::vector<CrossListIndex> m_indices_allowed;
    bool m_requires_manual_copy = false;
    bool m_did_clear = false;
    size_t m_stable_id_counter = 0;
};

std::unique_ptr<LstBase> get_list_from_path(Obj& obj, ColKey col)
{
    // For link columns, `Obj::get_listbase_ptr()` always returns an instance whose concrete type is
    // `LnkLst`, which uses condensed indexes. However, we are interested in using non-condensed
    // indexes, so we need to manually construct a `Lst<ObjKey>` instead for lists of non-embedded
    // links.
    REALM_ASSERT(col.is_list());
    std::unique_ptr<LstBase> list;
    if (col.get_type() == col_type_Link || col.get_type() == col_type_LinkList) {
        auto table = obj.get_table();
        if (!table->get_link_target(col)->is_embedded()) {
            list = obj.get_list_ptr<ObjKey>(col);
        }
        else {
            list = obj.get_listbase_ptr(col);
        }
    }
    else {
        list = obj.get_listbase_ptr(col);
    }
    return list;
}

struct InternDictKey {
    size_t pos = realm::npos;
    size_t size = realm::npos;
    bool is_null() const
    {
        return pos == realm::npos && size == realm::npos;
    }
    constexpr bool operator==(const InternDictKey& other) const noexcept
    {
        return pos == other.pos && size == other.size;
    }
    constexpr bool operator!=(const InternDictKey& other) const noexcept
    {
        return !operator==(other);
    }
    constexpr bool operator<(const InternDictKey& other) const noexcept
    {
        if (pos < other.pos) {
            return true;
        }
        else if (pos == other.pos) {
            return size < other.size;
        }
        return false;
    }
};

struct InterningBuffer {
    StringData get_key(const InternDictKey& key) const
    {
        if (key.is_null()) {
            return {};
        }
        if (key.size == 0) {
            return "";
        }
        REALM_ASSERT(key.pos < m_dict_keys_buffer.size());
        REALM_ASSERT(key.pos + key.size <= m_dict_keys_buffer.size());
        return StringData{m_dict_keys_buffer.data() + key.pos, key.size};
    }

    InternDictKey get_or_add(const StringData& str)
    {
        for (auto& key : m_dict_keys) {
            StringData existing = get_key(key);
            if (existing == str) {
                return key;
            }
        }
        InternDictKey new_key{};
        if (str.is_null()) {
            m_dict_keys.push_back(new_key);
        }
        else {
            size_t next_pos = m_dict_keys_buffer.size();
            new_key.pos = next_pos;
            new_key.size = str.size();
            m_dict_keys_buffer.append(str);
            m_dict_keys.push_back(new_key);
        }
        return new_key;
    }
    InternDictKey get_interned_key(const StringData& str) const
    {
        if (str.is_null()) {
            return {};
        }
        for (auto& key : m_dict_keys) {
            StringData existing = get_key(key);
            if (existing == str) {
                return key;
            }
        }
        REALM_UNREACHABLE();
        return {};
    }
    std::string print() const
    {
        return util::format("InterningBuffer of size=%1:'%2'", m_dict_keys.size(), m_dict_keys_buffer);
    }

private:
    std::string m_dict_keys_buffer;
    std::vector<InternDictKey> m_dict_keys;
};

// A wrapper around a PathInstruction which enables storing this path in a
// FlatMap or other container. The advantage of using this instead of a PathInstruction
// is the use of ColKey instead of column names and that because it is not possible to use
// the InternStrings of a PathInstruction because they are tied to a specific Changeset, and
// while the ListPath can be used across multiple Changesets.
struct ListPath {
    ListPath(TableKey table_key, ObjKey obj_key)
        : m_table_key(table_key)
        , m_obj_key(obj_key)
    {
    }

    struct Element {
        Element(size_t stable_ndx)
            : index(stable_ndx)
            , type(Type::ListIndex)
        {
        }
        Element(const InternDictKey& str)
            : intern_key(str)
            , type(Type::InternKey)
        {
        }
        Element(ColKey key)
            : col_key(key)
            , type(Type::ColumnKey)
        {
        }
        union {
            InternDictKey intern_key;
            size_t index;
            ColKey col_key;
        };
        enum class Type {
            InternKey,
            ListIndex,
            ColumnKey,
        } type;
        bool operator==(const Element& other) const
        {
            if (type == other.type) {
                switch (type) {
                    case Type::InternKey:
                        return intern_key == other.intern_key;
                    case Type::ListIndex:
                        return index == other.index;
                    case Type::ColumnKey:
                        return col_key == other.col_key;
                }
            }
            return false;
        }
        bool operator!=(const Element& other) const
        {
            return !(operator==(other));
        }
        bool operator<(const Element& other) const
        {
            if (type < other.type) {
                return true;
            }
            if (type == other.type) {
                switch (type) {
                    case Type::InternKey:
                        return intern_key < other.intern_key;
                    case Type::ListIndex:
                        return index < other.index;
                    case Type::ColumnKey:
                        return col_key < other.col_key;
                }
            }
            return false;
        }
    };
    std::vector<Element> m_path;
    TableKey m_table_key;
    ObjKey m_obj_key;

    void append(const Element& item)
    {
        m_path.push_back(item);
    }

    bool operator<(const ListPath& other) const
    {
        if (m_table_key < other.m_table_key || m_obj_key < other.m_obj_key || m_path.size() < other.m_path.size()) {
            return true;
        }
        return std::lexicographical_compare(m_path.begin(), m_path.end(), other.m_path.begin(), other.m_path.end());
    }

    bool operator==(const ListPath& other) const
    {
        if (m_table_key == other.m_table_key && m_obj_key == other.m_obj_key &&
            m_path.size() == other.m_path.size()) {
            for (size_t i = 0; i < m_path.size(); ++i) {
                if (m_path[i] != other.m_path[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    bool operator!=(const ListPath& other) const
    {
        return !(operator==(other));
    }

    std::string path_to_string(Transaction& remote, const InterningBuffer& buffer)
    {
        TableRef remote_table = remote.get_table(m_table_key);
        Obj base_obj = remote_table->get_object(m_obj_key);
        std::string path = util::format("%1.pk=%2", remote_table->get_name(), base_obj.get_primary_key());
        for (auto& e : m_path) {
            switch (e.type) {
                case Element::Type::ColumnKey:
                    path += util::format(".%1", remote_table->get_column_name(e.col_key));
                    remote_table = remote_table->get_link_target(e.col_key);
                    break;
                case Element::Type::ListIndex:
                    path += util::format("[%1]", e.index);
                    break;
                case Element::Type::InternKey:
                    path += util::format("[key='%1']", buffer.get_key(e.intern_key));
                    break;
            }
        }
        return path;
    }
};

struct RecoverLocalChangesetsHandler : public InstructionApplier {
    using Instruction = sync::Instruction;
    using ListPathCallback = util::UniqueFunction<bool(LstBase&, uint32_t, const ListPath&)>;
    Transaction& remote;
    Transaction& local;
    util::Logger& logger;
    InterningBuffer m_intern_keys;

    // Track any recovered operations on lists to make sure that they are allowed.
    // If not, the lists here will be copied verbatim from the local state to the remote.
    util::FlatMap<ListPath, ListTracker> m_lists;

    // The recovery fails if there seems to be conflict between the
    // instructions and state.
    //
    // Failure is triggered by:
    // 1. Destructive schema changes.
    // 2. Creation of an already existing table with another type.
    // 3. Creation of an already existing column with another type.

    RecoverLocalChangesetsHandler(Transaction& remote_wt, Transaction& local_wt, util::Logger& logger)
        : InstructionApplier(remote_wt)
        , remote{remote_wt}
        , local{local_wt}
        , logger{logger}
    {
    }

    REALM_NORETURN void handle_error(const std::string& message) const
    {
        std::string full_message =
            util::format("Unable to automatically recover local changes during client reset: '%1'", message);
        logger.error(full_message.c_str());
        throw realm::_impl::client_reset::ClientResetFailed(full_message);
    }

    void process_changesets(const std::vector<ChunkedBinaryData>& changesets)
    {
        for (const ChunkedBinaryData& chunked_changeset : changesets) {
            if (chunked_changeset.size() == 0)
                continue;

            ChunkedBinaryInputStream in{chunked_changeset};
            sync::Changeset parsed_changeset;
            sync::parse_changeset(in, parsed_changeset); // Throws
            // parsed_changeset.print(); // view the changes to be recovered in stdout for debugging

            InstructionApplier::begin_apply(parsed_changeset, &logger);
            for (auto instr : parsed_changeset) {
                if (!instr)
                    continue;
                instr->visit(*this); // Throws
            }
            InstructionApplier::end_apply();
        }

        copy_lists_with_unrecoverable_changes();
    }

    void copy_lists_with_unrecoverable_changes()
    {
        // Any modifications, moves or deletes to list elements which were not also created in the recovery
        // cannot be reliably applied because there is no way to know if the indices on the server have
        // shifted without a reliable server side history. For these lists, create a consistant state by
        // copying over the entire list from the recovering client's state. This does create a "last recovery wins"
        // scenario for modifications to lists, but this is only a best effort.
        // For example, consider a list [A,B].
        // Now the server has been reset, and applied an ArrayMove from a different client producing [B,A]
        // A client being reset tries to recover the instruction ArrayErase(index=0) intending to erase A.
        // But if this instruction were to be applied to the server's array, element B would be erased which is wrong.
        // So to prevent this, upon discovery of this type of instruction, replace the entire array to the client's
        // final state which would be [B].
        // IDEA: if a unique id were associated with each list element, we could recover lists correctly because
        // we would know where list elements ended up or if they were deleted by the server.
        EmbeddedObjectConverter embedded_object_tracker;
        for (auto& it : m_lists) {
            if (it.second.requires_manual_copy()) {
                std::string path_str = it.first.path_to_string(remote, m_intern_keys);
                bool did_translate = resolve(it.first, [&](LstBase& remote_list, LstBase& local_list) {
                    ConstTableRef local_table = local_list.get_table();
                    ConstTableRef remote_table = remote_list.get_table();
                    ColKey local_col_key = local_list.get_col_key();
                    ColKey remote_col_key = remote_list.get_col_key();
                    Obj local_obj = local_list.get_obj();
                    Obj remote_obj = remote_list.get_obj();
                    InterRealmValueConverter value_converter(local_table, local_col_key, remote_table, remote_col_key,
                                                             embedded_object_tracker);
                    logger.debug("Recovery overwrites list for '%1' size: %2 -> %3", path_str, remote_list.size(),
                                 local_list.size());
                    copy_list(local_obj, remote_obj, value_converter, nullptr);
                    embedded_object_tracker.process_pending();
                });
                if (!did_translate) {
                    // object no longer exists in the local state, ignore and continue
                    logger.warn("Discarding a list recovery made to an object which could not be resolved. "
                                "remote_path='%3'",
                                path_str);
                }
            }
        }
        embedded_object_tracker.process_pending();
    }

    bool resolve_path(ListPath& path, Obj remote_obj, Obj local_obj,
                      util::UniqueFunction<void(LstBase&, LstBase&)> callback)
    {
        for (auto it = path.m_path.begin(); it != path.m_path.end();) {
            if (!remote_obj || !local_obj) {
                return false;
            }
            REALM_ASSERT(it->type == ListPath::Element::Type::ColumnKey);
            ColKey col = it->col_key;
            REALM_ASSERT(col);
            if (col.is_list()) {
                std::unique_ptr<LstBase> remote_list = get_list_from_path(remote_obj, col);
                ColKey local_col =
                    local_obj.get_table()->get_column_key(remote_obj.get_table()->get_column_name(col));
                REALM_ASSERT(local_col);
                std::unique_ptr<LstBase> local_list = get_list_from_path(local_obj, local_col);
                ++it;
                if (it == path.m_path.end()) {
                    callback(*remote_list, *local_list);
                    return true;
                }
                else {
                    REALM_ASSERT(it->type == ListPath::Element::Type::ListIndex);
                    REALM_ASSERT(it != path.m_path.end());
                    size_t stable_index_id = it->index;
                    REALM_ASSERT(stable_index_id != realm::npos);
                    // create a path just to this point
                    ListPath current_path = path;
                    current_path.m_path.erase(current_path.m_path.begin() +
                                                  (current_path.m_path.size() - (path.m_path.end() - it)),
                                              current_path.m_path.end());
                    if (m_lists.count(current_path) == 0) {
                        return false;
                    }
                    auto cross_index = m_lists[current_path].find_stable_id(stable_index_id);
                    if (!cross_index) {
                        return false;
                    }
                    REALM_ASSERT(cross_index->remote < remote_list->size());
                    REALM_ASSERT(cross_index->local < local_list->size());
                    REALM_ASSERT(dynamic_cast<LnkLst*>(remote_list.get()));
                    auto remote_link_list = static_cast<LnkLst*>(remote_list.get());
                    REALM_ASSERT(dynamic_cast<LnkLst*>(local_list.get()));
                    auto local_link_list = static_cast<LnkLst*>(local_list.get());
                    remote_obj = remote_link_list->get_object(cross_index->remote);
                    local_obj = local_link_list->get_object(cross_index->local);
                    ++it;
                }
            }
            else {
                REALM_ASSERT(col.is_dictionary());
                ++it;
                REALM_ASSERT(it != path.m_path.end());
                REALM_ASSERT(it->type == ListPath::Element::Type::InternKey);
                Dictionary remote_dict = remote_obj.get_dictionary(col);
                Dictionary local_dict = local_obj.get_dictionary(remote_obj.get_table()->get_column_name(col));
                StringData dict_key = m_intern_keys.get_key(it->intern_key);
                if (remote_dict.contains(dict_key) && local_dict.contains(dict_key)) {
                    remote_obj = remote_dict.get_object(dict_key);
                    local_obj = local_dict.get_object(dict_key);
                    ++it;
                }
                else {
                    return false;
                }
            }
        }
        return false;
    }

    bool resolve(ListPath& path, util::UniqueFunction<void(LstBase&, LstBase&)> callback)
    {
        auto remote_table = remote.get_table(path.m_table_key);
        if (remote_table) {
            auto local_table = local.get_table(remote_table->get_name());
            if (local_table) {
                auto remote_obj = remote_table->get_object(path.m_obj_key);
                if (remote_obj) {
                    auto local_obj_key = local_table->find_primary_key(remote_obj.get_primary_key());
                    if (local_obj_key) {
                        return resolve_path(path, remote_obj, local_table->get_object(local_obj_key),
                                            std::move(callback));
                    }
                }
            }
        }
        return false;
    }

    bool translate_list_element(LstBase& list, uint32_t index, Instruction::Path::iterator begin,
                                Instruction::Path::const_iterator end, const char* instr_name,
                                ListPathCallback list_callback, bool ignore_missing_dict_keys, ListPath& path)
    {
        if (begin == end) {
            return list_callback(list, index, path);
        }

        auto col = list.get_col_key();
        auto field_name = list.get_table()->get_column_name(col);

        if (col.get_type() == col_type_LinkList) {
            auto target = list.get_table()->get_link_target(col);
            if (!target->is_embedded()) {
                handle_error(util::format("%1: Reference through non-embedded link at '%3.%2[%4]'", instr_name,
                                          field_name, list.get_table()->get_name(), index));
            }

            REALM_ASSERT(dynamic_cast<LnkLst*>(&list));
            auto& link_list = static_cast<LnkLst&>(list);
            auto obj = link_list.get_obj();
            if (m_lists.count(path) != 0) {
                auto& list_tracker = m_lists[path];
                auto cross_ndx = list_tracker.update(index);
                if (!cross_ndx) {
                    return false; // not allowed to modify this list item
                }
                REALM_ASSERT(cross_ndx->remote != uint32_t(-1));
                REALM_ASSERT_EX(cross_ndx->remote < link_list.size(), cross_ndx->remote, link_list.size());
                *(begin - 1) = cross_ndx->remote;
                path.append(cross_ndx->stable_id);
                auto embedded_object = link_list.get_object(cross_ndx->remote);
                if (auto pfield = mpark::get_if<InternString>(&*begin)) {
                    ++begin;
                    return translate_field(embedded_object, *pfield, begin, end, instr_name, std::move(list_callback),
                                           ignore_missing_dict_keys, path);
                }
                else {
                    handle_error(util::format("%1: Embedded object field reference is not a string", instr_name));
                }
            }
            else {
                // no record of this base list so far, track it for verbatim copy
                m_lists[path].queue_for_manual_copy();
                return false;
            }
        }
        else {
            handle_error(util::format(
                "%1: Resolving path through unstructured list element on '%3.%2', which is a list of type '%4'",
                instr_name, field_name, list.get_table()->get_name(), col.get_type()));
        }
        REALM_UNREACHABLE();
        return false;
    }

    bool translate_dictionary_element(Dictionary& dict, InternString key, Instruction::Path::iterator begin,
                                      Instruction::Path::const_iterator end, const char* instr_name,
                                      ListPathCallback list_callback, bool ignore_missing_dict_keys, ListPath& path)
    {
        StringData string_key = get_string(key);
        InternDictKey translated_key = m_intern_keys.get_or_add(string_key);
        if (begin == end) {
            if (ignore_missing_dict_keys && dict.find(Mixed{string_key}) == dict.end()) {
                return false;
            }
            return true;
        }

        path.append(translated_key);
        auto col = dict.get_col_key();
        auto table = dict.get_table();
        auto field_name = table->get_column_name(col);

        if (col.get_type() == col_type_Link) {
            auto target = dict.get_target_table();
            if (!target->is_embedded()) {
                handle_error(util::format("%1: Reference through non-embedded link at '%3.%2[%4]'", instr_name,
                                          field_name, table->get_name(), string_key));
            }

            auto embedded_object = dict.get_object(string_key);
            if (!embedded_object) {
                logger.warn("Discarding a local %1 made to an embedded object which no longer exists through "
                            "dictionary key '%2.%3[%4]'",
                            instr_name, table->get_name(), table->get_column_name(col), string_key);
                return false; // discard this instruction as it operates over a non-existant link
            }

            if (auto pfield = mpark::get_if<InternString>(&*begin)) {
                ++begin;
                return translate_field(embedded_object, *pfield, begin, end, instr_name, std::move(list_callback),
                                       ignore_missing_dict_keys, path);
            }
            else {
                handle_error(util::format("%1: Embedded object field reference is not a string", instr_name));
            }
        }
        else {
            handle_error(util::format(
                "%1: Resolving path through non link element on '%3.%2', which is a dictionary of type '%4'",
                instr_name, field_name, table->get_name(), col.get_type()));
        }
    }

    bool translate_field(Obj& obj, InternString field, Instruction::Path::iterator begin,
                         Instruction::Path::const_iterator end, const char* instr_name,
                         ListPathCallback list_callback, bool ignore_missing_dict_keys, ListPath& path)
    {
        auto field_name = get_string(field);
        ColKey col = obj.get_table()->get_column_key(field_name);
        if (!col) {
            handle_error(util::format("%1 instruction for path '%2.%3' could not be found", instr_name,
                                      obj.get_table()->get_name(), field_name));
        }
        path.append(col);
        if (begin == end) {
            if (col.is_list()) {
                auto list = obj.get_listbase_ptr(col);
                return list_callback(*list, uint32_t(-1), path); // Array Clear does not have an index
            }
            return true;
        }

        if (col.is_list()) {
            if (auto pindex = mpark::get_if<uint32_t>(&*begin)) {
                std::unique_ptr<LstBase> list = get_list_from_path(obj, col);
                ++begin;
                return translate_list_element(*list, *pindex, begin, end, instr_name, std::move(list_callback),
                                              ignore_missing_dict_keys, path);
            }
            else {
                handle_error(util::format("%1: List index is not an integer on field '%2' in class '%3'", instr_name,
                                          field_name, obj.get_table()->get_name()));
            }
        }
        else if (col.is_dictionary()) {
            if (auto pkey = mpark::get_if<InternString>(&*begin)) {
                auto dict = obj.get_dictionary(col);
                ++begin;
                return translate_dictionary_element(dict, *pkey, begin, end, instr_name, std::move(list_callback),
                                                    ignore_missing_dict_keys, path);
            }
            else {
                handle_error(util::format("%1: Dictionary key is not a string on field '%2' in class '%3'",
                                          instr_name, field_name, obj.get_table()->get_name()));
            }
        }
        else if (col.get_type() == col_type_Link) {
            auto target = obj.get_table()->get_link_target(col);
            if (!target->is_embedded()) {
                handle_error(util::format("%1: Reference through non-embedded link in field '%2' in class '%3'",
                                          instr_name, field_name, obj.get_table()->get_name()));
            }
            if (obj.is_null(col)) {
                logger.warn(
                    "Discarding a local %1 made to an embedded object which no longer exists along path '%2.%3'",
                    instr_name, obj.get_table()->get_name(), obj.get_table()->get_column_name(col));
                return false; // discard this instruction as it operates over a null link
            }

            auto embedded_object = obj.get_linked_object(col);
            if (auto pfield = mpark::get_if<InternString>(&*begin)) {
                ++begin;
                return translate_field(embedded_object, *pfield, begin, end, instr_name, std::move(list_callback),
                                       ignore_missing_dict_keys, path);
            }
            else {
                handle_error(util::format("%1: Embedded object field reference is not a string", instr_name));
            }
        }
        else {
            handle_error(util::format("%1: Resolving path through unstructured field '%3.%2' of type %4", instr_name,
                                      field_name, obj.get_table()->get_name(), col.get_type()));
        }
        REALM_UNREACHABLE();
    }

    bool translate_path(instr::PathInstruction& instr, const char* instr_name, ListPathCallback list_callback,
                        bool ignore_missing_dict_keys = false)
    {
        Obj obj;
        if (auto mobj = get_top_object(instr, instr_name)) {
            obj = std::move(*mobj);
        }
        else {
            logger.warn("Cannot recover '%1' which operates on a deleted object", instr_name);
            return false;
        }
        ListPath path(obj.get_table()->get_key(), obj.get_key());
        return translate_field(obj, instr.field, instr.path.begin(), instr.path.end(), instr_name,
                               std::move(list_callback), ignore_missing_dict_keys, path);
    }

    void operator()(const Instruction::AddTable& instr)
    {
        // Rely on InstructionApplier to validate existing tables
        StringData class_name = get_string(instr.table);
        try {
            InstructionApplier::operator()(instr);
        }
        catch (const std::runtime_error& err) {
            handle_error(util::format(
                "While recovering from a client reset, an AddTable instruction for '%1' could not be applied: '%2'",
                class_name, err.what()));
        }
    }

    void operator()(const Instruction::EraseTable& instr)
    {
        // Destructive schema changes are not allowed by the resetting client.
        static_cast<void>(instr);
        StringData class_name = get_string(instr.table);
        handle_error(util::format("Types cannot be erased during client reset recovery: '%1'", class_name));
    }

    void operator()(const Instruction::CreateObject& instr)
    {
        // This should always succeed, and no path translation is needed because Create operates on top level objects.
        InstructionApplier::operator()(instr);
    }

    void operator()(const Instruction::EraseObject& instr)
    {
        if (auto obj = get_top_object(instr, "EraseObject")) {
            // FIXME: The InstructionApplier uses obj->invalidate() rather than remove(). It should have the same net
            // effect, but that is not the case. Notably when erasing an object which has links from a Lst<Mixed> the
            // list size does not decrease because there is no hiding the unresolved (null) element.
            // InstructionApplier::operator()(instr);
            obj->remove();
        }
        // if the object doesn't exist, a local delete is a no-op.
    }

    void operator()(const Instruction::Update& instr)
    {
        const char* instr_name = "Update";
        Instruction::Update instr_copy = instr;
        const bool dictionary_erase = instr.value.type == instr::Payload::Type::Erased;
        if (translate_path(
                instr_copy, instr_name,
                [&](LstBase& list, uint32_t index, const ListPath& path) {
                    auto cross_index = m_lists[path].update(index);
                    if (cross_index) {
                        instr_copy.prior_size = static_cast<uint32_t>(list.size());
                        instr_copy.path.back() = cross_index->remote;
                    }
                    return bool(cross_index);
                },
                dictionary_erase)) {
            if (!check_links_exist(instr_copy.value)) {
                if (!allows_null_links(instr_copy, instr_name)) {
                    logger.warn("Discarding an update which links to a deleted object");
                    return;
                }
                instr_copy.value = {};
            }
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::AddInteger& instr)
    {
        const char* instr_name = "AddInteger";
        Instruction::AddInteger instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase&, uint32_t, const ListPath&) {
                REALM_UNREACHABLE();
                return true;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::Clear& instr)
    {
        const char* instr_name = "Clear";
        Instruction::Clear instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase&, uint32_t ndx, const ListPath& path) {
                REALM_ASSERT(ndx == uint32_t(-1));
                m_lists[path].clear();
                // Clear.prior_size is ignored and always zero
                return true;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::AddColumn& instr)
    {
        // Rather than duplicating a bunch of validation, use the existing type checking
        // that happens when adding a preexisting column and if there is a problem catch
        // the BadChangesetError and stop recovery
        try {
            InstructionApplier::operator()(instr);
        }
        catch (const BadChangesetError& err) {
            handle_error(util::format(
                "While recovering during client reset, an AddColumn instruction could not be applied: '%1'",
                err.message()));
        }
    }

    void operator()(const Instruction::EraseColumn& instr)
    {
        // Destructive schema changes are not allowed by the resetting client.
        static_cast<void>(instr);
        handle_error(util::format("Properties cannot be erased during client reset recovery"));
    }

    void operator()(const Instruction::ArrayInsert& instr)
    {
        const char* instr_name = "ArrayInsert";
        if (!check_links_exist(instr.value)) {
            logger.warn("Discarding %1 which links to a deleted object", instr_name);
            return;
        }
        Instruction::ArrayInsert instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase& list, uint32_t index, const ListPath& path) {
                REALM_ASSERT(index != uint32_t(-1));
                size_t list_size = list.size();
                auto cross_index = m_lists[path].insert(index, list_size);
                if (cross_index) {
                    instr_copy.path.back() = cross_index->remote;
                    instr_copy.prior_size = static_cast<uint32_t>(list_size);
                }
                return bool(cross_index);
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::ArrayMove& instr)
    {
        const char* instr_name = "ArrayMove";
        Instruction::ArrayMove instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase& list, uint32_t index, const ListPath& path) {
                REALM_ASSERT(index != uint32_t(-1));
                size_t lst_size = list.size();
                uint32_t translated_from, translated_to;
                bool allowed_to_move = m_lists[path].move(static_cast<uint32_t>(index), instr.ndx_2, lst_size,
                                                          translated_from, translated_to);
                if (allowed_to_move) {
                    instr_copy.prior_size = static_cast<uint32_t>(lst_size);
                    instr_copy.path.back() = translated_from;
                    instr_copy.ndx_2 = translated_to;
                }
                return allowed_to_move;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::ArrayErase& instr)
    {
        const char* instr_name = "ArrayErase";
        Instruction::ArrayErase instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase& list, uint32_t index, const ListPath& path) {
                auto obj = list.get_obj();
                uint32_t translated_index;
                bool allowed_to_delete = m_lists[path].remove(static_cast<uint32_t>(index), translated_index);
                if (allowed_to_delete) {
                    instr_copy.prior_size = static_cast<uint32_t>(list.size());
                    instr_copy.path.back() = translated_index;
                }
                return allowed_to_delete;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::SetInsert& instr)
    {
        const char* instr_name = "SetInsert";
        if (!check_links_exist(instr.value)) {
            logger.warn("Discarding a %1 which links to a deleted object", instr_name);
            return;
        }
        Instruction::SetInsert instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase&, uint32_t, const ListPath&) {
                REALM_UNREACHABLE(); // there is validation before this point
                return false;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }

    void operator()(const Instruction::SetErase& instr)
    {
        const char* instr_name = "SetErase";
        Instruction::SetErase instr_copy = instr;
        if (translate_path(instr_copy, instr_name, [&](LstBase&, uint32_t, const ListPath&) {
                REALM_UNREACHABLE(); // there is validation before this point
                return false;
            })) {
            InstructionApplier::operator()(instr_copy);
        }
    }
};

} // anonymous namespace

void client_reset::transfer_group(const Transaction& group_src, Transaction& group_dst, util::Logger& logger)
{
    logger.debug("transfer_group, src size = %1, dst size = %2", group_src.size(), group_dst.size());

    // Find all tables in dst that should be removed.
    std::set<std::string> tables_to_remove;
    for (auto table_key : group_dst.get_table_keys()) {
        if (!group_dst.table_is_public(table_key))
            continue;
        StringData table_name = group_dst.get_table_name(table_key);
        logger.debug("key = %1, table_name = %2", table_key.value, table_name);
        ConstTableRef table_src = group_src.get_table(table_name);
        if (!table_src) {
            logger.debug("Table '%1' will be removed", table_name);
            tables_to_remove.insert(table_name);
            continue;
        }
        // Check whether the table type is the same.
        TableRef table_dst = group_dst.get_table(table_key);
        auto pk_col_src = table_src->get_primary_key_column();
        auto pk_col_dst = table_dst->get_primary_key_column();
        bool has_pk_src = bool(pk_col_src);
        bool has_pk_dst = bool(pk_col_dst);
        if (has_pk_src != has_pk_dst) {
            throw ClientResetFailed(util::format("Client reset requires a primary key column in %1 table '%2'",
                                                 (has_pk_src ? "dest" : "source"), table_name));
        }
        if (!has_pk_src)
            continue;

        // Now the tables both have primary keys. Check type.
        if (pk_col_src.get_type() != pk_col_dst.get_type()) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key types (%1 vs %2) on '%3'",
                             pk_col_src.get_type(), pk_col_dst.get_type(), table_name));
        }
        // Check collection type, nullability etc. but having an index doesn't matter;
        ColumnAttrMask pk_col_src_attr = pk_col_src.get_attrs();
        ColumnAttrMask pk_col_dst_attr = pk_col_dst.get_attrs();
        pk_col_src_attr.reset(ColumnAttr::col_attr_Indexed);
        pk_col_dst_attr.reset(ColumnAttr::col_attr_Indexed);
        if (pk_col_src_attr != pk_col_dst_attr) {
            throw ClientResetFailed(
                util::format("Client reset found incompatible primary key attributes (%1 vs %2) on '%3'",
                             pk_col_src.value, pk_col_dst.value, table_name));
        }
        // Check name.
        StringData pk_col_name_src = table_src->get_column_name(pk_col_src);
        StringData pk_col_name_dst = table_dst->get_column_name(pk_col_dst);
        if (pk_col_name_src != pk_col_name_dst) {
            throw ClientResetFailed(
                util::format("Client reset requires equal pk column names but '%1' != '%2' on '%3'", pk_col_name_src,
                             pk_col_name_dst, table_name));
        }
        // The table survives.
        logger.debug("Table '%1' will remain", table_name);
    }

    // If there have been any tables marked for removal stop.
    // We consider two possible options for recovery:
    // 1: Remove the tables. But this will generate destructive schema
    //    schema changes that the local Realm cannot advance through.
    //    Since this action will fail down the line anyway, give up now.
    // 2: Keep the tables locally and ignore them. But the local app schema
    //    still has these classes and trying to modify anything in them will
    //    create sync instructions on tables that sync doesn't know about.
    if (!tables_to_remove.empty()) {
        std::string names_list;
        for (const std::string& table_name : tables_to_remove) {
            names_list += Group::table_name_to_class_name(table_name);
            names_list += ", ";
        }
        if (names_list.size() > 2) {
            // remove the final ", "
            names_list = names_list.substr(0, names_list.size() - 2);
        }
        throw ClientResetFailed(
            util::format("Client reset cannot recover when classes have been removed: {%1}", names_list));
    }

    // Create new tables in dst if needed.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        auto pk_col_src = table_src->get_primary_key_column();
        TableRef table_dst = group_dst.get_table(table_name);
        if (!table_dst) {
            // Create the table.
            if (table_src->is_embedded()) {
                REALM_ASSERT(!pk_col_src);
                group_dst.add_embedded_table(table_name);
            }
            else {
                REALM_ASSERT(pk_col_src); // a sync table will have a pk
                auto pk_col_src = table_src->get_primary_key_column();
                DataType pk_type = DataType(pk_col_src.get_type());
                StringData pk_col_name = table_src->get_column_name(pk_col_src);
                group_dst.add_table_with_primary_key(table_name, pk_type, pk_col_name, pk_col_src.is_nullable());
            }
        }
    }

    // Now the class tables are identical.
    size_t num_tables;
    {
        size_t num_tables_src = 0;
        for (auto table_key : group_src.get_table_keys()) {
            if (group_src.table_is_public(table_key))
                ++num_tables_src;
        }
        size_t num_tables_dst = 0;
        for (auto table_key : group_dst.get_table_keys()) {
            if (group_dst.table_is_public(table_key))
                ++num_tables_dst;
        }
        REALM_ASSERT(num_tables_src == num_tables_dst);
        num_tables = num_tables_src;
    }
    logger.debug("The number of tables is %1", num_tables);

    // Remove columns in dst if they are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        std::vector<std::string> columns_to_remove;
        for (ColKey col_key : table_dst->get_column_keys()) {
            StringData col_name = table_dst->get_column_name(col_key);
            ColKey col_key_src = table_src->get_column_key(col_name);
            if (!col_key_src) {
                columns_to_remove.push_back(col_name);
                continue;
            }
        }
        if (!columns_to_remove.empty()) {
            std::string columns_list;
            for (const std::string& col_name : columns_to_remove) {
                columns_list += col_name;
                columns_list += ", ";
            }
            throw ClientResetFailed(
                util::format("Client reset cannot recover when columns have been removed from '%1': {%2}", table_name,
                             columns_list));
        }
    }

    // Add columns in dst if present in src and absent in dst.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_dst);
        for (ColKey col_key : table_src->get_column_keys()) {
            StringData col_name = table_src->get_column_name(col_key);
            ColKey col_key_dst = table_dst->get_column_key(col_name);
            if (!col_key_dst) {
                DataType col_type = table_src->get_column_type(col_key);
                bool nullable = col_key.is_nullable();
                bool has_search_index = table_src->has_search_index(col_key);
                logger.trace("Create column, table = %1, column name = %2, "
                             " type = %3, nullable = %4, has_search_index = %5",
                             table_name, col_name, col_key.get_type(), nullable, has_search_index);
                ColKey col_key_dst;
                if (Table::is_link_type(col_key.get_type())) {
                    ConstTableRef target_src = table_src->get_link_target(col_key);
                    TableRef target_dst = group_dst.get_table(target_src->get_name());
                    if (col_key.is_list()) {
                        col_key_dst = table_dst->add_column_list(*target_dst, col_name);
                    }
                    else if (col_key.is_set()) {
                        col_key_dst = table_dst->add_column_set(*target_dst, col_name);
                    }
                    else if (col_key.is_dictionary()) {
                        DataType key_type = table_src->get_dictionary_key_type(col_key);
                        col_key_dst = table_dst->add_column_dictionary(*target_dst, col_name, key_type);
                    }
                    else {
                        REALM_ASSERT(!col_key.is_collection());
                        col_key_dst = table_dst->add_column(*target_dst, col_name);
                    }
                }
                else if (col_key.is_list()) {
                    col_key_dst = table_dst->add_column_list(col_type, col_name, nullable);
                }
                else if (col_key.is_set()) {
                    col_key_dst = table_dst->add_column_set(col_type, col_name, nullable);
                }
                else if (col_key.is_dictionary()) {
                    DataType key_type = table_src->get_dictionary_key_type(col_key);
                    col_key_dst = table_dst->add_column_dictionary(col_type, col_name, nullable, key_type);
                }
                else {
                    REALM_ASSERT(!col_key.is_collection());
                    col_key_dst = table_dst->add_column(col_type, col_name, nullable);
                }

                if (has_search_index)
                    table_dst->add_search_index(col_key_dst);
            }
            else {
                // column preexists in dest, make sure the types match
                if (col_key.get_type() != col_key_dst.get_type()) {
                    throw ClientResetFailed(util::format(
                        "Incompatable column type change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.get_type(), col_key_dst.get_type()));
                }
                ColumnAttrMask src_col_attrs = col_key.get_attrs();
                ColumnAttrMask dst_col_attrs = col_key_dst.get_attrs();
                src_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                dst_col_attrs.reset(ColumnAttr::col_attr_Indexed);
                // make sure the attributes such as collection type, nullability etc. match
                // but index equality doesn't matter here.
                if (src_col_attrs != dst_col_attrs) {
                    throw ClientResetFailed(util::format(
                        "Incompatable column attribute change detected during client reset for '%1.%2' (%3 vs %4)",
                        table_name, col_name, col_key.value, col_key_dst.value));
                }
            }
        }
    }

    // Now the schemas are identical.

    // Remove objects in dst that are absent in src.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
            continue;
        auto table_src = group_src.get_table(table_key);
        // There are no primary keys in embedded tables but this is ok, because
        // embedded objects are tied to the lifetime of top level objects.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        logger.debug("Removing objects in '%1'", table_name);
        auto table_dst = group_dst.get_table(table_name);

        auto pk_col = table_dst->get_primary_key_column();
        REALM_ASSERT_DEBUG(pk_col); // sync realms always have a pk
        std::vector<std::pair<Mixed, ObjKey>> objects_to_remove;
        for (auto obj : *table_dst) {
            auto pk = obj.get_any(pk_col);
            if (!table_src->find_primary_key(pk)) {
                objects_to_remove.emplace_back(pk, obj.get_key());
            }
        }
        for (auto& pair : objects_to_remove) {
            logger.debug("  removing '%1'", pair.first);
            table_dst->remove_object(pair.second);
        }
    }

    EmbeddedObjectConverter embedded_tracker;

    // Now src and dst have identical schemas and no extraneous objects from dst.
    // There may be missing object from src and the values of existing objects may
    // still differ. Diff all the values and create missing objects on the fly.
    for (auto table_key : group_src.get_table_keys()) {
        if (!group_src.table_is_public(table_key))
            continue;
        ConstTableRef table_src = group_src.get_table(table_key);
        // Embedded objects don't have a primary key, so they are handled
        // as a special case when they are encountered as a link value.
        if (table_src->is_embedded())
            continue;
        StringData table_name = table_src->get_name();
        TableRef table_dst = group_dst.get_table(table_name);
        REALM_ASSERT(table_src->get_column_count() == table_dst->get_column_count());
        auto pk_col = table_src->get_primary_key_column();
        REALM_ASSERT(pk_col);
        logger.debug("Updating values for table '%1', number of rows = %2, "
                     "number of columns = %3, primary_key_col = %4, "
                     "primary_key_type = %5",
                     table_name, table_src->size(), table_src->get_column_count(), pk_col.get_index().val,
                     pk_col.get_type());

        InterRealmObjectConverter converter(table_src, table_dst, embedded_tracker);

        for (const Obj& src : *table_src) {
            auto src_pk = src.get_primary_key();
            bool updated = false;
            // get or create the object
            auto dst = table_dst->create_object_with_primary_key(src_pk, &updated);
            REALM_ASSERT(dst);

            converter.copy(src, dst, &updated);
            if (updated) {
                logger.debug("  updating %1", src_pk);
            }
        }
        embedded_tracker.process_pending();
    }
}

client_reset::LocalVersionIDs client_reset::perform_client_reset_diff(DB& db_local, DBRef db_remote,
                                                                      sync::SaltedFileIdent client_file_ident,
                                                                      util::Logger& logger,
                                                                      bool recover_local_changes)
{
    logger.info("Client reset, path_local = %1, "
                "client_file_ident.ident = %2, "
                "client_file_ident.salt = %3,"
                "remote = %4, mode = %5",
                db_local.get_path(), client_file_ident.ident, client_file_ident.salt,
                (db_remote ? db_remote->get_path() : "<none>"), recover_local_changes ? "recover" : "discardLocal");

    auto wt_local = db_local.start_write();
    auto history_local = dynamic_cast<ClientHistory*>(wt_local->get_replication()->_get_history_write());
    REALM_ASSERT(history_local);
    VersionID old_version_local = wt_local->get_version_of_current_transaction();
    sync::version_type current_version_local = old_version_local.version;
    wt_local->get_history()->ensure_updated(current_version_local);
    BinaryData recovered_changeset;
    std::vector<ChunkedBinaryData> local_changes;

    if (recover_local_changes) {
        local_changes = history_local->get_local_changes(current_version_local);
        logger.info("Local changesets to recover: %1", local_changes.size());
    }

    sync::SaltedVersion fresh_server_version = {0, 0};

    if (db_remote) {
        auto wt_remote = db_remote->start_write();
        auto history_remote = dynamic_cast<ClientHistory*>(wt_remote->get_replication()->_get_history_write());
        REALM_ASSERT(history_remote);
        sync::version_type current_version_remote = wt_remote->get_version();
        history_local->set_client_file_ident_in_wt(current_version_local, client_file_ident);
        history_remote->set_client_file_ident_in_wt(current_version_remote, client_file_ident);

        sync::version_type remote_version;
        SaltedFileIdent remote_ident;
        SyncProgress remote_progress;
        history_remote->get_status(remote_version, remote_ident, remote_progress);
        fresh_server_version = remote_progress.latest_server_version;

        if (recover_local_changes) {
            RecoverLocalChangesetsHandler handler{*wt_remote, *wt_local, logger};
            handler.process_changesets(local_changes); // throws on error
            ClientReplication* client_repl = dynamic_cast<ClientReplication*>(wt_remote->get_replication());
            REALM_ASSERT_RELEASE(client_repl);
            ChangesetEncoder& encoder = client_repl->get_instruction_encoder();
            const sync::ChangesetEncoder::Buffer& buffer = encoder.buffer();
            recovered_changeset = {buffer.data(), buffer.size()};
        }

        transfer_group(*wt_remote, *wt_local, logger);
    }

    history_local->set_client_reset_adjustments(current_version_local, client_file_ident, fresh_server_version,
                                                recovered_changeset);

    // Finally, the local Realm is committed. The changes to the remote Realm are discarded.
    wt_local->commit_and_continue_as_read();
    VersionID new_version_local = wt_local->get_version_of_current_transaction();
    logger.info("perform_client_reset_diff is done, old_version.version = %1, "
                "old_version.index = %2, new_version.version = %3, "
                "new_version.index = %4",
                old_version_local.version, old_version_local.index, new_version_local.version,
                new_version_local.index);

    return LocalVersionIDs{old_version_local, new_version_local};
}
