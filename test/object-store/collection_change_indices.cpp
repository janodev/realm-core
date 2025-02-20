////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include <catch2/catch.hpp>

#include <realm/object-store/impl/collection_notifier.hpp>

#include "util/index_helpers.hpp"

#include <limits>

using namespace realm;

TEST_CASE("collection_change: insert()") {
    _impl::CollectionChangeBuilder c;

    SECTION("adds the row to the insertions set") {
        c.insert(5);
        c.insert(8);
        REQUIRE_INDICES(c.insertions, 5, 8);
    }

    SECTION("shifts previous insertions and modifications") {
        c.insert(5);
        c.modify(8, 1);

        c.insert(1);
        REQUIRE_INDICES(c.insertions, 1, 6);
        REQUIRE_INDICES(c.modifications, 9);
        REQUIRE_COLUMN_INDICES(c.columns, 1, 9);
    }

    SECTION("does not shift previous deletions") {
        c.erase(8);
        c.erase(3);
        c.insert(5);

        REQUIRE_INDICES(c.insertions, 5);
        REQUIRE_INDICES(c.deletions, 3, 8);
    }

    SECTION("shifts destination of previous moves after the insertion point") {
        c.moves = {{10, 5}, {10, 2}, {3, 10}};
        c.insert(4);
        REQUIRE_MOVES(c, {10, 6}, {10, 2}, {3, 11});
    }
}

TEST_CASE("collection_change: modify()") {
    _impl::CollectionChangeBuilder c;

    SECTION("marks the row as modified") {
        c.modify(5);
        REQUIRE_INDICES(c.modifications, 5);
        REQUIRE(c.columns.empty());
    }

    SECTION("also marks newly inserted rows as modified") {
        c.insert(5);
        c.modify(5);
        REQUIRE_INDICES(c.modifications, 5);
        REQUIRE(c.columns.empty());
    }

    SECTION("is idempotent") {
        c.modify(5);
        c.modify(5);
        c.modify(5);
        c.modify(5);
        REQUIRE_INDICES(c.modifications, 5);
        REQUIRE(c.columns.empty());
    }

    SECTION("marks the appropriate column as modified when applicable") {
        c.modify(5, 2);
        REQUIRE_INDICES(c.modifications, 5);
        REQUIRE(c.columns.size() == 1);
        REQUIRE_INDICES(c.columns[2], 5);

        c.modify(4, 2);
        REQUIRE_INDICES(c.modifications, 4, 5);
        REQUIRE(c.columns.size() == 1);
        REQUIRE_INDICES(c.columns[2], 4, 5);

        c.modify(3, 1);
        REQUIRE_INDICES(c.modifications, 3, 4, 5);
        REQUIRE(c.columns.size() == 2);
        REQUIRE_INDICES(c.columns[1], 3);
        REQUIRE_INDICES(c.columns[2], 4, 5);
    }
}

TEST_CASE("collection_change: erase()") {
    _impl::CollectionChangeBuilder c;

    SECTION("adds the row to the deletions set") {
        c.erase(5);
        REQUIRE_INDICES(c.deletions, 5);
    }

    SECTION("is shifted for previous deletions") {
        c.erase(5);
        c.erase(6);
        REQUIRE_INDICES(c.deletions, 5, 7);
    }

    SECTION("is shifted for previous insertions") {
        c.insert(5);
        c.erase(6);
        REQUIRE_INDICES(c.deletions, 5);
    }

    SECTION("removes previous insertions") {
        c.insert(5);
        c.erase(5);
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());
    }

    SECTION("removes previous modifications") {
        c.modify(5, 0);
        c.erase(5);
        REQUIRE(c.modifications.empty());
        REQUIRE(c.columns[0].empty());
        REQUIRE_INDICES(c.deletions, 5);
    }

    SECTION("shifts previous modifications") {
        c.modify(5, 0);
        c.erase(4);
        REQUIRE_INDICES(c.modifications, 4);
        REQUIRE_COLUMN_INDICES(c.columns, 0, 4);
        REQUIRE_INDICES(c.deletions, 4);
    }

    SECTION("removes previous moves to the row being erased") {
        c.moves = {{10, 5}};
        c.erase(5);
        REQUIRE(c.moves.empty());
    }

    SECTION("shifts the destination of previous moves") {
        c.moves = {{10, 5}, {10, 2}, {3, 10}};
        c.erase(4);
        REQUIRE_MOVES(c, {10, 4}, {10, 2}, {3, 9});
    }
}

TEST_CASE("collection_change: clear()") {
    _impl::CollectionChangeBuilder c;

    SECTION("removes all insertions") {
        c.insertions = {1, 2, 3};
        c.clear(0);
        REQUIRE(c.insertions.empty());
    }

    SECTION("removes all modifications") {
        c.modifications = {1, 2, 3};
        c.clear(0);
        REQUIRE(c.modifications.empty());
    }

    SECTION("removes all moves") {
        c.moves = {{1, 3}};
        c.clear(0);
        REQUIRE(c.moves.empty());
    }

    SECTION("sets deletions to the number of rows before any changes") {
        c.insertions = {1, 2, 3};
        c.clear(5);
        REQUIRE_INDICES(c.deletions, 0, 1);

        c.deletions = {1, 2, 3};
        c.clear(5);
        REQUIRE_INDICES(c.deletions, 0, 1, 2, 3, 4, 5, 6, 7);
    }
}

TEST_CASE("collection_change: move()") {
    _impl::CollectionChangeBuilder c;

    SECTION("adds the move to the list of moves") {
        c.move(5, 6);
        REQUIRE_MOVES(c, {5, 6});
    }

    SECTION("updates previous moves to the source of this move") {
        c.move(5, 6);
        c.move(6, 7);
        REQUIRE_MOVES(c, {5, 7});
    }

    SECTION("shifts previous moves and is shifted by them") {
        c.move(5, 10);
        c.move(6, 12);
        REQUIRE_MOVES(c, {5, 9}, {7, 12});

        c.move(10, 0);
        REQUIRE_MOVES(c, {5, 10}, {7, 12}, {11, 0});
    }

    SECTION("does not report a move if the source is newly inserted") {
        c.insert(5);
        c.move(5, 10);
        REQUIRE_INDICES(c.insertions, 10);
        REQUIRE(c.moves.empty());
    }

    SECTION("shifts previous insertions and modifications") {
        c.insert(5);
        c.modify(6, 0);
        c.move(10, 0);
        REQUIRE_INDICES(c.insertions, 0, 6);
        REQUIRE_INDICES(c.modifications, 7);
        REQUIRE_COLUMN_INDICES(c.columns, 0, 7);
        REQUIRE_MOVES(c, {9, 0});
    }

    SECTION("marks the target row as modified if the source row was") {
        c.modify(5, 0);

        c.move(5, 10);
        REQUIRE_INDICES(c.modifications, 10);
        REQUIRE_COLUMN_INDICES(c.columns, 0, 10);

        c.move(6, 12);
        REQUIRE_INDICES(c.modifications, 9);
        REQUIRE_COLUMN_INDICES(c.columns, 0, 9);
    }

    SECTION("bumps previous moves to the same location") {
        c.move(5, 10);
        c.move(7, 10);
        REQUIRE_MOVES(c, {5, 9}, {8, 10});

        c = {};
        c.move(5, 10);
        c.move(15, 10);
        REQUIRE_MOVES(c, {5, 11}, {15, 10});
    }

    SECTION("collapses redundant swaps of adjacent rows to a no-op") {
        c.move(7, 8);
        c.move(7, 8);
        c.clean_up_stale_moves();
        REQUIRE(c.empty());
    }
}

TEST_CASE("collection_change: calculate() table order") {
    _impl::CollectionChangeBuilder c;

    auto all_modified = [](auto) {
        return true;
    };
    auto none_modified = [](auto) {
        return false;
    };
    bool in_table_order = true;

    SECTION("returns an empty set when input and output are identical") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, none_modified, in_table_order);
        REQUIRE(c.empty());
    }

    SECTION("marks all as inserted when prev is empty") {
        c = _impl::CollectionChangeBuilder::calculate({}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.insertions, 0, 1, 2);
    }

    SECTION("marks all as deleted when new is empty") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 0, 1, 2);
    }

    SECTION("marks npos rows in prev as deleted") {
        c = _impl::CollectionChangeBuilder::calculate({-1, 1, 2, 3, -1}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 0, 4);
    }

    SECTION("marks modified rows which do not move as modified") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.modifications, 0, 1, 2);
    }

    SECTION("does not mark unmodified rows as modified") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, none_modified, in_table_order);
        REQUIRE(c.modifications.empty());
    }

    SECTION("marks newly added rows as insertions") {
        c = _impl::CollectionChangeBuilder::calculate({2, 3}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.insertions, 0);

        c = _impl::CollectionChangeBuilder::calculate({1, 3}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.insertions, 1);

        c = _impl::CollectionChangeBuilder::calculate({1, 2}, {1, 2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.insertions, 2);
    }

    SECTION("marks removed rows as deleted") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 2);

        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 1);

        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {2, 3}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 0);
    }

    SECTION("marks rows as both inserted and deleted") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3, 4}, all_modified, in_table_order);
        REQUIRE_INDICES(c.deletions, 1);
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE(c.moves.empty());
    }

    SECTION("does not mark rows as modified if they are new") {
        c = _impl::CollectionChangeBuilder::calculate({3}, {3, 5}, all_modified, in_table_order);
        REQUIRE_INDICES(c.modifications, 0);
    }

#if 0 // FIXME: these tests might be applicable to LinkingObjects
    SECTION("reports moves which can be produced by move_last_over()") {
        auto calc = [&](std::vector<int64_t> values) {
            return _impl::CollectionChangeBuilder::calculate(values, {1, 2, 3}, none_modified, all);
        };

        REQUIRE(calc({1, 2, 3}).empty());
        REQUIRE_MOVES(calc({1, 3, 2}), {2, 1});
        REQUIRE_MOVES(calc({2, 1, 3}), {1, 0});
        REQUIRE_MOVES(calc({2, 3, 1}), {2, 0});
        REQUIRE_MOVES(calc({3, 1, 2}), {1, 0}, {2, 1});
        REQUIRE_MOVES(calc({3, 2, 1}), {2, 0}, {1, 1});
    }

    SECTION("resolves ambiguous moves using the candidate set") {
        c = _impl::CollectionChangeBuilder::calculate({3, 1, 2}, {1, 2, 3}, none_modified, all);
        REQUIRE_MOVES(c, {1, 0}, {2, 1});
        c = _impl::CollectionChangeBuilder::calculate({3, 1, 2}, {1, 2, 3}, none_modified, IndexSet{3});
        REQUIRE_MOVES(c, {0, 2});
    }
#endif
}

TEST_CASE("collection_change: calculate() sorted") {
    _impl::CollectionChangeBuilder c;

    auto all_modified = [](auto) {
        return true;
    };
    auto none_modified = [](auto) {
        return false;
    };
    size_t npos = -1;

    SECTION("returns an empty set when input and output are identical") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, none_modified);
        REQUIRE(c.empty());
    }

    SECTION("marks all as inserted when prev is empty") {
        c = _impl::CollectionChangeBuilder::calculate({}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.insertions, 0, 1, 2);
    }

    SECTION("marks all as deleted when new is empty") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {}, all_modified);
        REQUIRE_INDICES(c.deletions, 0, 1, 2);
    }

    SECTION("marks npos rows in prev as deleted") {
        c = _impl::CollectionChangeBuilder::calculate({npos, 1, 2, 3, npos}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 0, 4);
    }

    SECTION("marks modified rows which do not move as modified") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.modifications, 0, 1, 2);
    }

    SECTION("does not mark unmodified rows as modified") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2, 3}, none_modified);
        REQUIRE(c.modifications.empty());
    }

    SECTION("marks newly added rows as insertions") {
        c = _impl::CollectionChangeBuilder::calculate({2, 3}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.insertions, 0);

        c = _impl::CollectionChangeBuilder::calculate({1, 3}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.insertions, 1);

        c = _impl::CollectionChangeBuilder::calculate({1, 2}, {1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.insertions, 2);
    }

    SECTION("marks removed rows as deleted") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 2}, all_modified);
        REQUIRE_INDICES(c.deletions, 2);

        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 1);

        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {2, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 0);
    }

    SECTION("marks rows as both inserted and deleted") {
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3, 4}, all_modified);
        REQUIRE_INDICES(c.deletions, 1);
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE(c.moves.empty());
    }

    SECTION("marks rows as modified even if they moved") {
        c = _impl::CollectionChangeBuilder::calculate({3, 5}, {5, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 1);
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.modifications, 0, 1);
    }

    SECTION("does not mark rows as modified if they are new") {
        c = _impl::CollectionChangeBuilder::calculate({3}, {3, 5}, all_modified);
        REQUIRE_INDICES(c.modifications, 0);
    }

    SECTION("reports inserts/deletes for simple reorderings") {
        auto calc = [&](std::vector<size_t> old_rows, std::vector<size_t> new_rows) {
            return _impl::CollectionChangeBuilder::calculate(old_rows, new_rows, none_modified);
        };

        c = calc({1, 2, 3}, {1, 2, 3});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());

        c = calc({1, 2, 3}, {1, 3, 2});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({1, 2, 3}, {2, 1, 3});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({1, 2, 3}, {2, 3, 1});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({1, 2, 3}, {3, 1, 2});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({1, 2, 3}, {3, 2, 1});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({1, 3, 2}, {1, 2, 3});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({1, 3, 2}, {1, 3, 2});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());

        c = calc({1, 3, 2}, {2, 1, 3});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({1, 3, 2}, {2, 3, 1});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({1, 3, 2}, {3, 1, 2});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({1, 3, 2}, {3, 2, 1});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({2, 1, 3}, {1, 2, 3});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({2, 1, 3}, {1, 3, 2});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({2, 1, 3}, {2, 1, 3});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());

        c = calc({2, 1, 3}, {2, 3, 1});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({2, 1, 3}, {3, 1, 2});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({2, 1, 3}, {3, 2, 1});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({2, 3, 1}, {1, 2, 3});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({2, 3, 1}, {1, 3, 2});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({2, 3, 1}, {2, 1, 3});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({2, 3, 1}, {2, 3, 1});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());

        c = calc({2, 3, 1}, {3, 1, 2});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({2, 3, 1}, {3, 2, 1});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({3, 1, 2}, {1, 2, 3});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({3, 1, 2}, {1, 3, 2});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({3, 1, 2}, {2, 1, 3});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({3, 1, 2}, {2, 3, 1});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({3, 1, 2}, {3, 1, 2});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());

        c = calc({3, 1, 2}, {3, 2, 1});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({3, 2, 1}, {1, 2, 3});
        REQUIRE_INDICES(c.insertions, 0, 1);
        REQUIRE_INDICES(c.deletions, 1, 2);

        c = calc({3, 2, 1}, {1, 3, 2});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({3, 2, 1}, {2, 1, 3});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.deletions, 0);

        c = calc({3, 2, 1}, {2, 3, 1});
        REQUIRE_INDICES(c.insertions, 0);
        REQUIRE_INDICES(c.deletions, 1);

        c = calc({3, 2, 1}, {3, 1, 2});
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.deletions, 2);

        c = calc({3, 2, 1}, {3, 2, 1});
        REQUIRE(c.insertions.empty());
        REQUIRE(c.deletions.empty());
    }

    SECTION("prefers to produce diffs where modified rows are the ones to move when it is ambiguous") {
        auto two_modified = [](auto ndx) {
            return ndx == 2;
        };
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3, 2}, two_modified);
        REQUIRE_INDICES(c.deletions, 1);
        REQUIRE_INDICES(c.insertions, 2);

        auto three_modified = [](auto ndx) {
            return ndx == 3;
        };
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {1, 3, 2}, three_modified);
        REQUIRE_INDICES(c.deletions, 2);
        REQUIRE_INDICES(c.insertions, 1);
    }

    SECTION("prefers smaller diffs over larger diffs moving only modified rows") {
        auto two_modified = [](auto ndx) {
            return ndx == 2;
        };
        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, {2, 3, 1}, two_modified);
        REQUIRE_INDICES(c.deletions, 0);
        REQUIRE_INDICES(c.insertions, 2);
    }

    SECTION("supports duplicate indices") {
        c = _impl::CollectionChangeBuilder::calculate({1, 1, 2, 2, 3, 3}, {1, 2, 3, 1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 3, 5);
        REQUIRE_INDICES(c.insertions, 1, 2);
    }

    SECTION("deletes and inserts the last option when any in a range could be deleted") {
        c = _impl::CollectionChangeBuilder::calculate({3, 2, 1, 1, 2, 3}, {1, 1, 2, 2, 3, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 0, 1);
        REQUIRE_INDICES(c.insertions, 3, 5);
    }

    SECTION("reports insertions/deletions when the number of duplicate entries changes") {
        c = _impl::CollectionChangeBuilder::calculate({1, 1, 1, 1, 2, 3}, {1, 2, 3, 1}, all_modified);
        REQUIRE_INDICES(c.deletions, 1, 2, 3);
        REQUIRE_INDICES(c.insertions, 3);

        c = _impl::CollectionChangeBuilder::calculate({1, 2, 3, 1}, {1, 1, 1, 1, 2, 3}, all_modified);
        REQUIRE_INDICES(c.deletions, 3);
        REQUIRE_INDICES(c.insertions, 1, 2, 3);
    }

    SECTION("properly recurses into smaller subblocks") {
        std::vector<size_t> prev = {10, 1, 2, 11, 3, 4, 5, 12, 6, 7, 13};
        std::vector<size_t> next = {13, 1, 2, 12, 3, 4, 5, 11, 6, 7, 10};
        c = _impl::CollectionChangeBuilder::calculate(prev, next, all_modified);
        REQUIRE_INDICES(c.deletions, 0, 3, 7, 10);
        REQUIRE_INDICES(c.insertions, 0, 3, 7, 10);
    }

    SECTION("produces diffs which let merge collapse insert -> move -> delete to no-op") {
        auto four_modified = [](auto ndx) {
            return ndx == 4;
        };
        for (int insert_pos = 0; insert_pos < 4; ++insert_pos) {
            for (int move_to_pos = 0; move_to_pos < 4; ++move_to_pos) {
                if (insert_pos == move_to_pos)
                    continue;
                CAPTURE(insert_pos);
                CAPTURE(move_to_pos);

                std::vector<size_t> after_insert = {1, 2, 3};
                after_insert.insert(after_insert.begin() + insert_pos, 4);
                c = _impl::CollectionChangeBuilder::calculate({1, 2, 3}, after_insert, four_modified);

                std::vector<size_t> after_move = {1, 2, 3};
                after_move.insert(after_move.begin() + move_to_pos, 4);
                c.merge(_impl::CollectionChangeBuilder::calculate(after_insert, after_move, four_modified));

                c.merge(_impl::CollectionChangeBuilder::calculate(after_move, {1, 2, 3}, four_modified));
                REQUIRE(c.empty());
            }
        }
    }
}

TEST_CASE("collection_change: merge()") {
    _impl::CollectionChangeBuilder c;

    SECTION("is a no-op if the new set is empty") {
        c = {{1, 2, 3}, {4, 5}, {6, 7}, {{8, 9}}};
        c.columns = {{0, {6}}, {1, {7}}};
        c.merge({});
        REQUIRE_INDICES(c.deletions, 1, 2, 3, 8);
        REQUIRE_INDICES(c.insertions, 4, 5, 9);
        REQUIRE_INDICES(c.modifications, 6, 7);
        REQUIRE_COLUMN_INDICES(c.columns, 0, 6);
        REQUIRE_COLUMN_INDICES(c.columns, 1, 7);
        REQUIRE_MOVES(c, {8, 9});
    }

    SECTION("replaces the set with the new set if the old set is empty") {
        c.merge({{1, 2, 3}, {4, 5}, {6, 7}, {{8, 9}}});
        REQUIRE_INDICES(c.deletions, 1, 2, 3, 8);
        REQUIRE_INDICES(c.insertions, 4, 5, 9);
        REQUIRE_INDICES(c.modifications, 6, 7);
        REQUIRE_MOVES(c, {8, 9});
    }

    SECTION("clear collection flag gets propagated") {
        c = {{1, 2, 3}, {}, {}, {}, false, true};
        c.merge({});
        REQUIRE(c.collection_was_cleared);

        c = {{1, 2, 3}, {4, 5}, {6, 7}, {{8, 9}}, false, false};
        c.merge({});
        REQUIRE(!c.collection_was_cleared);

        c = {};
        c.merge({{1, 2, 3}, {}, {}, {}, false, true});
        REQUIRE(c.collection_was_cleared);

        c = {};
        c.merge({{1, 2, 3}, {4, 5}, {6, 7}, {{8, 9}}, false, false});
        REQUIRE(!c.collection_was_cleared);

        c = {{1, 2, 3}, {4, 5}, {6, 7}, {{8, 9}}, false, false};
        c.merge({{1, 2, 3}, {}, {}, {}, false, true});
        REQUIRE(c.collection_was_cleared);

        c = {{1, 2, 3}, {}, {}, {}, false, true};
        c.merge({{}, {1, 2, 3}, {}, {}, false, false});
        REQUIRE(!c.collection_was_cleared);

        c = {{1, 2, 3}, {}, {}, {}, false, true};
        c.merge({{1, 2, 3}, {1, 2, 3}, {}, {}, false, true});
        REQUIRE(c.collection_was_cleared);

        c = {{1, 2, 3}, {4, 5}, {}, {}, false, false};
        c.merge({{}, {1, 2, 3}, {}, {}, false, false});
        REQUIRE(!c.collection_was_cleared);
    }

    SECTION("shifts deletions by previous deletions") {
        c = {{5}, {}, {}, {}};
        c.merge({{3}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 3, 5);

        c = {{5}, {}, {}, {}};
        c.merge({{4}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 4, 5);

        c = {{5}, {}, {}, {}};
        c.merge({{5}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 5, 6);

        c = {{5}, {}, {}, {}};
        c.merge({{6}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 5, 7);
    }

    SECTION("shifts deletions by previous insertions") {
        c = {{}, {5}, {}, {}};
        c.merge({{4}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 4);

        c = {{}, {5}, {}, {}};
        c.merge({{6}, {}, {}, {}});
        REQUIRE_INDICES(c.deletions, 5);
    }

    SECTION("shifts previous insertions by deletions") {
        c = {{}, {2, 3}, {}, {}};
        c.merge({{1}, {}, {}, {}});
        REQUIRE_INDICES(c.insertions, 1, 2);
    }

    SECTION("removes previous insertions for newly deleted rows") {
        c = {{}, {1, 2}, {}, {}};
        c.merge({{2}, {}, {}, {}});
        REQUIRE_INDICES(c.insertions, 1);
    }

    SECTION("removes previous modifications for newly deleted rows") {
        c = {{}, {}, {2, 3}, {}};
        c.merge({{2}, {}, {}, {}});
        REQUIRE_INDICES(c.modifications, 2);
    }

    SECTION("shifts previous modifications for deletions of other rows") {
        c = {{}, {}, {2, 3}, {}};
        c.merge({{1}, {}, {}, {}});
        REQUIRE_INDICES(c.modifications, 1, 2);
    }

    SECTION("removes moves to rows which have been deleted") {
        c = {{}, {}, {}, {{2, 3}}};
        c.merge({{3}, {}, {}, {}});
        REQUIRE(c.moves.empty());
    }

    SECTION("shifts destinations of previous moves to reflect new deletions") {
        c = {{}, {}, {}, {{2, 5}}};
        c.merge({{3}, {}, {}, {}});
        REQUIRE_MOVES(c, {2, 4});
    }

    SECTION("does not modify old deletions based on new insertions") {
        c = {{1, 3}, {}, {}, {}};
        c.merge({{}, {1, 2, 3}, {}, {}});
        REQUIRE_INDICES(c.deletions, 1, 3);
        REQUIRE_INDICES(c.insertions, 1, 2, 3);
    }

    SECTION("shifts previous insertions to reflect new insertions") {
        c = {{}, {1, 5}, {}, {}};
        c.merge({{}, {1, 4}, {}, {}});
        REQUIRE_INDICES(c.insertions, 1, 2, 4, 7);
    }

    SECTION("shifts previous modifications to reflect new insertions") {
        c = {{}, {}, {1, 5}, {}};
        c.merge({{}, {1, 4}, {}, {}});
        REQUIRE_INDICES(c.modifications, 2, 7);
        REQUIRE_INDICES(c.insertions, 1, 4);
    }

    SECTION("shifts previous move destinations to reflect new insertions") {
        c = {{}, {}, {}, {{2, 5}}};
        c.merge({{}, {3}, {}});
        REQUIRE_MOVES(c, {2, 6});
    }

    SECTION("does not modify old deletions based on new modifications") {
        c = {{1, 2, 3}, {}, {}, {}};
        c.merge({{}, {}, {2}});
        REQUIRE_INDICES(c.deletions, 1, 2, 3);
        REQUIRE_INDICES(c.modifications, 2);
    }

    SECTION("tracks modifications made to previously inserted rows") {
        c = {{}, {2}, {}, {}};
        c.merge({{}, {}, {1, 2, 3}});
        REQUIRE_INDICES(c.insertions, 2);
        REQUIRE_INDICES(c.modifications, 1, 2, 3);
    }

    SECTION("unions modifications with old modifications") {
        c = {{}, {}, {2}, {}};
        c.merge({{}, {}, {1, 2, 3}});
        REQUIRE_INDICES(c.modifications, 1, 2, 3);
    }

    SECTION("tracks modifications for previous moves") {
        c = {{}, {}, {}, {{1, 2}}};
        c.merge({{}, {}, {2, 3}});
        REQUIRE_INDICES(c.modifications, 2, 3);
    }

    SECTION("updates new move sources to reflect previous inserts and deletes") {
        c = {{1}, {}, {}, {}};
        c.merge({{}, {}, {}, {{2, 3}}});
        REQUIRE_MOVES(c, {3, 3});

        c = {{}, {1}, {}, {}};
        c.merge({{}, {}, {}, {{2, 3}}});
        REQUIRE_MOVES(c, {1, 3});

        c = {{2}, {4}, {}, {}};
        c.merge({{}, {}, {}, {{5, 10}}});
        REQUIRE_MOVES(c, {5, 10});
    }

    SECTION("updates the row modified for rows moved after a modification") {
        c = {{}, {}, {1}, {}};
        c.merge({{}, {}, {}, {{1, 3}}});
        REQUIRE_INDICES(c.modifications, 3);
        REQUIRE_MOVES(c, {1, 3});
    }

    SECTION("updates the row modified for chained moves") {
        c = {{}, {}, {1}, {}};
        c.merge({{}, {}, {}, {{1, 3}}});
        c.merge({{}, {}, {}, {{3, 5}}});
        REQUIRE_INDICES(c.modifications, 5);
        REQUIRE_MOVES(c, {1, 5});
    }

    SECTION("updates the row inserted for moves of previously new rows") {
        c = {{}, {1}, {}, {}};
        c.merge({{}, {}, {}, {{1, 3}}});
        REQUIRE(c.moves.empty());
        REQUIRE_INDICES(c.insertions, 3);
    }

    SECTION("updates old moves when the destination is moved again") {
        c = {{}, {}, {}, {{1, 3}}};
        c.merge({{}, {}, {}, {{3, 5}}});
        REQUIRE_MOVES(c, {1, 5});
    }

    SECTION("shifts destination of previous moves to reflect new moves like an insert/delete pair would") {
        c = {{}, {}, {}, {{1, 3}}};
        c.merge({{}, {}, {}, {{2, 5}}});
        REQUIRE_MOVES(c, {1, 2}, {3, 5});

        c = {{}, {}, {}, {{1, 10}}};
        c.merge({{}, {}, {}, {{2, 5}}});
        REQUIRE_MOVES(c, {1, 10}, {3, 5});

        c = {{}, {}, {}, {{5, 10}}};
        c.merge({{}, {}, {}, {{12, 2}}});
        REQUIRE_MOVES(c, {5, 11}, {12, 2});
    }

    SECTION("moves shift previous inserts like an insert/delete pair would") {
        c = {{}, {5}};
        c.merge({{}, {}, {}, {{2, 6}}});
        REQUIRE_INDICES(c.insertions, 4, 6);
    }

    SECTION("moves shift previous modifications like an insert/delete pair would") {
        c = {{}, {}, {5}};
        c.merge({{}, {}, {}, {{2, 6}}});
        REQUIRE_INDICES(c.modifications, 4);
    }

    SECTION("moves are shifted by previous deletions like an insert/delete pair would") {
        c = {{5}};
        c.merge({{}, {}, {}, {{2, 6}}});
        REQUIRE_MOVES(c, {2, 6});

        c = {{5}};
        c.merge({{}, {}, {}, {{6, 2}}});
        REQUIRE_MOVES(c, {7, 2});
    }

    SECTION("leapfrogging rows collapse to an empty changeset") {
        c = {{1}, {0}, {}, {{1, 0}}};
        c.merge({{1}, {0}, {}, {{1, 0}}});
        REQUIRE(c.empty());
    }

    SECTION("modify -> move -> unmove leaves row marked as modified") {
        c = {{}, {}, {1}};
        c.merge({{1}, {2}, {}, {{1, 2}}});
        c.merge({{1}});

        REQUIRE_INDICES(c.deletions, 2);
        REQUIRE(c.insertions.empty());
        REQUIRE(c.moves.empty());
        REQUIRE_INDICES(c.modifications, 1);
    }

    SECTION("modifying a previously moved row which stops being a move due to more deletions") {
        // Make it stop being a move in the same transaction as the modify
        c = {{1, 2}, {0, 1}, {}, {{1, 0}, {2, 1}}};
        c.merge({{0, 2}, {1}, {0}, {}});

        REQUIRE_INDICES(c.deletions, 0, 1);
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.modifications, 0);
        REQUIRE(c.moves.empty());

        // Same net change, but make it no longer a move in the transaction after the modify
        c = {{1, 2}, {0, 1}, {}, {{1, 0}, {2, 1}}};
        c.merge({{}, {}, {1}, {}});
        c.merge({{0, 2}, {0}, {}, {{2, 0}}});
        c.merge({{0}, {1}, {}, {}});

        REQUIRE_INDICES(c.deletions, 0, 1);
        REQUIRE_INDICES(c.insertions, 1);
        REQUIRE_INDICES(c.modifications, 0);
        REQUIRE(c.moves.empty());
    }

    SECTION("column-level modifications") {
        _impl::CollectionChangeBuilder c2;
        c = {{}, {}, {1, 2, 3}, {}};
        c.columns = {{0, {1}}, {1, {2, 3}}};

        SECTION("preserved on no-op merges") {
            c.merge({});
            REQUIRE_COLUMN_INDICES(c.columns, 0, 1);
            REQUIRE_COLUMN_INDICES(c.columns, 1, 2, 3);

            c2.merge(std::move(c));
            REQUIRE_COLUMN_INDICES(c2.columns, 0, 1);
            REQUIRE_COLUMN_INDICES(c2.columns, 1, 2, 3);
        }

        SECTION("merged with other column-level modifications") {
            c2.modifications = {0, 4};
            c2.columns = {{0, {1, 2}}, {2, {4}}};
            c.merge(std::move(c2));

            REQUIRE_COLUMN_INDICES(c.columns, 0, 1, 2);
            REQUIRE_COLUMN_INDICES(c.columns, 1, 2, 3);
            REQUIRE_COLUMN_INDICES(c.columns, 2, 4);
        }

        SECTION("removed by deletions") {
            c2.deletions = {2};
            c.merge(std::move(c2));
            REQUIRE_COLUMN_INDICES(c.columns, 0, 1);
            REQUIRE_COLUMN_INDICES(c.columns, 1, 2);
        }

        SECTION("shifted by insertions") {
            c2.insertions = {3};
            c.merge(std::move(c2));
            REQUIRE_COLUMN_INDICES(c.columns, 0, 1);
            REQUIRE_COLUMN_INDICES(c.columns, 1, 2, 4);
        }
    }
}
