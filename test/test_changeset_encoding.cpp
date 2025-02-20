#include "test.hpp"

#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_encoder.hpp>
#include <realm/sync/changeset_parser.hpp>

using namespace realm;
using namespace realm::sync::instr;
using realm::sync::Changeset;

namespace {
Changeset encode_then_parse(const Changeset& changeset)
{
    using realm::util::SimpleNoCopyInputStream;

    sync::ChangesetEncoder::Buffer buffer;
    encode_changeset(changeset, buffer);
    SimpleNoCopyInputStream stream{buffer};
    Changeset parsed;
    parse_changeset(stream, parsed);
    return parsed;
}
} // namespace

TEST(ChangesetEncoding_AddTable)
{
    Changeset changeset;
    AddTable instr;
    instr.table = changeset.intern_string("Foo");
    instr.type = AddTable::PrimaryKeySpec{
        changeset.intern_string("pk"),
        Payload::Type::Int,
        true,
    };
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_EraseTable)
{
    Changeset changeset;
    EraseTable instr;
    instr.table = changeset.intern_string("Foo");
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_AddColumn)
{
    Changeset changeset;
    AddColumn instr;
    instr.table = changeset.intern_string("Foo");
    instr.field = changeset.intern_string("foo");
    instr.type = Payload::Type::Link;
    instr.collection_type = AddColumn::CollectionType::List;
    instr.nullable = false;
    instr.link_target_table = changeset.intern_string("Bar");
    instr.key_type = Payload::Type::Null;
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_EraseColumn)
{
    Changeset changeset;
    EraseColumn instr;
    instr.table = changeset.intern_string("Foo");
    instr.field = changeset.intern_string("foo");
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_CreateObject)
{
    Changeset changeset;
    CreateObject instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_Update_Field)
{
    Changeset changeset;
    sync::instr::Update instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    instr.field = changeset.intern_string("bar");
    instr.is_default = true;
    CHECK(!instr.is_array_update());
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_Update_Deep)
{
    Changeset changeset;
    sync::instr::Update instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    instr.field = changeset.intern_string("bar");
    instr.is_default = true;
    instr.path.push_back(changeset.intern_string("baz"));
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(changeset.intern_string("boo"));
    CHECK(!instr.is_array_update());
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_Update_ArrayUpdate)
{
    Changeset changeset;
    sync::instr::Update instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    instr.field = changeset.intern_string("bar");
    instr.prior_size = 500;
    instr.path.push_back(123);
    CHECK(instr.is_array_update());
    CHECK_EQUAL(instr.index(), 123);
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_Update_ArrayUpdate_Deep)
{
    Changeset changeset;
    sync::instr::Update instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    instr.field = changeset.intern_string("bar");
    instr.prior_size = 500;
    instr.path.push_back(changeset.intern_string("baz"));
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(changeset.intern_string("boo"));
    instr.path.push_back(123);
    CHECK(instr.is_array_update());
    CHECK_EQUAL(instr.index(), 123);
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_AddInteger)
{
    Changeset changeset;
    AddInteger instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{123};
    instr.field = changeset.intern_string("bar");
    instr.value = 500;
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_ArrayInsert)
{
    Changeset changeset;
    ArrayInsert instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{mpark::monostate{}};
    instr.field = changeset.intern_string("foo");
    instr.path.push_back(123);
    instr.path.push_back(234);
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(5);
    instr.value = Payload{changeset.append_string("Hello, World!")};
    instr.prior_size = 123;
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_ArrayMove)
{
    Changeset changeset;
    ArrayMove instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{mpark::monostate{}};
    instr.field = changeset.intern_string("foo");
    instr.path.push_back(123);
    instr.path.push_back(234);
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(5);
    instr.prior_size = 123;
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_ArrayErase)
{
    Changeset changeset;
    ArrayErase instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{mpark::monostate{}};
    instr.field = changeset.intern_string("foo");
    instr.path.push_back(123);
    instr.path.push_back(234);
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(5);
    instr.prior_size = 123;
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_Clear)
{
    Changeset changeset;
    Clear instr;
    instr.table = changeset.intern_string("Foo");
    instr.object = PrimaryKey{mpark::monostate{}};
    instr.field = changeset.intern_string("foo");
    instr.path.push_back(123);
    instr.path.push_back(234);
    instr.path.push_back(changeset.intern_string("lol"));
    instr.path.push_back(5);
    changeset.push_back(instr);

    auto parsed = encode_then_parse(changeset);
    CHECK_EQUAL(changeset, parsed);
    CHECK(**changeset.begin() == instr);
}

TEST(ChangesetEncoding_AccentWords)
{
    sync::ChangesetEncoder encoder;

    encoder.intern_string("Prógram");
    encoder.intern_string("Program");
    // Bug #5193 caused "Program" to not be found as an intern string
    // although it was just created before.
    encoder.intern_string("Program");
    auto& buffer = encoder.buffer();

    using realm::util::SimpleNoCopyInputStream;
    SimpleNoCopyInputStream stream{buffer};
    Changeset parsed;
    // This will throw if a string is interned twice.
    CHECK_NOTHROW(parse_changeset(stream, parsed));
}
