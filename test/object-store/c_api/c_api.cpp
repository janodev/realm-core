#include "catch2/catch.hpp"

#include <realm.h>
#include <realm/object-store/object.hpp>
#include <realm/object-store/c_api/types.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>

#include "sync/flx_sync_harness.hpp"
#include "util/test_file.hpp"
#include "realm/object-store/impl/object_accessor_impl.hpp"

#include <cstring>
#include <numeric>
#include <thread>

using namespace realm;

extern "C" int realm_c_api_tests(const char* file);

template <class T>
T checked(T x)
{
    if (!x) {
        realm_error_t err_info;
        if (realm_get_last_error(&err_info)) {
            FAIL(err_info.message);
        }
    }
    return x;
}

realm_value_t rlm_str_val(const char* str)
{
    realm_value_t val;
    val.type = RLM_TYPE_STRING;
    val.string = realm_string_t{str, std::strlen(str)};
    return val;
}

realm_value_t rlm_int_val(int64_t n)
{
    realm_value_t val;
    val.type = RLM_TYPE_INT;
    val.integer = n;
    return val;
}

realm_value_t rlm_float_val(float fnum)
{
    realm_value_t val;
    val.type = RLM_TYPE_FLOAT;
    val.fnum = fnum;
    return val;
}

realm_value_t rlm_double_val(double dnum)
{
    realm_value_t val;
    val.type = RLM_TYPE_DOUBLE;
    val.dnum = dnum;
    return val;
}

realm_value_t rlm_object_id_val(const char* hex_digits)
{
    size_t len = strlen(hex_digits);
    REALM_ASSERT(len == 12);
    realm_value_t val;
    val.type = RLM_TYPE_OBJECT_ID;
    for (size_t i = 0; i < 12; ++i) {
        val.object_id.bytes[i] = uint8_t(hex_digits[i]);
    }
    return val;
}

realm_value_t rlm_timestamp_val(int64_t seconds, int32_t nanoseconds)
{
    realm_value_t val;
    val.type = RLM_TYPE_TIMESTAMP;
    val.timestamp.seconds = seconds;
    val.timestamp.nanoseconds = nanoseconds;
    return val;
}

realm_value_t rlm_bool_val(bool b)
{
    realm_value_t val;
    val.type = RLM_TYPE_BOOL;
    val.boolean = b;
    return val;
}

realm_value_t rlm_decimal_val(double d)
{
    realm_value_t val;
    val.type = RLM_TYPE_DECIMAL128;

    realm::Decimal128 dec{d};
    val.decimal128.w[0] = dec.raw()->w[0];
    val.decimal128.w[1] = dec.raw()->w[1];

    return val;
}

realm_value_t rlm_uuid_val(const char* str)
{
    realm_value_t val;
    val.type = RLM_TYPE_UUID;
    realm::UUID uuid{realm::StringData{str}};
    auto bytes = uuid.to_bytes();
    for (size_t i = 0; i < 16; ++i) {
        val.uuid.bytes[i] = bytes[i];
    }
    return val;
}

realm_value_t rlm_binary_val(const uint8_t* bytes, size_t len)
{
    realm_value_t val;
    val.type = RLM_TYPE_BINARY;
    val.binary.size = len;
    val.binary.data = bytes;
    return val;
}

realm_value_t rlm_link_val(realm_class_key_t cls, realm_object_key_t obj)
{
    realm_value_t val;
    val.type = RLM_TYPE_LINK;
    val.link.target_table = cls;
    val.link.target = obj;
    return val;
}

realm_value_t rlm_null()
{
    realm_value_t null;
    null.type = RLM_TYPE_NULL;
    return null;
}

std::string rlm_stdstr(realm_value_t val)
{
    CHECK(val.type == RLM_TYPE_STRING);
    return std::string(val.string.data, 0, val.string.size);
}

bool rlm_val_eq(realm_value_t lhs, realm_value_t rhs)
{
    if (lhs.type != rhs.type)
        return false;

    switch (lhs.type) {
        case RLM_TYPE_NULL:
            return true;
        case RLM_TYPE_INT:
            return lhs.integer == rhs.integer;
        case RLM_TYPE_BOOL:
            return lhs.boolean == rhs.boolean;
        case RLM_TYPE_STRING:
            return strncmp(lhs.string.data, rhs.string.data, lhs.string.size) == 0;
        case RLM_TYPE_BINARY:
            return memcmp(lhs.binary.data, rhs.binary.data, lhs.binary.size) == 0;
        case RLM_TYPE_TIMESTAMP:
            return lhs.timestamp.seconds == rhs.timestamp.seconds &&
                   lhs.timestamp.nanoseconds == rhs.timestamp.nanoseconds;
        case RLM_TYPE_FLOAT:
            return lhs.fnum == rhs.fnum;
        case RLM_TYPE_DOUBLE:
            return lhs.dnum == rhs.dnum;
        case RLM_TYPE_DECIMAL128:
            return lhs.decimal128.w[0] == rhs.decimal128.w[0] && lhs.decimal128.w[1] == rhs.decimal128.w[1];
        case RLM_TYPE_OBJECT_ID:
            return memcmp(lhs.object_id.bytes, rhs.object_id.bytes, 12) == 0;
        case RLM_TYPE_LINK:
            return lhs.link.target_table == rhs.link.target_table && lhs.link.target == rhs.link.target;
        case RLM_TYPE_UUID:
            return memcmp(lhs.uuid.bytes, rhs.uuid.bytes, 16) == 0;
    }
    REALM_TERMINATE("");
}

struct RealmReleaseDeleter {
    void operator()(void* ptr)
    {
        realm_release(ptr);
    }
};

template <class T>
using CPtr = std::unique_ptr<T, RealmReleaseDeleter>;

template <class T>
CPtr<T> cptr(T* ptr)
{
    return CPtr<T>{ptr};
}

template <class T>
CPtr<T> cptr_checked(T* ptr)
{
    return cptr(checked(ptr));
}

template <class T>
CPtr<T> clone_cptr(const CPtr<T>& ptr)
{
    void* clone = realm_clone(ptr.get());
    return CPtr<T>{static_cast<T*>(clone)};
}

template <class T>
CPtr<T> clone_cptr(const T* ptr)
{
    void* clone = realm_clone(ptr);
    return CPtr<T>{static_cast<T*>(clone)};
}

#define CHECK_ERR(err)                                                                                               \
    do {                                                                                                             \
        realm_error_t _err;                                                                                          \
        _err.message = "";                                                                                           \
        _err.error = RLM_ERR_NONE;                                                                                   \
        CHECK(realm_get_last_error(&_err));                                                                          \
        if (_err.error != err) {                                                                                     \
            CHECK(_err.error == err);                                                                                \
            CHECK(std::string{_err.message} == "");                                                                  \
        }                                                                                                            \
        else {                                                                                                       \
            realm_clear_last_error();                                                                                \
        }                                                                                                            \
    } while (false);

TEST_CASE("C API (C)") {
    const char* file_name = "c_api_test_c.realm";

    // FIXME: Use a better test file guard.
    if (realm::util::File::exists(file_name)) {
        CHECK(realm::util::File::try_remove(file_name));
    }

    CHECK(realm_c_api_tests(file_name) == 0);
}

TEST_CASE("C API (non-database)") {
    SECTION("realm_get_library_version_numbers()") {
        int major, minor, patch;
        const char* extra;
        realm_get_library_version_numbers(&major, &minor, &patch, &extra);

        CHECK(major == REALM_VERSION_MAJOR);
        CHECK(minor == REALM_VERSION_MINOR);
        CHECK(patch == REALM_VERSION_PATCH);
        CHECK(std::string{extra} == REALM_VERSION_EXTRA);
    }

    SECTION("realm_get_library_version()") {
        const char* version = realm_get_library_version();
        CHECK(std::string{version} == REALM_VERSION_STRING);
    }

    SECTION("realm_release(NULL)") {
        // Just check that it doesn't crash.
        realm_release(nullptr);
    }

    SECTION("realm_get_last_error()") {
        CHECK(!realm_get_last_error(nullptr));
        CHECK(!realm_clear_last_error());

        auto synthetic = []() {
            throw std::runtime_error("Synthetic error");
        };
        CHECK(!realm_wrap_exceptions(synthetic));

        realm_error_t err;
        CHECK(realm_get_last_error(&err));
        CHECK(err.error == RLM_ERR_OTHER_EXCEPTION);
        CHECK(std::string{err.message} == "Synthetic error");
        realm_clear_last_error();
    }

    SECTION("realm_get_last_error_as_async_error()") {
        CHECK(!realm_get_last_error_as_async_error());

        auto synthetic = []() {
            throw std::runtime_error("Synthetic error");
        };
        CHECK(!realm_wrap_exceptions(synthetic));

        realm_async_error_t* async_err = realm_get_last_error_as_async_error();
        CHECK(async_err);

        realm_error_t err;
        realm_get_async_error(async_err, &err);

        CHECK(err.error == RLM_ERR_OTHER_EXCEPTION);
        CHECK(std::string{err.message} == "Synthetic error");

        SECTION("realm_clone()") {
            auto cloned = clone_cptr(async_err);
            CHECK(realm_equals(async_err, cloned.get()));
            realm_error_t err2;
            realm_get_async_error(cloned.get(), &err2);
            CHECK(err2.error == RLM_ERR_OTHER_EXCEPTION);
            CHECK(std::string{err2.message} == "Synthetic error");
        }

        SECTION("realm_equals()") {
            auto config = cptr(realm_config_new());
            CHECK(!realm_equals(config.get(), async_err));
            CHECK(!realm_equals(async_err, config.get()));
        }

        realm_release(async_err);
        realm_clear_last_error();
    }

    SECTION("realm_clear_last_error()") {
        auto synthetic = []() {
            throw std::runtime_error("Synthetic error");
        };
        CHECK(!realm_wrap_exceptions(synthetic));

        CHECK(realm_clear_last_error());
        CHECK(!realm_get_last_error(nullptr));
    }

    SECTION("realm_clone() error") {
        // realm_config_t is not clonable
        auto config = cptr(realm_config_new());
        CHECK(!realm_clone(config.get()));
        CHECK_ERR(RLM_ERR_NOT_CLONABLE);
    }

    SECTION("realm_create_thread_safe_reference() error") {
        // realm_config_t is not sendable between threads
        auto config = cptr(realm_config_new());
        CHECK(!realm_create_thread_safe_reference(config.get()));
        CHECK_ERR(RLM_ERR_LOGIC);
    }

    SECTION("realm_is_frozen() false by default") {
        // realm_config_t cannot be frozen, so is never frozen
        auto config = cptr(realm_config_new());
        CHECK(!realm_is_frozen(config.get()));
    }

    SECTION("realm_equals() with different object types returns false") {
        auto config = cptr(realm_config_new());
        auto schema = cptr(realm_schema_new(nullptr, 0, nullptr));
        CHECK(!realm_equals(config.get(), schema.get()));
        CHECK(!realm_equals(schema.get(), config.get()));
    }

    SECTION("realm_config_t") {
        auto config = cptr(realm_config_new());

        SECTION("realm_config_set_path()") {
            realm_config_set_path(config.get(), "hello");
            CHECK(std::string{realm_config_get_path(config.get())} == "hello");
        }

        SECTION("realm_config_set_encryption_key()") {
            uint8_t key[64] = {0};
            std::iota(std::begin(key), std::end(key), 0);
            CHECK(realm_config_set_encryption_key(config.get(), key, 64));

            uint8_t buffer[64];
            size_t len = realm_config_get_encryption_key(config.get(), buffer);
            CHECK(len == 64);

            CHECK(!realm_config_set_encryption_key(config.get(), key, 63));
            CHECK_ERR(RLM_ERR_LOGIC);
        }

        SECTION("realm_config_set_schema()") {
            auto empty_schema = cptr(realm_schema_new(nullptr, 0, nullptr));
            realm_config_set_schema(config.get(), empty_schema.get());
            auto schema = cptr(realm_config_get_schema(config.get()));
            CHECK(schema);
            CHECK(realm_equals(empty_schema.get(), schema.get()));
            realm_config_set_schema(config.get(), nullptr);
            CHECK(realm_config_get_schema(config.get()) == nullptr);
        }

        SECTION("realm_config_set_schema_version()") {
            realm_config_set_schema_version(config.get(), 26);
            CHECK(realm_config_get_schema_version(config.get()) == 26);
        }

        SECTION("realm_config_set_schema_mode()") {
            auto check_mode = [&](realm_schema_mode_e mode) {
                realm_config_set_schema_mode(config.get(), mode);
                CHECK(realm_config_get_schema_mode(config.get()) == mode);
            };
            check_mode(RLM_SCHEMA_MODE_AUTOMATIC);
            check_mode(RLM_SCHEMA_MODE_IMMUTABLE);
            check_mode(RLM_SCHEMA_MODE_READ_ONLY);
            check_mode(RLM_SCHEMA_MODE_SOFT_RESET_FILE);
            check_mode(RLM_SCHEMA_MODE_HARD_RESET_FILE);
            check_mode(RLM_SCHEMA_MODE_ADDITIVE_EXPLICIT);
            check_mode(RLM_SCHEMA_MODE_ADDITIVE_DISCOVERED);
            check_mode(RLM_SCHEMA_MODE_MANUAL);
        }

        SECTION("realm_config_set_disable_format_upgrade()") {
            realm_config_set_disable_format_upgrade(config.get(), true);
            CHECK(realm_config_get_disable_format_upgrade(config.get()) == true);
            realm_config_set_disable_format_upgrade(config.get(), false);
            CHECK(realm_config_get_disable_format_upgrade(config.get()) == false);
        }

        SECTION("realm_config_set_automatic_change_notifications()") {
            realm_config_set_automatic_change_notifications(config.get(), true);
            CHECK(realm_config_get_automatic_change_notifications(config.get()) == true);
            realm_config_set_automatic_change_notifications(config.get(), false);
            CHECK(realm_config_get_automatic_change_notifications(config.get()) == false);
        }

        SECTION("realm_config_set_force_sync_history()") {
            realm_config_set_force_sync_history(config.get(), true);
            CHECK(realm_config_get_force_sync_history(config.get()) == true);
            realm_config_set_force_sync_history(config.get(), false);
            CHECK(realm_config_get_force_sync_history(config.get()) == false);
        }

        SECTION("realm_config_set_max_number_of_active_versions()") {
            realm_config_set_max_number_of_active_versions(config.get(), 999);
            CHECK(realm_config_get_max_number_of_active_versions(config.get()) == 999);
        }

        SECTION("realm_config_set_in_memory()") {
            realm_config_set_in_memory(config.get(), true);
            CHECK(realm_config_get_in_memory(config.get()) == true);
        }

        SECTION("realm_config_set_fifo_path()") {
            realm_config_set_fifo_path(config.get(), "test_path.FIFO");
            CHECK(std::string{realm_config_get_fifo_path(config.get())} == "test_path.FIFO");
        }
    }
}

/// Generate realm_property_info_t for all possible property types.
std::vector<realm_property_info_t> all_property_types(const char* link_target)
{
    std::vector<realm_property_info_t> properties;

    static const char* names[] = {
        "int", "bool", "string", "binary", "timestamp", "float", "double", "decimal", "object_id", "uuid",
    };
    static const char* nullable_names[] = {
        "nullable_int",   "nullable_bool",   "nullable_string",  "nullable_binary",    "nullable_timestamp",
        "nullable_float", "nullable_double", "nullable_decimal", "nullable_object_id", "nullable_uuid",
    };
    static const char* list_names[] = {
        "int_list",   "bool_list",   "string_list",  "binary_list",    "timestamp_list",
        "float_list", "double_list", "decimal_list", "object_id_list", "uuid_list",
    };
    static const char* nullable_list_names[] = {
        "nullable_int_list",       "nullable_bool_list",  "nullable_string_list", "nullable_binary_list",
        "nullable_timestamp_list", "nullable_float_list", "nullable_double_list", "nullable_decimal_list",
        "nullable_object_id_list", "nullable_uuid_list",
    };
    static const char* set_names[] = {
        "int_set",   "bool_set",   "string_set",  "binary_set",    "timestamp_set",
        "float_set", "double_set", "decimal_set", "object_id_set", "uuid_set",
    };
    static const char* nullable_set_names[] = {
        "nullable_int_set",       "nullable_bool_set",  "nullable_string_set", "nullable_binary_set",
        "nullable_timestamp_set", "nullable_float_set", "nullable_double_set", "nullable_decimal_set",
        "nullable_object_id_set", "nullable_uuid_set",
    };
    static const char* dict_names[] = {
        "int_dict",   "bool_dict",   "string_dict",  "binary_dict",    "timestamp_dict",
        "float_dict", "double_dict", "decimal_dict", "object_id_dict", "uuid_dict",
    };
    static const char* nullable_dict_names[] = {
        "nullable_int_dict",       "nullable_bool_dict",  "nullable_string_dict", "nullable_binary_dict",
        "nullable_timestamp_dict", "nullable_float_dict", "nullable_double_dict", "nullable_decimal_dict",
        "nullable_object_id_dict", "nullable_uuid_dict",
    };
    static const realm_property_type_e types[] = {
        RLM_PROPERTY_TYPE_INT,       RLM_PROPERTY_TYPE_BOOL,  RLM_PROPERTY_TYPE_STRING, RLM_PROPERTY_TYPE_BINARY,
        RLM_PROPERTY_TYPE_TIMESTAMP, RLM_PROPERTY_TYPE_FLOAT, RLM_PROPERTY_TYPE_DOUBLE, RLM_PROPERTY_TYPE_DECIMAL128,
        RLM_PROPERTY_TYPE_OBJECT_ID, RLM_PROPERTY_TYPE_UUID,
    };

    size_t num_names = std::distance(std::begin(names), std::end(names));
    size_t num_nullable_names = std::distance(std::begin(nullable_names), std::end(nullable_names));
    size_t num_list_names = std::distance(std::begin(list_names), std::end(list_names));
    size_t num_nullable_list_names = std::distance(std::begin(nullable_list_names), std::end(nullable_list_names));
    size_t num_set_names = std::distance(std::begin(set_names), std::end(set_names));
    size_t num_nullable_set_names = std::distance(std::begin(nullable_set_names), std::end(nullable_set_names));
    size_t num_dict_names = std::distance(std::begin(dict_names), std::end(dict_names));
    size_t num_nullable_dict_names = std::distance(std::begin(nullable_dict_names), std::end(nullable_dict_names));
    size_t num_types = std::distance(std::begin(types), std::end(types));

    REALM_ASSERT(num_names == num_types);
    REALM_ASSERT(num_nullable_names == num_types);
    REALM_ASSERT(num_list_names == num_types);
    REALM_ASSERT(num_nullable_list_names == num_types);
    REALM_ASSERT(num_set_names == num_types);
    REALM_ASSERT(num_nullable_set_names == num_types);
    REALM_ASSERT(num_dict_names == num_types);
    REALM_ASSERT(num_nullable_dict_names == num_types);

    for (size_t i = 0; i < num_names; ++i) {
        const char* public_name = i == 0 ? "public_int" : "";
        realm_property_info_t normal{
            names[i],
            public_name,
            types[i],
            RLM_COLLECTION_TYPE_NONE,
            "",
            "",
            RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t nullable{
            nullable_names[i],     "", types[i], RLM_COLLECTION_TYPE_NONE, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NULLABLE,
        };
        realm_property_info_t list{
            list_names[i],       "", types[i], RLM_COLLECTION_TYPE_LIST, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t nullable_list{
            nullable_list_names[i], "", types[i], RLM_COLLECTION_TYPE_LIST, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NULLABLE,
        };
        realm_property_info_t set{
            set_names[i],        "", types[i], RLM_COLLECTION_TYPE_SET, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t nullable_set{
            nullable_set_names[i], "", types[i], RLM_COLLECTION_TYPE_SET, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NULLABLE,
        };
        realm_property_info_t dict{
            dict_names[i],       "", types[i], RLM_COLLECTION_TYPE_DICTIONARY, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t nullable_dict{
            nullable_dict_names[i], "", types[i], RLM_COLLECTION_TYPE_DICTIONARY, "", "", RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NULLABLE,
        };

        properties.push_back(normal);
        properties.push_back(nullable);
        properties.push_back(list);
        properties.push_back(nullable_list);
        properties.push_back(set);
        properties.push_back(nullable_set);
        properties.push_back(dict);
        properties.push_back(nullable_dict);
    }

    realm_property_info_t link{
        "link",      "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_NONE,
        link_target, "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NULLABLE,
    };
    realm_property_info_t link_list{
        "link_list", "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_LIST,
        link_target, "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
    };
    realm_property_info_t link_set{
        "link_set",  "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_SET,
        link_target, "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
    };
    realm_property_info_t link_dict{
        "link_dict", "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_DICTIONARY,
        link_target, "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NULLABLE,
    };

    properties.push_back(link);
    properties.push_back(link_list);
    properties.push_back(link_set);
    properties.push_back(link_dict);

    // realm_property_info_t mixed{
    //     "mixed", "", RLM_PROPERTY_TYPE_MIXED,  RLM_COLLECTION_TYPE_NONE,
    //     "",      "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NULLABLE,
    // };
    // realm_property_info_t mixed_list{
    //     "mixed_list", "", RLM_PROPERTY_TYPE_MIXED,  RLM_COLLECTION_TYPE_LIST,
    //     "",           "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
    // };

    // properties.push_back(mixed);
    // properties.push_back(mixed_list);

    // FIXME: Object Store schema handling does not support TypedLink yet.
    // realm_property_info_t typed_link{
    //     "typed_link", "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_NONE,
    //     "",           "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NULLABLE,
    // };
    // realm_property_info_t typed_link_list{
    //     "typed_link_list",   "", RLM_PROPERTY_TYPE_OBJECT, RLM_COLLECTION_TYPE_LIST, "", "",
    //     RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
    // };

    // properties.push_back(typed_link);
    // properties.push_back(typed_link_list);

    return properties;
}

static CPtr<realm_schema_t> make_schema()
{
    auto foo_properties = all_property_types("Bar");

    const realm_class_info_t classes[2] = {
        {
            "Foo",
            "",                    // primary key
            foo_properties.size(), // properties
            0,                     // computed_properties
            RLM_INVALID_CLASS_KEY,
            RLM_CLASS_NORMAL,
        },
        {
            "Bar",
            "int", // primary key
            3,     // properties
            1,     // computed properties,
            RLM_INVALID_CLASS_KEY,
            RLM_CLASS_NORMAL,
        },
    };

    const realm_property_info_t bar_properties[4] = {
        {
            "int",
            "",
            RLM_PROPERTY_TYPE_INT,
            RLM_COLLECTION_TYPE_NONE,
            "",
            "",
            RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_INDEXED | RLM_PROPERTY_PRIMARY_KEY,
        },
        {
            "strings",
            "",
            RLM_PROPERTY_TYPE_STRING,
            RLM_COLLECTION_TYPE_LIST,
            "",
            "",
            RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL | RLM_PROPERTY_NULLABLE,
        },
        {
            "doubles",
            "",
            RLM_PROPERTY_TYPE_DOUBLE,
            RLM_COLLECTION_TYPE_NONE,
            "",
            "",
            RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        },
        {
            "linking_objects",
            "",
            RLM_PROPERTY_TYPE_LINKING_OBJECTS,
            RLM_COLLECTION_TYPE_LIST,
            "Foo",
            "link_list",
            RLM_INVALID_PROPERTY_KEY,
            RLM_PROPERTY_NORMAL,
        },
    };

    const realm_property_info_t* class_properties[2] = {foo_properties.data(), bar_properties};

    return cptr(realm_schema_new(classes, 2, class_properties));
}

CPtr<realm_config_t> make_config(const char* filename, bool set_schema = true)
{
    auto config = cptr(realm_config_new());
    realm_config_set_path(config.get(), filename);
    realm_config_set_schema_mode(config.get(), RLM_SCHEMA_MODE_AUTOMATIC);

    if (set_schema) {
        auto schema = make_schema();
        CHECK(checked(schema.get()));
        REQUIRE(checked(realm_schema_validate(schema.get(), RLM_SCHEMA_VALIDATION_BASIC)));
        realm_config_set_schema(config.get(), schema.get());
        realm_config_set_schema_version(config.get(), 0);
    }

    realm_config_set_automatic_change_notifications(config.get(), true);
    realm_config_set_max_number_of_active_versions(config.get(), 1000);

    return config;
}

struct ConfigUserdata {
    size_t num_initializations = 0;
    size_t num_migrations = 0;
    size_t num_compact_on_launch = 0;
};

bool initialize_data(void* userdata_p, realm_t*)
{
    auto userdata = static_cast<ConfigUserdata*>(userdata_p);
    ++userdata->num_initializations;
    return true;
}

bool migrate_schema(void* userdata_p, realm_t* old, realm_t* new_, const realm_schema_t*)
{
    auto userdata = static_cast<ConfigUserdata*>(userdata_p);
    static_cast<void>(old);
    static_cast<void>(new_);
    ++userdata->num_migrations;
    return true;
}

bool should_compact_on_launch(void* userdata_p, uint64_t, uint64_t)
{
    auto userdata = static_cast<ConfigUserdata*>(userdata_p);
    ++userdata->num_compact_on_launch;
    return false;
}

TEST_CASE("C API") {
    TestFile test_file;

    SECTION("schema in config") {
        TestFile test_file_2;

        auto schema = make_schema();
        CHECK(checked(schema.get()));
        REQUIRE(checked(realm_schema_validate(schema.get(), RLM_SCHEMA_VALIDATION_BASIC)));
        auto config = cptr(realm_config_new());
        realm_config_set_path(config.get(), test_file_2.path.c_str());
        realm_config_set_schema_mode(config.get(), RLM_SCHEMA_MODE_AUTOMATIC);
        realm_config_set_schema_version(config.get(), 0);
        realm_config_set_schema(config.get(), schema.get());

        SECTION("data initialization callback") {
            ConfigUserdata userdata;
            realm_config_set_data_initialization_function(config.get(), initialize_data, &userdata);
            auto realm = cptr_checked(realm_open(config.get()));
            CHECK(userdata.num_initializations == 1);
        }

        SECTION("data initialization callback error") {
            ConfigUserdata userdata;
            realm_config_set_data_initialization_function(
                config.get(),
                [](void*, realm_t*) {
                    return false;
                },
                &userdata);
            CHECK(!realm_open(config.get()));
            CHECK_ERR(RLM_ERR_CALLBACK);
        }

        SECTION("migration callback") {
            ConfigUserdata userdata;
            realm_config_set_migration_function(config.get(), migrate_schema, &userdata);
            auto realm = cptr_checked(realm_open(config.get()));
            CHECK(userdata.num_migrations == 0);
            realm.reset();

            auto config2 = cptr(realm_config_new());
            auto empty_schema = cptr(realm_schema_new(nullptr, 0, nullptr));
            realm_config_set_path(config2.get(), test_file_2.path.c_str());
            realm_config_set_schema_mode(config2.get(), RLM_SCHEMA_MODE_AUTOMATIC);
            realm_config_set_schema_version(config2.get(), 999);
            realm_config_set_schema(config2.get(), empty_schema.get());
            realm_config_set_migration_function(config2.get(), migrate_schema, &userdata);
            auto realm2 = cptr_checked(realm_open(config2.get()));
            CHECK(userdata.num_migrations == 1);
        }

        SECTION("migration callback error") {
            ConfigUserdata userdata;
            auto realm = cptr_checked(realm_open(config.get()));
            realm.reset();

            auto config2 = cptr(realm_config_new());
            auto empty_schema = cptr(realm_schema_new(nullptr, 0, nullptr));
            realm_config_set_path(config2.get(), test_file_2.path.c_str());
            realm_config_set_schema_mode(config2.get(), RLM_SCHEMA_MODE_AUTOMATIC);
            realm_config_set_schema_version(config2.get(), 999);
            realm_config_set_schema(config2.get(), empty_schema.get());

            realm_config_set_migration_function(
                config2.get(),
                [](void*, realm_t*, realm_t*, const realm_schema_t*) {
                    return false;
                },
                &userdata);
            CHECK(!realm_open(config2.get()));
            CHECK_ERR(RLM_ERR_CALLBACK);
        }

        SECTION("should compact on launch callback") {
            ConfigUserdata userdata;
            realm_config_set_should_compact_on_launch_function(config.get(), should_compact_on_launch, &userdata);
            auto realm = cptr_checked(realm_open(config.get()));
            CHECK(userdata.num_compact_on_launch == 1);
        }
    }

    realm_t* realm;
    {
        auto schema = make_schema();
        CHECK(checked(schema.get()));
        REQUIRE(checked(realm_schema_validate(schema.get(), RLM_SCHEMA_VALIDATION_BASIC)));

        auto config = make_config(test_file.path.c_str(), false);

        realm = realm_open(config.get());
        REQUIRE(checked(realm));
        REQUIRE(checked(realm_update_schema(realm, schema.get())));

        CHECK(!realm_equals(realm, nullptr));

        auto realm2 = cptr(realm_open(config.get()));
        CHECK(checked(realm2.get()));
        CHECK(!realm_equals(realm, realm2.get()));
        CHECK(realm_equals(realm, realm));
    }

    CHECK(realm_get_num_classes(realm) == 2);

    SECTION("cached realm") {
        auto config2 = make_config(test_file.path.c_str(), false);
        realm_config_set_cached(config2.get(), true);
        REQUIRE(realm_config_get_cached(config2.get()));
        auto realm2 = cptr(realm_open(config2.get()));
        CHECK(!realm_equals(realm, realm2.get()));
        auto realm3 = cptr(realm_open(config2.get()));
        REQUIRE(realm_equals(realm3.get(), realm2.get()));
    }

    SECTION("native ptr conversion") {
        realm::SharedRealm native;
        _realm_get_native_ptr(realm, &native, sizeof(native));
        auto path = native->config().path;
        CHECK(path == test_file.path);

        auto realm2 = cptr_checked(_realm_from_native_ptr(&native, sizeof(native)));
        CHECK(realm_equals(realm2.get(), realm));
    }

    SECTION("realm changed notification") {
        bool realm_changed_callback_called = false;
        auto token = cptr(realm_add_realm_changed_callback(
            realm,
            [](void* userdata) {
                *reinterpret_cast<bool*>(userdata) = true;
            },
            &realm_changed_callback_called, [](void*) {}));

        realm_begin_write(realm);
        realm_commit(realm);
        CHECK(realm_changed_callback_called);
    }

    SECTION("schema is set after opening") {
        const realm_class_info_t baz = {
            "baz",
            "", // primary key
            1,  // properties
            0,  // computed_properties
            RLM_INVALID_CLASS_KEY,
            RLM_CLASS_NORMAL,
        };

        auto int_property = realm_property_info_t{
            "int", "", RLM_PROPERTY_TYPE_INT,    RLM_COLLECTION_TYPE_NONE,
            "",    "", RLM_INVALID_PROPERTY_KEY, RLM_PROPERTY_NORMAL,
        };
        realm_property_info_t* baz_properties = &int_property;

        // get class count
        size_t num_classes = realm_get_num_classes(realm);
        realm_class_key_t* out_keys = (realm_class_key_t*)malloc(sizeof(realm_class_key_t) * num_classes);
        // get class keys
        realm_get_class_keys(realm, out_keys, num_classes, nullptr);
        realm_class_info_t* classes = (realm_class_info_t*)malloc(sizeof(realm_class_info_t) * (num_classes + 1));
        const realm_property_info_t** properties =
            (const realm_property_info_t**)malloc(sizeof(realm_property_info_t*) * (num_classes + 1));
        // iterating through each class, "recreate" the old schema
        for (size_t i = 0; i < num_classes; i++) {
            realm_get_class(realm, out_keys[i], &classes[i]);
            size_t out_n;
            realm_get_class_properties(realm, out_keys[i], nullptr, 0, &out_n);
            realm_property_info_t* out_props = (realm_property_info_t*)malloc(sizeof(realm_property_info_t) * out_n);
            realm_get_class_properties(realm, out_keys[i], out_props, out_n, nullptr);
            properties[i] = out_props;
        }
        // add the new class and its properties to the arrays
        classes[num_classes] = baz;

        properties[num_classes] = baz_properties;

        // create a new schema and update the realm
        auto new_schema = realm_schema_new(classes, num_classes + 1, properties);

        // check that the schema changed callback fires with the new schema
        struct Context {
            realm_schema_t* expected_schema;
            bool result;
        } context = {new_schema, false};
        auto token = realm_add_schema_changed_callback(
            realm,
            [](void* userdata, auto* new_schema) {
                auto& ctx = *reinterpret_cast<Context*>(userdata);
                ctx.result = realm_equals(new_schema, ctx.expected_schema);
            },
            &context, [](void*) {});

        CHECK(checked(realm_update_schema(realm, new_schema)));
        CHECK(context.result);
        auto new_num_classes = realm_get_num_classes(realm);
        CHECK(new_num_classes == (num_classes + 1));

        bool found;
        realm_class_info_t baz_info;
        CHECK(checked(realm_find_class(realm, "baz", &found, &baz_info)));
        CHECK(found);
        realm_property_info_t baz_int_property;
        CHECK(checked(realm_find_property(realm, baz_info.key, "int", &found, &baz_int_property)));
        CHECK(found);

        free(out_keys);
        free(classes);
        for (size_t i = 0; i < num_classes; i++) {
            free((realm_property_info_t*)properties[i]);
        }
        free(properties);
        realm_release(new_schema);
        realm_release(token);
    }

    SECTION("schema validates") {
        auto schema = realm_get_schema(realm);
        CHECK(checked(schema));
        CHECK(checked(realm_schema_validate(schema, realm_schema_validation_mode::RLM_SCHEMA_VALIDATION_BASIC)));

        auto schema2 = realm_get_schema(realm);
        CHECK(checked(schema2));
        CHECK(realm_equals(schema, schema2));
        realm_release(schema2);
        realm_release(schema);
    }

    SECTION("clone schema") {
        auto schema = cptr(realm_get_schema(realm));
        auto schema2 = clone_cptr(schema);
        CHECK(schema.get() != schema2.get());
        CHECK(realm_equals(schema.get(), schema2.get()));
    }

    auto write = [&](auto&& f) {
        checked(realm_begin_write(realm));
        f();
        checked(realm_commit(realm));
        checked(realm_refresh(realm));
    };

    bool found = false;

    realm_class_info_t class_foo, class_bar;
    CHECK(checked(realm_find_class(realm, "Foo", &found, &class_foo)));
    REQUIRE(found);
    CHECK(checked(realm_find_class(realm, "Bar", &found, &class_bar)));
    REQUIRE(found);

    std::map<std::string, realm_property_key_t> foo_properties;
    for (const auto& p : all_property_types("Bar")) {
        realm_property_info_t info;
        bool found = false;
        REQUIRE(realm_find_property(realm, class_foo.key, p.name, &found, &info));
        REQUIRE(found);
        CHECK(p.key == RLM_INVALID_PROPERTY_KEY);
        CHECK(info.key != RLM_INVALID_PROPERTY_KEY);
        CHECK(info.type == p.type);
        CHECK(std::string{info.public_name} == p.public_name);
        CHECK(info.collection_type == p.collection_type);
        CHECK(std::string{info.link_target} == p.link_target);
        CHECK(std::string{info.link_origin_property_name} == p.link_origin_property_name);
        foo_properties[info.name] = info.key;
    }

    std::map<std::string, realm_property_key_t> bar_properties;
    {
        realm_property_info_t info;
        bool found = false;
        REQUIRE(checked(realm_find_property(realm, class_bar.key, "int", &found, &info)));
        REQUIRE(found);
        bar_properties["int"] = info.key;

        REQUIRE(checked(realm_find_property(realm, class_bar.key, "strings", &found, &info)));
        REQUIRE(found);
        bar_properties["strings"] = info.key;

        REQUIRE(checked(realm_find_property(realm, class_bar.key, "doubles", &found, &info)));
        REQUIRE(found);
        bar_properties["doubles"] = info.key;

        REQUIRE(checked(realm_find_property(realm, class_bar.key, "linking_objects", &found, &info)));
        REQUIRE(found);
        bar_properties["linking_objects"] = info.key;
    }

    realm_property_key_t foo_int_key = foo_properties["int"];
    realm_property_key_t foo_str_key = foo_properties["string"];
    realm_property_key_t foo_links_key = foo_properties["link_list"];
    realm_property_key_t bar_int_key = bar_properties["int"];
    realm_property_key_t bar_strings_key = bar_properties["strings"];
    realm_property_key_t bar_doubles_key = bar_properties["doubles"];

    SECTION("realm_freeze()") {
        auto realm2 = cptr_checked(realm_freeze(realm));
        CHECK(!realm_is_frozen(realm));
        CHECK(realm_is_frozen(realm2.get()));
    }

    SECTION("realm_find_class() errors") {
        bool found = true;
        CHECK(realm_find_class(realm, "does not exist", &found, nullptr));
        CHECK(!found);
    }

    SECTION("realm_compact()") {
        bool did_compact = false;
        CHECK(checked(realm_compact(realm, &did_compact)));
        CHECK(did_compact);
    }

    SECTION("realm_get_class_keys()") {
        realm_class_key_t keys[2];
        size_t found = 0;
        CHECK(checked(realm_get_class_keys(realm, keys, 2, &found)));
        CHECK(found == 2);
        CHECK(checked(realm_get_class_keys(realm, keys, 1, &found)));
        CHECK(found == 1);
    }

    SECTION("realm_find_property() errors") {
        realm_property_info_t dummy;
        CHECK(!realm_find_property(realm, 123123123, "Foo", &found, &dummy));
        CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);
        CHECK(!realm_find_property(realm, 123123123, "Foo", &found, nullptr));
        CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);

        bool found;
        CHECK(checked(realm_find_property(realm, class_foo.key, "int", nullptr, nullptr)));
        CHECK(checked(realm_find_property(realm, class_foo.key, "int", &found, nullptr)));
        CHECK(found);

        found = true;
        CHECK(checked(realm_find_property(realm, class_foo.key, "i don't exist", &found, nullptr)));
        CHECK(!found);
    }

    SECTION("realm_find_property_by_public_name()") {
        realm_property_info_t property;
        bool found = false;
        CHECK(checked(realm_find_property_by_public_name(realm, class_foo.key, "public_int", &found, &property)));
        CHECK(found);
        CHECK(property.key == foo_int_key);

        found = false;
        CHECK(checked(realm_find_property_by_public_name(realm, class_foo.key, "string", &found, &property)));
        CHECK(found);
        CHECK(property.key == foo_properties["string"]);

        CHECK(checked(realm_find_property_by_public_name(realm, class_foo.key, "i don't exist", &found, &property)));
        CHECK(!found);
    }

    SECTION("realm_get_property_keys()") {
        realm_property_key_t properties[3];
        size_t num_found = 0;
        CHECK(checked(realm_get_property_keys(realm, class_foo.key, properties, 3, &num_found)));
        CHECK(num_found == 3);
        CHECK(properties[0] == foo_properties["int"]);

        num_found = 0;
        CHECK(checked(realm_get_property_keys(realm, class_bar.key, properties, 3, &num_found)));
        CHECK(num_found == 3);
        CHECK(properties[2] == bar_properties["doubles"]);

        num_found = 0;
        CHECK(checked(realm_get_property_keys(realm, class_bar.key, properties, 1, &num_found)));
        CHECK(num_found == 1);
        CHECK(properties[0] == bar_properties["int"]);

        num_found = 0;
        CHECK(checked(realm_get_property_keys(realm, class_foo.key, nullptr, 0, &num_found)));
        CHECK(num_found == class_foo.num_properties + class_foo.num_computed_properties);

        std::vector<realm_property_key_t> ps;
        ps.resize(1000);
        CHECK(checked(realm_get_property_keys(realm, class_foo.key, ps.data(), ps.size(), &num_found)));
        CHECK(num_found == class_foo.num_properties + class_foo.num_computed_properties);

        CHECK(checked(realm_get_property_keys(realm, class_bar.key, ps.data(), ps.size(), &num_found)));
        CHECK(num_found == 4);
    }

    SECTION("realm_get_property()") {
        realm_property_info_t prop;
        CHECK(checked(realm_get_property(realm, class_bar.key, bar_properties["linking_objects"], &prop)));
        CHECK(prop.key == bar_properties["linking_objects"]);
        CHECK(std::string{prop.name} == "linking_objects");

        CHECK(!realm_get_property(realm, class_bar.key, 123123123, &prop));
        CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
    }

    SECTION("realm_object_create() errors") {
        SECTION("invalid table") {
            write([&]() {
                auto p = realm_object_create(realm, 123123123);
                CHECK(!p);
                CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);
            });
        }

        SECTION("missing primary key") {
            write([&]() {
                auto p = realm_object_create(realm, class_bar.key);
                CHECK(!p);
                CHECK_ERR(RLM_ERR_MISSING_PRIMARY_KEY);
            });
        }

        SECTION("wrong primary key type") {
            write([&]() {
                auto p = realm_object_create_with_primary_key(realm, class_bar.key, rlm_str_val("Hello"));
                CHECK(!p);
                CHECK_ERR(RLM_ERR_WRONG_PRIMARY_KEY_TYPE);
            });

            write([&]() {
                auto p = realm_object_create_with_primary_key(realm, class_bar.key, rlm_null());
                CHECK(!p);
                CHECK_ERR(RLM_ERR_PROPERTY_NOT_NULLABLE);
            });
        }

        SECTION("class does not have a primary key") {
            write([&]() {
                CHECK(!realm_object_create_with_primary_key(realm, class_foo.key, rlm_int_val(123)));
                CHECK_ERR(RLM_ERR_UNEXPECTED_PRIMARY_KEY);
            });
        }

        SECTION("duplicate primary key") {
            write([&]() {
                cptr_checked(realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(123)));
                auto p = realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(123));
                CHECK(!p);
                CHECK_ERR(RLM_ERR_DUPLICATE_PRIMARY_KEY_VALUE);
            });
        }

        SECTION("not in a transaction") {
            CHECK(!realm_object_create(realm, class_foo.key));
            CHECK_ERR(RLM_ERR_NOT_IN_A_TRANSACTION);
        }
    }


    SECTION("objects") {
        CPtr<realm_object_t> obj1;
        CPtr<realm_object_t> obj2;
        auto int_val1 = rlm_int_val(123);
        auto int_val2 = rlm_int_val(456);
        write([&]() {
            obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
            CHECK(obj1);
            CHECK(checked(realm_set_value(obj1.get(), foo_int_key, int_val1, false)));
            CHECK(checked(realm_set_value(obj1.get(), foo_str_key, rlm_str_val("Hello, World!"), false)));
            obj2 = cptr_checked(realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(1)));
            CHECK(obj2);
            CPtr<realm_object_t> obj3 = cptr_checked(realm_object_create(realm, class_foo.key));
            CHECK(obj3);
            CHECK(checked(realm_set_value(obj3.get(), foo_int_key, int_val2, false)));
            CPtr<realm_object_t> obj4 = cptr_checked(realm_object_create(realm, class_foo.key));
            CHECK(obj3);
            CHECK(checked(realm_set_value(obj4.get(), foo_int_key, int_val1, false)));
        });

        size_t foo_count, bar_count;
        CHECK(checked(realm_get_num_objects(realm, class_foo.key, &foo_count)));
        CHECK(checked(realm_get_num_objects(realm, class_bar.key, &bar_count)));
        REQUIRE(foo_count == 3);
        REQUIRE(bar_count == 1);

        SECTION("realm_clone()") {
            auto obj1a = clone_cptr(obj1);
            CHECK(realm_equals(obj1a.get(), obj1.get()));
        }

        SECTION("native pointer mapping") {
            auto object = *static_cast<const realm::Object*>(_realm_object_get_native_ptr(obj1.get()));
            auto obj = object.obj();
            CHECK(obj.get<int64_t>(realm::ColKey(foo_int_key)) == int_val1.integer);

            auto obj1a = cptr_checked(_realm_object_from_native_copy(&object, sizeof(object)));
            CHECK(realm_equals(obj1.get(), obj1a.get()));
            auto obj1b = cptr_checked(_realm_object_from_native_move(&object, sizeof(object)));
            CHECK(realm_equals(obj1.get(), obj1b.get()));
        }

        SECTION("realm_get_num_objects()") {
            size_t num_foos, num_bars;
            CHECK(checked(realm_get_num_objects(realm, class_foo.key, &num_foos)));
            CHECK(checked(realm_get_num_objects(realm, class_bar.key, &num_bars)));
            CHECK(num_foos == 3);
            CHECK(num_bars == 1);

            CHECK(checked(realm_get_num_objects(realm, class_bar.key, nullptr)));
            CHECK(!realm_get_num_objects(realm, 123123123, nullptr));
            CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);
        }

        SECTION("realm_get_object()") {
            realm_object_key_t obj1_key = realm_object_get_key(obj1.get());
            auto obj1a = cptr_checked(realm_get_object(realm, class_foo.key, obj1_key));
            CHECK(obj1a);
            CHECK(realm_equals(obj1a.get(), obj1.get()));

            realm_object_key_t invalid_key = 123123123;
            CHECK(!realm_get_object(realm, class_foo.key, invalid_key));
            CHECK_ERR(RLM_ERR_NO_SUCH_OBJECT);

            realm_class_key_t invalid_class_key = 123123123;
            CHECK(!realm_get_object(realm, invalid_class_key, obj1_key));
            CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);
        }

        SECTION("create object with primary key that already exists") {
            bool did_create;
            auto obj2a = cptr_checked(
                realm_object_get_or_create_with_primary_key(realm, class_bar.key, rlm_int_val(1), &did_create));
            CHECK(!did_create);
            CHECK(realm_equals(obj2a.get(), obj2.get()));
        }

        SECTION("realm_get_value()") {
            realm_value_t value;
            CHECK(checked(realm_get_value(obj1.get(), foo_int_key, &value)));
            CHECK(value.type == RLM_TYPE_INT);
            CHECK(value.integer == 123);

            CHECK(checked(realm_get_value(obj1.get(), foo_str_key, &value)));
            CHECK(value.type == RLM_TYPE_STRING);
            CHECK(strncmp(value.string.data, "Hello, World!", value.string.size) == 0);

            CHECK(checked(realm_get_value(obj1.get(), foo_int_key, nullptr)));

            CHECK(!realm_get_value(obj1.get(), 123123123, &value));
            CHECK_ERR(RLM_ERR_INVALID_PROPERTY);

            CHECK(!realm_get_value(obj1.get(), 123123123, nullptr));
            CHECK_ERR(RLM_ERR_INVALID_PROPERTY);

            // Cannot use realm_get_value() to get a list.
            CHECK(!realm_get_value(obj1.get(), foo_links_key, &value));
            CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);

            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            CHECK(!realm_get_value(obj1.get(), foo_int_key, &value));
            CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);
        }

        SECTION("realm_get_values()") {
            realm_value_t values[3];

            realm_property_key_t keys1[3] = {foo_int_key, foo_str_key, foo_int_key};
            CHECK(checked(realm_get_values(obj1.get(), 3, keys1, values)));

            CHECK(values[0].type == RLM_TYPE_INT);
            CHECK(values[1].type == RLM_TYPE_STRING);
            CHECK(values[2].type == RLM_TYPE_INT);

            CHECK(values[0].integer == 123);
            CHECK(strncmp(values[1].string.data, "Hello, World!", values[1].string.size) == 0);
            CHECK(values[2].integer == 123);

            realm_property_key_t keys2[3] = {foo_int_key, 123123123, foo_str_key};
            CHECK(!realm_get_values(obj1.get(), 3, keys2, values));
            CHECK_ERR(RLM_ERR_INVALID_PROPERTY);

            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            CHECK(!realm_get_values(obj1.get(), 3, keys1, values));
            CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);
        }

        SECTION("realm_set_value() errors") {
            CHECK(!realm_set_value(obj1.get(), foo_int_key, rlm_int_val(456), false));
            CHECK_ERR(RLM_ERR_NOT_IN_A_TRANSACTION);

            write([&]() {
                CHECK(!realm_set_value(obj1.get(), foo_int_key, rlm_null(), false));
                CHECK_ERR(RLM_ERR_PROPERTY_NOT_NULLABLE);

                CHECK(!realm_set_value(obj1.get(), foo_int_key, rlm_str_val("a"), false));
                CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);

                CHECK(!realm_set_value(obj1.get(), 123123123, rlm_int_val(123), false));
                CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
            });
        }

        SECTION("realm_set_values() errors") {
            realm_value_t int456 = rlm_int_val(456);
            CHECK(!realm_set_values(obj1.get(), 1, &foo_int_key, &int456, false));
            CHECK_ERR(RLM_ERR_NOT_IN_A_TRANSACTION);

            write([&]() {
                realm_value_t value;
                realm_property_key_t keys1[3] = {foo_int_key, foo_str_key, foo_int_key};
                realm_property_key_t keys2[3] = {foo_int_key, 123123123, foo_str_key};

                // No error; check that the last value wins when there are
                // duplicate keys.
                realm_value_t values1[3] = {rlm_int_val(234), rlm_str_val("aaa"), rlm_int_val(345)};
                CHECK(checked(realm_set_values(obj1.get(), 3, keys1, values1, false)));

                realm_get_value(obj1.get(), foo_int_key, &value);
                CHECK(value.type == RLM_TYPE_INT);
                CHECK(value.integer == 345);
                realm_get_value(obj1.get(), foo_str_key, &value);
                CHECK(value.type == RLM_TYPE_STRING);
                CHECK(strncmp("aaa", value.string.data, value.string.size) == 0);

                // Type mismatch error.
                realm_value_t values2[3] = {rlm_int_val(111), rlm_str_val("bbb"), rlm_str_val("ccc")};
                CHECK(!realm_set_values(obj1.get(), 3, keys1, values2, false));
                CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);
                // Properties should remain unchanged.
                realm_get_value(obj1.get(), foo_int_key, &value);
                CHECK(value.type == RLM_TYPE_INT);
                CHECK(value.integer == 345);
                realm_get_value(obj1.get(), foo_str_key, &value);
                CHECK(value.type == RLM_TYPE_STRING);

                // Invalid property key error.
                CHECK(!realm_set_values(obj1.get(), 3, keys2, values2, false));
                CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
                // Properties should remain unchanged.
                realm_get_value(obj1.get(), foo_int_key, &value);
                CHECK(value.type == RLM_TYPE_INT);
                CHECK(value.integer == 345);
                realm_get_value(obj1.get(), foo_str_key, &value);
                CHECK(value.type == RLM_TYPE_STRING);
            });
        }

        SECTION("get/set all property types") {
            realm_value_t null = rlm_null();
            realm_value_t integer = rlm_int_val(987);
            realm_value_t boolean = rlm_bool_val(true);
            realm_value_t string = rlm_str_val("My string");
            const uint8_t binary_data[] = {0, 1, 2, 3, 4, 5, 6, 7};
            realm_value_t binary = rlm_binary_val(binary_data, sizeof(binary_data));
            realm_value_t timestamp = rlm_timestamp_val(1000000, 123123123);
            realm_value_t fnum = rlm_float_val(123.f);
            realm_value_t dnum = rlm_double_val(456.0);
            realm_value_t decimal = rlm_decimal_val(999.0);
            realm_value_t object_id = rlm_object_id_val("abc123abc123");
            realm_value_t uuid = rlm_uuid_val("01234567-9abc-4def-9012-3456789abcde");
            realm_value_t link = rlm_link_val(class_bar.key, realm_object_get_key(obj2.get()));

            write([&]() {
                CHECK(realm_set_value(obj1.get(), foo_properties["int"], integer, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["bool"], boolean, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["string"], string, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["binary"], binary, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["timestamp"], timestamp, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["float"], fnum, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["double"], dnum, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["decimal"], decimal, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["object_id"], object_id, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["uuid"], uuid, false));

                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_int"], integer, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_bool"], boolean, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_string"], string, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_binary"], binary, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_timestamp"], timestamp, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_float"], fnum, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_double"], dnum, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_decimal"], decimal, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_object_id"], object_id, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_uuid"], uuid, false));

                CHECK(realm_set_value(obj1.get(), foo_properties["link"], link, false));
            });

            realm_value_t value;

            CHECK(realm_get_value(obj1.get(), foo_properties["int"], &value));
            CHECK(rlm_val_eq(value, integer));
            CHECK(realm_get_value(obj1.get(), foo_properties["bool"], &value));
            CHECK(rlm_val_eq(value, boolean));
            CHECK(realm_get_value(obj1.get(), foo_properties["string"], &value));
            CHECK(rlm_val_eq(value, string));
            CHECK(realm_get_value(obj1.get(), foo_properties["binary"], &value));
            CHECK(rlm_val_eq(value, binary));
            CHECK(realm_get_value(obj1.get(), foo_properties["timestamp"], &value));
            CHECK(rlm_val_eq(value, timestamp));
            CHECK(realm_get_value(obj1.get(), foo_properties["float"], &value));
            CHECK(rlm_val_eq(value, fnum));
            CHECK(realm_get_value(obj1.get(), foo_properties["double"], &value));
            CHECK(rlm_val_eq(value, dnum));
            CHECK(realm_get_value(obj1.get(), foo_properties["decimal"], &value));
            CHECK(rlm_val_eq(value, decimal));
            CHECK(realm_get_value(obj1.get(), foo_properties["object_id"], &value));
            CHECK(rlm_val_eq(value, object_id));
            CHECK(realm_get_value(obj1.get(), foo_properties["uuid"], &value));
            CHECK(rlm_val_eq(value, uuid));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_int"], &value));
            CHECK(rlm_val_eq(value, integer));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_bool"], &value));
            CHECK(rlm_val_eq(value, boolean));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_string"], &value));
            CHECK(rlm_val_eq(value, string));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_binary"], &value));
            CHECK(rlm_val_eq(value, binary));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_timestamp"], &value));
            CHECK(rlm_val_eq(value, timestamp));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_float"], &value));
            CHECK(rlm_val_eq(value, fnum));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_double"], &value));
            CHECK(rlm_val_eq(value, dnum));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_decimal"], &value));
            CHECK(rlm_val_eq(value, decimal));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_object_id"], &value));
            CHECK(rlm_val_eq(value, object_id));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_uuid"], &value));
            CHECK(rlm_val_eq(value, uuid));
            CHECK(realm_get_value(obj1.get(), foo_properties["link"], &value));
            CHECK(rlm_val_eq(value, link));

            write([&]() {
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_int"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_bool"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_string"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_binary"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_timestamp"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_float"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_double"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_decimal"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_object_id"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["nullable_uuid"], null, false));
                CHECK(realm_set_value(obj1.get(), foo_properties["link"], null, false));
            });

            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_int"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_bool"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_string"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_binary"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_timestamp"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_float"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_double"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_decimal"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_object_id"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["nullable_uuid"], &value));
            CHECK(rlm_val_eq(value, null));
            CHECK(realm_get_value(obj1.get(), foo_properties["link"], &value));
            CHECK(rlm_val_eq(value, null));
        }

        SECTION("find with primary key") {
            bool found = false;

            auto p = cptr_checked(realm_object_find_with_primary_key(realm, class_bar.key, rlm_int_val(1), &found));
            CHECK(found);
            auto p_key = realm_object_get_key(p.get());
            auto obj2_key = realm_object_get_key(obj2.get());
            CHECK(p_key == obj2_key);
            CHECK(realm_equals(p.get(), obj2.get()));

            CHECK(!realm_object_find_with_primary_key(realm, class_bar.key, rlm_int_val(2), &found));
            CHECK(!found);
            CHECK(!realm_object_find_with_primary_key(realm, class_bar.key, rlm_int_val(2), nullptr));

            // Check that finding by type-mismatched values just find nothing.
            CHECK(!realm_object_find_with_primary_key(realm, class_bar.key, rlm_null(), &found));
            CHECK(!found);
            CHECK(!realm_object_find_with_primary_key(realm, class_bar.key, rlm_str_val("a"), &found));
            CHECK(!found);
            CHECK(!realm_object_find_with_primary_key(realm, class_bar.key, rlm_str_val("a"), nullptr));

            // Invalid class key
            CHECK(!realm_object_find_with_primary_key(realm, 123123123, rlm_int_val(1), nullptr));
            CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);
        }

        SECTION("find all") {
            auto r = cptr_checked(realm_object_find_all(realm, class_bar.key));
            size_t count;
            realm_results_count(r.get(), &count);
            CHECK(count == 1);
        }

        SECTION("query") {
            auto arg = rlm_str_val("Hello, World!");
            auto q =
                cptr_checked(realm_query_parse(realm, class_foo.key, "string == $0 SORT(int ASCENDING)", 1, &arg));

            SECTION("realm_query_description()") {
                const char* descr = realm_query_get_description(q.get());
                std::string expected = "string == \"Hello, World!\" SORT(int ASC)";
                CHECK(descr == expected);
            }

            SECTION("realm_query_count()") {
                size_t count;
                CHECK(checked(realm_query_count(q.get(), &count)));
                CHECK(count == 1);

                SECTION("cloned") {
                    auto q2 = clone_cptr(q.get());
                    size_t count2;
                    CHECK(checked(realm_query_count(q2.get(), &count2)));
                    CHECK(count == count2);
                }
                SECTION("realm_query_append_query") {
                    auto q2 = cptr_checked(realm_query_append_query(q.get(), "TRUEPREDICATE LIMIT(1)", 1, &arg));
                    size_t count;
                    CHECK(checked(realm_query_count(q2.get(), &count)));
                    CHECK(count == 1);
                    q2 = cptr_checked(realm_query_append_query(q.get(), "FALSEPREDICATE", 1, &arg));
                    CHECK(checked(realm_query_count(q2.get(), &count)));
                    CHECK(count == 0);
                    q2 = cptr_checked(realm_query_append_query(q.get(), "TRUEPREDICATE LIMIT(0)", 1, &arg));
                    CHECK(checked(realm_query_count(q2.get(), &count)));
                    CHECK(count == 0);
                    q2 = cptr_checked(realm_query_append_query(q.get(), "TRUEPREDICATE LIMIT(10)", 1, &arg));
                    CHECK(checked(realm_query_count(q2.get(), &count)));
                    CHECK(count == 1);
                    q2 = cptr_checked(realm_query_append_query(q.get(), "int == $0", 1, &int_val2));
                    CHECK(checked(realm_query_count(q2.get(), &count)));
                    CHECK(count == 0);
                }
            }

            SECTION("realm_query_parse() errors") {
                // Invalid class key
                CHECK(!realm_query_parse(realm, 123123123, "string == $0", 1, &arg));
                CHECK_ERR(RLM_ERR_NO_SUCH_TABLE);

                // Invalid syntax
                CHECK(!realm_query_parse(realm, class_foo.key, "lel", 0, nullptr));
                CHECK_ERR(RLM_ERR_INVALID_QUERY_STRING);

                // Invalid number of arguments
                CHECK(!realm_query_parse(realm, class_foo.key, "string == $0", 0, nullptr));
                CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
            }

            SECTION("interpolate all types") {
                realm_value_t int_arg = rlm_int_val(123);

                realm_value_t bool_arg = rlm_bool_val(true);
                realm_value_t string_arg = rlm_str_val("foobar");
                static const uint8_t binary_data[3] = {1, 2, 3};
                realm_value_t binary_arg = rlm_binary_val(binary_data, 3);
                realm_value_t timestamp_arg = rlm_timestamp_val(1000000, 1);
                realm_value_t float_arg = rlm_float_val(123.f);
                realm_value_t double_arg = rlm_double_val(456.0);
                realm_value_t decimal_arg = rlm_decimal_val(789.0);
                realm_value_t object_id_arg = rlm_object_id_val("abc123abc123");
                realm_value_t uuid_arg = rlm_uuid_val("01234567-9abc-4def-9012-3456789abcde");
                realm_value_t link_arg = rlm_link_val(class_bar.key, realm_object_get_key(obj2.get()));

                auto q_int = cptr_checked(realm_query_parse(realm, class_foo.key, "int == $0", 1, &int_arg));
                auto q_bool = cptr_checked(realm_query_parse(realm, class_foo.key, "bool == $0", 1, &bool_arg));
                auto q_string = cptr_checked(realm_query_parse(realm, class_foo.key, "string == $0", 1, &string_arg));
                auto q_binary = cptr_checked(realm_query_parse(realm, class_foo.key, "binary == $0", 1, &binary_arg));
                auto q_timestamp =
                    cptr_checked(realm_query_parse(realm, class_foo.key, "timestamp == $0", 1, &timestamp_arg));
                auto q_float = cptr_checked(realm_query_parse(realm, class_foo.key, "float == $0", 1, &float_arg));
                auto q_double = cptr_checked(realm_query_parse(realm, class_foo.key, "double == $0", 1, &double_arg));
                auto q_decimal =
                    cptr_checked(realm_query_parse(realm, class_foo.key, "decimal == $0", 1, &decimal_arg));
                auto q_object_id =
                    cptr_checked(realm_query_parse(realm, class_foo.key, "object_id == $0", 1, &object_id_arg));
                auto q_uuid = cptr_checked(realm_query_parse(realm, class_foo.key, "uuid == $0", 1, &uuid_arg));
                auto q_link = cptr_checked(realm_query_parse(realm, class_foo.key, "link == $0", 1, &link_arg));

                CHECK(cptr_checked(realm_query_find_all(q_int.get())));
                CHECK(cptr_checked(realm_query_find_all(q_bool.get())));
                CHECK(cptr_checked(realm_query_find_all(q_string.get())));
                CHECK(cptr_checked(realm_query_find_all(q_binary.get())));
                CHECK(cptr_checked(realm_query_find_all(q_timestamp.get())));
                CHECK(cptr_checked(realm_query_find_all(q_float.get())));
                CHECK(cptr_checked(realm_query_find_all(q_double.get())));
                CHECK(cptr_checked(realm_query_find_all(q_decimal.get())));
                CHECK(cptr_checked(realm_query_find_all(q_object_id.get())));
                CHECK(cptr_checked(realm_query_find_all(q_uuid.get())));
                CHECK(cptr_checked(realm_query_find_all(q_link.get())));

                SECTION("type mismatch") {
                    CHECK(!realm_query_parse(realm, class_foo.key, "int == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "bool == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "string == $0", 1, &decimal_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "timestamp == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "double == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "float == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "binary == $0", 1, &int_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "decimal == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "object_id == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "uuid == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                    CHECK(!realm_query_parse(realm, class_foo.key, "link == $0", 1, &string_arg));
                    CHECK_ERR(RLM_ERR_INVALID_QUERY);
                }
            }

            SECTION("realm_query_find_first()") {
                realm_value_t found_value = rlm_null();
                bool found;
                CHECK(checked(realm_query_find_first(q.get(), &found_value, &found)));
                CHECK(found);
                CHECK(found_value.type == RLM_TYPE_LINK);
                CHECK(found_value.link.target_table == class_foo.key);
                CHECK(found_value.link.target == realm_object_get_key(obj1.get()));
            }

            SECTION("results") {
                auto r = cptr_checked(realm_query_find_all(q.get()));
                CHECK(!realm_is_frozen(r.get()));

                SECTION("realm_results_count()") {
                    size_t count;
                    CHECK(checked(realm_results_count(r.get(), &count)));
                    CHECK(count == 1);

                    SECTION("cloned") {
                        auto r2 = clone_cptr(r.get());
                        size_t count2;
                        CHECK(checked(realm_results_count(r2.get(), &count2)));
                        CHECK(count == count2);
                    }
                }

                SECTION("empty result") {
                    auto q2 =
                        cptr_checked(realm_query_parse(realm, class_foo.key, "string == 'boogeyman'", 0, nullptr));
                    auto r2 = cptr_checked(realm_query_find_all(q2.get()));
                    size_t count;
                    CHECK(checked(realm_results_count(r2.get(), &count)));
                    CHECK(count == 0);
                    realm_value_t value = rlm_null();
                    CHECK(!realm_results_get(r2.get(), 0, &value));
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("realm_results_get()") {
                    realm_value_t value = rlm_null();
                    CHECK(checked(realm_results_get(r.get(), 0, &value)));
                    CHECK(value.type == RLM_TYPE_LINK);
                    CHECK(value.link.target_table == class_foo.key);
                    CHECK(value.link.target == realm_object_get_key(obj1.get()));

                    CHECK(!realm_results_get(r.get(), 1, &value));
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("realm_results_get_object()") {
                    auto p = cptr_checked(realm_results_get_object(r.get(), 0));
                    CHECK(p.get());
                    CHECK(realm_equals(p.get(), obj1.get()));

                    CHECK(!realm_results_get_object(r.get(), 1));
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("realm_results_filter()") {
                    auto q2 = cptr_checked(realm_query_parse(realm, class_foo.key, "int == 789", 0, nullptr));
                    auto r2 = cptr_checked(realm_results_filter(r.get(), q2.get()));
                    size_t count;
                    CHECK(checked(realm_results_count(r2.get(), &count)));
                    CHECK(count == 0);
                }

                SECTION("realm_results_sort()") {
                    auto r_all = cptr_checked(realm_object_find_all(realm, class_foo.key));
                    auto p = cptr_checked(realm_results_get_object(r_all.get(), 0));
                    CHECK(p.get());
                    CHECK(realm_equals(p.get(), obj1.get()));
                    auto r2 = cptr_checked(realm_results_sort(r_all.get(), "int DESCENDING, float ASCENDING"));
                    p = cptr_checked(realm_results_get_object(r2.get(), 1));
                    CHECK(p.get());
                    CHECK(realm_equals(p.get(), obj1.get()));
                }

                SECTION("realm_results_distinct()") {
                    auto r_all = cptr_checked(realm_object_find_all(realm, class_foo.key));
                    size_t count;
                    realm_results_count(r_all.get(), &count);
                    CHECK(count == 3);
                    auto r2 = cptr_checked(realm_results_distinct(r_all.get(), "int"));
                    realm_results_count(r2.get(), &count);
                    CHECK(count == 2);
                }

                SECTION("realm_results_limit()") {
                    auto r_all = cptr_checked(realm_object_find_all(realm, class_foo.key));
                    size_t count;
                    realm_results_count(r_all.get(), &count);
                    CHECK(count == 3);
                    auto r2 = cptr_checked(realm_results_limit(r_all.get(), 1));
                    realm_results_count(r2.get(), &count);
                    CHECK(count == 1);
                }

                SECTION("realm_results_snapshot()") {
                    auto r_all = cptr_checked(realm_object_find_all(realm, class_foo.key));
                    auto r_snapshot = cptr_checked(realm_results_snapshot(r_all.get()));
                    size_t count;
                    realm_results_count(r_all.get(), &count);
                    CHECK(count == 3);
                    realm_results_count(r_snapshot.get(), &count);
                    CHECK(count == 3);
                    write([&]() {
                        auto p = cptr_checked(realm_results_get_object(r_all.get(), 0));
                        realm_object_delete(p.get());
                    });
                    realm_results_count(r_all.get(), &count);
                    CHECK(count == 2);
                    realm_results_count(r_snapshot.get(), &count);
                    CHECK(count == 3);
                }

                SECTION("realm_results_min()") {
                    realm_value_t value = rlm_null();
                    CHECK(checked(realm_results_min(r.get(), foo_int_key, &value, &found)));
                    CHECK(found);
                    CHECK(value.type == RLM_TYPE_INT);
                    CHECK(value.integer == 123);

                    CHECK(!realm_results_min(r.get(), RLM_INVALID_PROPERTY_KEY, nullptr, nullptr));
                    CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
                }

                SECTION("realm_results_max()") {
                    realm_value_t value = rlm_null();
                    CHECK(checked(realm_results_max(r.get(), foo_int_key, &value, &found)));
                    CHECK(found);
                    CHECK(value.type == RLM_TYPE_INT);
                    CHECK(value.integer == 123);

                    CHECK(!realm_results_max(r.get(), RLM_INVALID_PROPERTY_KEY, nullptr, nullptr));
                    CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
                }

                SECTION("realm_results_sum()") {
                    realm_value_t value = rlm_null();
                    CHECK(checked(realm_results_sum(r.get(), foo_int_key, &value, &found)));
                    CHECK(found);
                    CHECK(value.type == RLM_TYPE_INT);
                    CHECK(value.integer == 123);

                    size_t count;
                    realm_results_count(r.get(), &count);
                    CHECK(count == 1);

                    CHECK(!realm_results_sum(r.get(), RLM_INVALID_PROPERTY_KEY, nullptr, nullptr));
                    CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
                }

                SECTION("realm_results_average()") {
                    realm_value_t value = rlm_null();
                    CHECK(checked(realm_results_average(r.get(), foo_int_key, &value, &found)));
                    CHECK(found);
                    CHECK(value.type == RLM_TYPE_DOUBLE);
                    CHECK(value.dnum == 123.0);

                    CHECK(!realm_results_average(r.get(), RLM_INVALID_PROPERTY_KEY, nullptr, nullptr));
                    CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
                }

                SECTION("realm_results_delete_all()") {
                    CHECK(!realm_results_delete_all(r.get()));
                    CHECK_ERR(RLM_ERR_NOT_IN_A_TRANSACTION);

                    write([&]() {
                        size_t num_objects;
                        CHECK(checked(realm_get_num_objects(realm, class_foo.key, &num_objects)));
                        CHECK(num_objects == 3);
                        CHECK(checked(realm_results_delete_all(r.get())));
                        CHECK(checked(realm_get_num_objects(realm, class_foo.key, &num_objects)));
                        CHECK(num_objects == 2);
                    });
                }

                SECTION("lists") {
                    auto list = cptr_checked(realm_get_list(obj1.get(), foo_properties["link_list"]));
                    cptr_checked(realm_query_parse_for_list(list.get(), "TRUEPREDICATE", 0, nullptr));
                }

                SECTION("empty results") {
                    auto empty_q = cptr_checked(realm_query_parse_for_results(r.get(), "FALSEPREDICATE", 0, nullptr));
                    auto empty_r = cptr_checked(realm_query_find_all(empty_q.get()));

                    SECTION("realm_results_count()") {
                        size_t count;
                        CHECK(realm_results_count(empty_r.get(), &count));
                        CHECK(count == 0);
                    }

                    SECTION("realm_results_min()") {
                        realm_value_t value;
                        bool found = true;
                        CHECK(realm_results_min(empty_r.get(), foo_int_key, &value, &found));
                        CHECK(rlm_val_eq(value, rlm_null()));
                        CHECK(!found);
                    }

                    SECTION("realm_results_max()") {
                        realm_value_t value;
                        bool found = true;
                        CHECK(realm_results_max(empty_r.get(), foo_int_key, &value, &found));
                        CHECK(rlm_val_eq(value, rlm_null()));
                        CHECK(!found);
                    }

                    SECTION("realm_results_sum()") {
                        realm_value_t value;
                        bool found = true;
                        CHECK(realm_results_sum(empty_r.get(), foo_int_key, &value, &found));
                        CHECK(rlm_val_eq(value, rlm_int_val(0)));
                        CHECK(!found);
                    }

                    SECTION("realm_results_average()") {
                        realm_value_t value;
                        bool found = true;
                        CHECK(realm_results_average(empty_r.get(), foo_int_key, &value, &found));
                        CHECK(rlm_val_eq(value, rlm_null()));
                        CHECK(!found);
                    }
                }
            }
        }

        SECTION("delete causes invalidation errors") {
            write([&]() {
                // Get a list instance for later
                auto list = cptr_checked(realm_get_list(obj1.get(), foo_links_key));

                CHECK(checked(realm_object_delete(obj1.get())));
                CHECK(!realm_object_is_valid(obj1.get()));

                realm_clear_last_error();
                CHECK(!realm_object_delete(obj1.get()));
                CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);

                realm_clear_last_error();
                CHECK(!realm_set_value(obj1.get(), foo_int_key, rlm_int_val(123), false));
                CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);

                realm_clear_last_error();
                auto list2 = realm_get_list(obj1.get(), foo_links_key);
                CHECK(!list2);
                CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);

                size_t size;
                CHECK(!realm_list_size(list.get(), &size));
                CHECK_ERR(RLM_ERR_INVALIDATED_OBJECT);
            });
        }

        SECTION("lists") {
            SECTION("realm_get_list() errors") {
                CHECK(!realm_get_list(obj2.get(), bar_int_key));
                CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);

                CHECK(!realm_get_list(obj2.get(), 123123123));
                CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
            }

            SECTION("nullable strings") {
                auto strings = cptr_checked(realm_get_list(obj2.get(), bar_strings_key));
                CHECK(strings);
                CHECK(!realm_is_frozen(strings.get()));

                realm_value_t a = rlm_str_val("a");
                realm_value_t b = rlm_str_val("b");
                realm_value_t c = rlm_null();

                SECTION("realm_equals() type check") {
                    CHECK(!realm_equals(strings.get(), obj1.get()));
                }

                SECTION("realm_clone()") {
                    auto list2 = clone_cptr(strings.get());
                    CHECK(realm_equals(strings.get(), list2.get()));
                    CHECK(strings.get() != list2.get());
                }

                SECTION("insert, then get") {
                    write([&]() {
                        CHECK(checked(realm_list_insert(strings.get(), 0, a)));
                        CHECK(checked(realm_list_insert(strings.get(), 1, b)));
                        CHECK(checked(realm_list_insert(strings.get(), 2, c)));

                        realm_value_t a2, b2, c2;
                        CHECK(checked(realm_list_get(strings.get(), 0, &a2)));
                        CHECK(checked(realm_list_get(strings.get(), 1, &b2)));
                        CHECK(checked(realm_list_get(strings.get(), 2, &c2)));

                        CHECK(rlm_stdstr(a2) == "a");
                        CHECK(rlm_stdstr(b2) == "b");
                        CHECK(c2.type == RLM_TYPE_NULL);
                    });
                }

                SECTION("equality") {
                    auto strings2 = cptr_checked(realm_get_list(obj2.get(), bar_strings_key));
                    CHECK(strings2);
                    CHECK(realm_equals(strings.get(), strings2.get()));

                    write([&]() {
                        auto obj3 =
                            cptr_checked(realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(2)));
                        CHECK(obj3);
                        auto strings3 = cptr_checked(realm_get_list(obj3.get(), bar_strings_key));
                        CHECK(!realm_equals(strings.get(), strings3.get()));
                    });
                }
            }

            SECTION("get/insert all property types") {
                realm_value_t null = rlm_null();
                realm_value_t integer = rlm_int_val(987);
                realm_value_t boolean = rlm_bool_val(true);
                realm_value_t string = rlm_str_val("My string");
                const uint8_t binary_data[] = {0, 1, 2, 3, 4, 5, 6, 7};
                realm_value_t binary = rlm_binary_val(binary_data, sizeof(binary_data));
                realm_value_t timestamp = rlm_timestamp_val(1000000, 123123123);
                realm_value_t fnum = rlm_float_val(123.f);
                realm_value_t dnum = rlm_double_val(456.0);
                realm_value_t decimal = rlm_decimal_val(999.0);
                realm_value_t object_id = rlm_object_id_val("abc123abc123");
                realm_value_t uuid = rlm_uuid_val("01234567-9abc-4def-9012-3456789abcde");

                auto int_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["int_list"]));
                auto bool_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["bool_list"]));
                auto string_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["string_list"]));
                auto binary_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["binary_list"]));
                auto timestamp_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["timestamp_list"]));
                auto float_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["float_list"]));
                auto double_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["double_list"]));
                auto decimal_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["decimal_list"]));
                auto object_id_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["object_id_list"]));
                auto uuid_list = cptr_checked(realm_get_list(obj1.get(), foo_properties["uuid_list"]));
                auto nullable_int_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_int_list"]));
                auto nullable_bool_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_bool_list"]));
                auto nullable_string_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_string_list"]));
                auto nullable_binary_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_binary_list"]));
                auto nullable_timestamp_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_timestamp_list"]));
                auto nullable_float_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_float_list"]));
                auto nullable_double_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_double_list"]));
                auto nullable_decimal_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_decimal_list"]));
                auto nullable_object_id_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_object_id_list"]));
                auto nullable_uuid_list =
                    cptr_checked(realm_get_list(obj1.get(), foo_properties["nullable_uuid_list"]));

                write([&]() {
                    CHECK(realm_list_insert(int_list.get(), 0, integer));
                    CHECK(realm_list_insert(bool_list.get(), 0, boolean));
                    CHECK(realm_list_insert(string_list.get(), 0, string));
                    CHECK(realm_list_insert(binary_list.get(), 0, binary));
                    CHECK(realm_list_insert(timestamp_list.get(), 0, timestamp));
                    CHECK(realm_list_insert(float_list.get(), 0, fnum));
                    CHECK(realm_list_insert(double_list.get(), 0, dnum));
                    CHECK(realm_list_insert(decimal_list.get(), 0, decimal));
                    CHECK(realm_list_insert(object_id_list.get(), 0, object_id));
                    CHECK(realm_list_insert(uuid_list.get(), 0, uuid));

                    CHECK(realm_list_insert(nullable_int_list.get(), 0, integer));
                    CHECK(realm_list_insert(nullable_bool_list.get(), 0, boolean));
                    CHECK(realm_list_insert(nullable_string_list.get(), 0, string));
                    CHECK(realm_list_insert(nullable_binary_list.get(), 0, binary));
                    CHECK(realm_list_insert(nullable_timestamp_list.get(), 0, timestamp));
                    CHECK(realm_list_insert(nullable_float_list.get(), 0, fnum));
                    CHECK(realm_list_insert(nullable_double_list.get(), 0, dnum));
                    CHECK(realm_list_insert(nullable_decimal_list.get(), 0, decimal));
                    CHECK(realm_list_insert(nullable_object_id_list.get(), 0, object_id));
                    CHECK(realm_list_insert(nullable_uuid_list.get(), 0, uuid));

                    CHECK(realm_list_insert(nullable_int_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_bool_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_string_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_binary_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_timestamp_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_float_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_double_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_decimal_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_object_id_list.get(), 1, null));
                    CHECK(realm_list_insert(nullable_uuid_list.get(), 1, null));
                });

                realm_value_t value;

                CHECK(realm_list_get(int_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_list_get(bool_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_list_get(string_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_list_get(binary_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_list_get(timestamp_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_list_get(float_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_list_get(double_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_list_get(decimal_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_list_get(object_id_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_list_get(uuid_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, uuid));
                CHECK(realm_list_get(nullable_int_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_list_get(nullable_bool_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_list_get(nullable_string_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_list_get(nullable_binary_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_list_get(nullable_timestamp_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_list_get(nullable_float_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_list_get(nullable_double_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_list_get(nullable_decimal_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_list_get(nullable_object_id_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_list_get(nullable_uuid_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, uuid));

                write([&]() {
                    CHECK(realm_list_insert(nullable_int_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_bool_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_string_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_binary_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_timestamp_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_float_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_double_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_decimal_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_object_id_list.get(), 0, null));
                    CHECK(realm_list_insert(nullable_uuid_list.get(), 0, null));
                });

                CHECK(realm_list_get(nullable_int_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_bool_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_string_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_binary_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_timestamp_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_float_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_double_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_decimal_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_object_id_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_list_get(nullable_uuid_list.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
            }

            SECTION("links") {
                CPtr<realm_list_t> bars;

                write([&]() {
                    bars = cptr_checked(realm_get_list(obj1.get(), foo_links_key));
                    auto bar_link = realm_object_as_link(obj2.get());
                    realm_value_t bar_link_val;
                    bar_link_val.type = RLM_TYPE_LINK;
                    bar_link_val.link = bar_link;
                    CHECK(checked(realm_list_insert(bars.get(), 0, bar_link_val)));
                    CHECK(checked(realm_list_insert(bars.get(), 1, bar_link_val)));
                    size_t size;
                    CHECK(checked(realm_list_size(bars.get(), &size)));
                    CHECK(size == 2);
                });

                SECTION("get") {
                    realm_value_t val;
                    CHECK(checked(realm_list_get(bars.get(), 0, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == class_bar.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    CHECK(checked(realm_list_get(bars.get(), 1, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == class_bar.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    auto result = realm_list_get(bars.get(), 2, &val);
                    CHECK(!result);
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("set wrong type") {
                    write([&]() {
                        auto foo2 = cptr(realm_object_create(realm, class_foo.key));
                        CHECK(foo2);
                        realm_value_t foo2_link_val;
                        foo2_link_val.type = RLM_TYPE_LINK;
                        foo2_link_val.link = realm_object_as_link(foo2.get());

                        CHECK(!realm_list_set(bars.get(), 0, foo2_link_val));
                        CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);
                    });
                }

                SECTION("realm_list_clear()") {
                    write([&]() {
                        CHECK(realm_list_clear(bars.get()));
                    });
                    size_t size;
                    CHECK(realm_list_size(bars.get(), &size));
                    CHECK(size == 0);

                    size_t num_bars;
                    CHECK(realm_get_num_objects(realm, class_bar.key, &num_bars));
                    CHECK(num_bars != 0);
                }

                SECTION("realm_list_remove_all()") {
                    size_t num_bars;
                    size_t size;

                    write([&]() {
                        CHECK(checked(realm_list_remove_all(bars.get())));
                    });

                    CHECK(realm_list_size(bars.get(), &size));
                    CHECK(size == 0);

                    CHECK(realm_get_num_objects(realm, class_bar.key, &num_bars));
                    CHECK(num_bars == 0);
                }
            }

            SECTION("notifications") {
                struct State {
                    CPtr<realm_collection_changes_t> changes;
                    CPtr<realm_async_error_t> error;
                    bool destroyed = false;
                    bool called = false;
                };

                State state;

                auto on_change = [](void* userdata, const realm_collection_changes_t* changes) {
                    auto* state = static_cast<State*>(userdata);
                    state->changes = clone_cptr(changes);
                    state->called = true;
                };

                auto on_error = [](void* userdata, const realm_async_error_t* err) {
                    auto* state = static_cast<State*>(userdata);
                    state->error = clone_cptr(err);
                };

                CPtr<realm_list_t> strings = cptr_checked(realm_get_list(obj2.get(), bar_strings_key));

                auto str1 = rlm_str_val("a");
                auto str2 = rlm_str_val("b");
                auto null = rlm_null();

                auto require_change = [&]() {
                    auto token = cptr_checked(realm_list_add_notification_callback(
                        strings.get(), &state, nullptr, nullptr, on_change, on_error, nullptr));
                    checked(realm_refresh(realm));
                    return token;
                };

                SECTION("userdata is freed when the token is destroyed") {
                    auto token = cptr_checked(realm_list_add_notification_callback(
                        strings.get(), &state,
                        [](void* p) {
                            static_cast<State*>(p)->destroyed = true;
                        },
                        nullptr, nullptr, nullptr, nullptr));
                    CHECK(!state.destroyed);
                    token.reset();
                    CHECK(state.destroyed);
                }

                SECTION("insertion sends a change callback") {
                    auto token = require_change();
                    write([&]() {
                        checked(realm_list_insert(strings.get(), 0, str1));
                        checked(realm_list_insert(strings.get(), 1, str2));
                        checked(realm_list_insert(strings.get(), 2, null));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 0);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 0);
                    CHECK(num_moves == 0);

                    realm_index_range_t insertion_range;
                    realm_collection_changes_get_ranges(state.changes.get(), nullptr, 0, &insertion_range, 1, nullptr,
                                                        0, nullptr, 0, nullptr, 0);
                    CHECK(insertion_range.from == 0);
                    CHECK(insertion_range.to == 3);
                }

                SECTION("modifying target of list with a filter") {
                    auto bars = cptr_checked(realm_get_list(obj1.get(), foo_links_key));
                    write([&]() {
                        auto bar_link = realm_object_as_link(obj2.get());
                        realm_value_t bar_link_val;
                        bar_link_val.type = RLM_TYPE_LINK;
                        bar_link_val.link = bar_link;
                        CHECK(checked(realm_list_insert(bars.get(), 0, bar_link_val)));
                    });

                    realm_key_path_elem_t bar_strings[] = {{class_bar.key, bar_doubles_key}};
                    realm_key_path_t key_path_bar_strings[] = {{1, bar_strings}};
                    realm_key_path_array_t key_path_array = {1, key_path_bar_strings};
                    auto token = cptr_checked(realm_list_add_notification_callback(
                        bars.get(), &state, nullptr, &key_path_array, on_change, on_error, nullptr));
                    checked(realm_refresh(realm));

                    state.called = false;
                    write([&]() {
                        checked(realm_set_value(obj2.get(), bar_doubles_key, rlm_double_val(5.0), false));
                    });
                    REQUIRE(state.called);
                    CHECK(!state.error);
                    CHECK(state.changes);

                    state.called = false;
                    write([&]() {
                        checked(realm_list_insert(strings.get(), 0, str1));
                        checked(realm_list_insert(strings.get(), 1, str2));
                        checked(realm_list_insert(strings.get(), 2, null));
                    });
                    REQUIRE(!state.called);
                }

                SECTION("insertion, deletion, modification, modification after") {
                    write([&]() {
                        checked(realm_list_insert(strings.get(), 0, str1));
                        checked(realm_list_insert(strings.get(), 1, str2));
                        checked(realm_list_insert(strings.get(), 2, str1));
                    });

                    auto token = require_change();

                    write([&]() {
                        checked(realm_list_erase(strings.get(), 1));
                        checked(realm_list_insert(strings.get(), 0, null));
                        checked(realm_list_insert(strings.get(), 1, null));

                        // This element was previously at 0, and ends up at 2.
                        checked(realm_list_set(strings.get(), 2, str1));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 1);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 1);
                    CHECK(num_moves == 0);

                    size_t num_deletions, num_insertions, num_modifications;
                    realm_collection_changes_get_num_changes(state.changes.get(), &num_deletions, &num_insertions,
                                                             &num_modifications, &num_moves);
                    CHECK(num_deletions == 1);
                    CHECK(num_insertions == 2);
                    CHECK(num_modifications == 1);

                    realm_index_range_t deletions, insertions, modifications, modifications_after;
                    realm_collection_move_t moves;
                    realm_collection_changes_get_ranges(state.changes.get(), &deletions, 1, &insertions, 1,
                                                        &modifications, 1, &modifications_after, 1, &moves, 1);
                    CHECK(deletions.from == 1);
                    CHECK(deletions.to == 2);

                    CHECK(insertions.from == 0);
                    CHECK(insertions.to == 2);

                    CHECK(modifications.from == 0);
                    CHECK(modifications.to == 1);

                    CHECK(modifications_after.from == 2);
                    CHECK(modifications_after.to == 3);

                    std::vector<size_t> deletions_v, insertions_v, modifications_v, modifications_after_v;
                    std::vector<realm_collection_move_t> moves_v;
                    deletions_v.resize(100, size_t(-1));
                    insertions_v.resize(100, size_t(-1));
                    modifications_v.resize(100, size_t(-1));
                    modifications_after_v.resize(100, size_t(-1));
                    moves_v.resize(100, realm_collection_move_t{size_t(-1), size_t(-1)});
                    realm_collection_changes_get_changes(state.changes.get(), deletions_v.data(), 100,
                                                         insertions_v.data(), 100, modifications_v.data(), 100,
                                                         modifications_after_v.data(), 100, moves_v.data(), 100);
                    CHECK(deletions_v[0] == 1);
                    CHECK(deletions_v[1] == size_t(-1));
                    CHECK(insertions_v[0] == 0);
                    CHECK(insertions_v[1] == 1);
                    CHECK(insertions_v[2] == size_t(-1));
                    CHECK(modifications_v[0] == 0);
                    CHECK(modifications_v[1] == size_t(-1));
                    CHECK(modifications_after_v[0] == 2);
                    CHECK(modifications_after_v[1] == size_t(-1));
                }
            }
        }

        SECTION("sets") {
            SECTION("realm_get_set() errors") {
                CHECK(!realm_get_set(obj1.get(), foo_properties["int"]));
                CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);

                CHECK(!realm_get_set(obj1.get(), 123123123));
                CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
            }

            SECTION("nullable strings") {
                auto strings = cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_string_set"]));
                CHECK(strings);
                CHECK(!realm_is_frozen(strings.get()));

                realm_value_t a = rlm_str_val("a");
                realm_value_t b = rlm_str_val("b");
                realm_value_t c = rlm_null();

                SECTION("realm_equals() type check") {
                    CHECK(!realm_equals(strings.get(), obj1.get()));
                }

                SECTION("realm_clone()") {
                    auto set2 = clone_cptr(strings.get());
                    CHECK(realm_equals(strings.get(), set2.get()));
                    CHECK(strings.get() != set2.get());
                }

                SECTION("insert, then get, then erase") {
                    write([&]() {
                        bool inserted = false;
                        CHECK(checked(realm_set_insert(strings.get(), a, nullptr, &inserted)));
                        CHECK(inserted);
                        CHECK(checked(realm_set_insert(strings.get(), b, nullptr, &inserted)));
                        CHECK(inserted);
                        CHECK(checked(realm_set_insert(strings.get(), c, nullptr, &inserted)));
                        CHECK(inserted);

                        size_t a_index, b_index, c_index;
                        bool found = false;
                        CHECK(checked(realm_set_find(strings.get(), a, &a_index, &found)));
                        CHECK(found);
                        CHECK(checked(realm_set_find(strings.get(), b, &b_index, &found)));
                        CHECK(found);
                        CHECK(checked(realm_set_find(strings.get(), c, &c_index, &found)));
                        CHECK(found);

                        realm_value_t a2, b2, c2;
                        CHECK(checked(realm_set_get(strings.get(), a_index, &a2)));
                        CHECK(checked(realm_set_get(strings.get(), b_index, &b2)));
                        CHECK(checked(realm_set_get(strings.get(), c_index, &c2)));

                        CHECK(rlm_stdstr(a2) == "a");
                        CHECK(rlm_stdstr(b2) == "b");
                        CHECK(c2.type == RLM_TYPE_NULL);

                        bool erased = false;
                        CHECK(checked(realm_set_erase(strings.get(), a2, &erased)));
                        CHECK(erased);
                        CHECK(checked(realm_set_erase(strings.get(), rlm_int_val(987), &erased)));
                        CHECK(!erased);
                    });
                }

                SECTION("equality") {
                    auto strings2 = cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_string_set"]));
                    CHECK(strings2);
                    CHECK(realm_equals(strings.get(), strings2.get()));

                    write([&]() {
                        auto obj3 = cptr_checked(realm_object_create(realm, class_foo.key));
                        CHECK(obj3);
                        auto strings3 =
                            cptr_checked(realm_get_set(obj3.get(), foo_properties["nullable_string_set"]));
                        CHECK(!realm_equals(strings.get(), strings3.get()));
                    });
                }
            }

            SECTION("get/insert all property types") {
                realm_value_t null = rlm_null();
                realm_value_t integer = rlm_int_val(987);
                realm_value_t boolean = rlm_bool_val(true);
                realm_value_t string = rlm_str_val("My string");
                const uint8_t binary_data[] = {0, 1, 2, 3, 4, 5, 6, 7};
                realm_value_t binary = rlm_binary_val(binary_data, sizeof(binary_data));
                realm_value_t timestamp = rlm_timestamp_val(1000000, 123123123);
                realm_value_t fnum = rlm_float_val(123.f);
                realm_value_t dnum = rlm_double_val(456.0);
                realm_value_t decimal = rlm_decimal_val(999.0);
                realm_value_t object_id = rlm_object_id_val("abc123abc123");
                realm_value_t uuid = rlm_uuid_val("01234567-9abc-4def-9012-3456789abcde");

                auto int_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["int_set"]));
                auto bool_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["bool_set"]));
                auto string_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["string_set"]));
                auto binary_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["binary_set"]));
                auto timestamp_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["timestamp_set"]));
                auto float_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["float_set"]));
                auto double_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["double_set"]));
                auto decimal_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["decimal_set"]));
                auto object_id_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["object_id_set"]));
                auto uuid_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["uuid_set"]));
                auto nullable_int_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_int_set"]));
                auto nullable_bool_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_bool_set"]));
                auto nullable_string_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_string_set"]));
                auto nullable_binary_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_binary_set"]));
                auto nullable_timestamp_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_timestamp_set"]));
                auto nullable_float_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_float_set"]));
                auto nullable_double_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_double_set"]));
                auto nullable_decimal_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_decimal_set"]));
                auto nullable_object_id_set =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_object_id_set"]));
                auto nullable_uuid_set = cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_uuid_set"]));

                write([&]() {
                    CHECK(realm_set_insert(int_set.get(), integer, nullptr, nullptr));
                    CHECK(realm_set_insert(bool_set.get(), boolean, nullptr, nullptr));
                    CHECK(realm_set_insert(string_set.get(), string, nullptr, nullptr));
                    CHECK(realm_set_insert(binary_set.get(), binary, nullptr, nullptr));
                    CHECK(realm_set_insert(timestamp_set.get(), timestamp, nullptr, nullptr));
                    CHECK(realm_set_insert(float_set.get(), fnum, nullptr, nullptr));
                    CHECK(realm_set_insert(double_set.get(), dnum, nullptr, nullptr));
                    CHECK(realm_set_insert(decimal_set.get(), decimal, nullptr, nullptr));
                    CHECK(realm_set_insert(object_id_set.get(), object_id, nullptr, nullptr));
                    CHECK(realm_set_insert(uuid_set.get(), uuid, nullptr, nullptr));

                    CHECK(realm_set_insert(nullable_int_set.get(), integer, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_bool_set.get(), boolean, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_string_set.get(), string, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_binary_set.get(), binary, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_timestamp_set.get(), timestamp, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_float_set.get(), fnum, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_double_set.get(), dnum, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_decimal_set.get(), decimal, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_object_id_set.get(), object_id, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_uuid_set.get(), uuid, nullptr, nullptr));

                    CHECK(realm_set_insert(nullable_int_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_bool_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_string_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_binary_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_timestamp_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_float_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_double_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_decimal_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_object_id_set.get(), null, nullptr, nullptr));
                    CHECK(realm_set_insert(nullable_uuid_set.get(), null, nullptr, nullptr));
                });

                realm_value_t value;

                CHECK(realm_set_get(int_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_set_get(bool_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_set_get(string_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_set_get(binary_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_set_get(timestamp_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_set_get(float_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_set_get(double_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_set_get(decimal_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_set_get(object_id_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_set_get(uuid_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, uuid));
                CHECK(realm_set_get(nullable_int_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_set_get(nullable_bool_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_set_get(nullable_string_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_set_get(nullable_binary_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_set_get(nullable_timestamp_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_set_get(nullable_float_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_set_get(nullable_double_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_set_get(nullable_decimal_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_set_get(nullable_object_id_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_set_get(nullable_uuid_set.get(), 1, &value));
                CHECK(rlm_val_eq(value, uuid));

                write([&]() {
                    size_t index;
                    bool inserted;
                    CHECK(realm_set_insert(nullable_int_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_bool_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_string_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_binary_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_timestamp_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_float_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_double_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_decimal_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_object_id_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                    CHECK(realm_set_insert(nullable_uuid_set.get(), null, &index, &inserted));
                    CHECK((index == 0 && !inserted));
                });

                // Note: This relies on the fact that NULL is "less than" other
                // values in the internal sort order.
                CHECK(realm_set_get(nullable_int_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_bool_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_string_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_binary_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_timestamp_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_float_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_double_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_decimal_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_object_id_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_set_get(nullable_uuid_set.get(), 0, &value));
                CHECK(rlm_val_eq(value, null));
            }

            SECTION("links") {
                CPtr<realm_set_t> bars;

                write([&]() {
                    bars = cptr_checked(realm_get_set(obj1.get(), foo_properties["link_set"]));
                    auto bar_link = realm_object_as_link(obj2.get());
                    realm_value_t bar_link_val;
                    bar_link_val.type = RLM_TYPE_LINK;
                    bar_link_val.link = bar_link;
                    size_t index;
                    bool inserted;
                    CHECK(checked(realm_set_insert(bars.get(), bar_link_val, &index, &inserted)));
                    CHECK((index == 0 && inserted));
                    CHECK(checked(realm_set_insert(bars.get(), bar_link_val, &index, &inserted)));
                    CHECK((index == 0 && !inserted));
                    size_t size;
                    CHECK(checked(realm_set_size(bars.get(), &size)));
                    CHECK(size == 1);
                });

                SECTION("get") {
                    realm_value_t val;
                    CHECK(checked(realm_set_get(bars.get(), 0, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == class_bar.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    auto result = realm_set_get(bars.get(), 1, &val);
                    CHECK(!result);
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("insert wrong type") {
                    write([&]() {
                        auto foo2 = cptr(realm_object_create(realm, class_foo.key));
                        CHECK(foo2);
                        realm_value_t foo2_link_val;
                        foo2_link_val.type = RLM_TYPE_LINK;
                        foo2_link_val.link = realm_object_as_link(foo2.get());

                        CHECK(!realm_set_insert(bars.get(), foo2_link_val, nullptr, nullptr));
                        CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);
                    });
                }

                SECTION("realm_set_clear()") {
                    write([&]() {
                        CHECK(realm_set_clear(bars.get()));
                    });
                    size_t size;
                    CHECK(realm_set_size(bars.get(), &size));
                    CHECK(size == 0);

                    size_t num_bars;
                    CHECK(realm_get_num_objects(realm, class_bar.key, &num_bars));
                    CHECK(num_bars != 0);
                }

                SECTION("realm_set_remove_all()") {
                    realm_value_t val;
                    CHECK(checked(realm_set_get(bars.get(), 0, &val)));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == class_bar.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    size_t num_bars;
                    size_t size;

                    write([&]() {
                        CHECK(checked(realm_set_remove_all(bars.get())));
                    });

                    CHECK(realm_set_size(bars.get(), &size));
                    CHECK(size == 0);

                    CHECK(realm_get_num_objects(realm, class_bar.key, &num_bars));
                    CHECK(num_bars == 0);
                }
            }

            SECTION("notifications") {
                struct State {
                    CPtr<realm_collection_changes_t> changes;
                    CPtr<realm_async_error_t> error;
                    bool destroyed = false;
                };

                State state;

                auto on_change = [](void* userdata, const realm_collection_changes_t* changes) {
                    auto* state = static_cast<State*>(userdata);
                    state->changes = clone_cptr(changes);
                };

                auto on_error = [](void* userdata, const realm_async_error_t* err) {
                    auto* state = static_cast<State*>(userdata);
                    state->error = clone_cptr(err);
                };

                CPtr<realm_set_t> strings =
                    cptr_checked(realm_get_set(obj1.get(), foo_properties["nullable_string_set"]));

                auto str1 = rlm_str_val("a");
                auto str2 = rlm_str_val("b");
                auto null = rlm_null();

                auto require_change = [&]() {
                    auto token = cptr_checked(realm_set_add_notification_callback(
                        strings.get(), &state, nullptr, nullptr, on_change, on_error, nullptr));
                    checked(realm_refresh(realm));
                    return token;
                };

                SECTION("userdata is freed when the token is destroyed") {
                    auto token = cptr_checked(realm_set_add_notification_callback(
                        strings.get(), &state,
                        [](void* p) {
                            static_cast<State*>(p)->destroyed = true;
                        },
                        nullptr, nullptr, nullptr, nullptr));
                    CHECK(!state.destroyed);
                    token.reset();
                    CHECK(state.destroyed);
                }

                SECTION("insertion,deletion sends a change callback") {
                    write([&]() {
                        checked(realm_set_insert(strings.get(), str1, nullptr, nullptr));
                    });

                    auto token = require_change();
                    write([&]() {
                        checked(realm_set_erase(strings.get(), str1, nullptr));
                        checked(realm_set_insert(strings.get(), str2, nullptr, nullptr));
                        checked(realm_set_insert(strings.get(), null, nullptr, nullptr));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 1);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 0);
                    CHECK(num_moves == 0);

                    realm_index_range_t insertion_range, deletion_range;
                    realm_collection_changes_get_ranges(state.changes.get(), &deletion_range, 1, &insertion_range, 1,
                                                        nullptr, 0, nullptr, 0, nullptr, 0);
                    CHECK(deletion_range.from == 0);
                    CHECK(deletion_range.to == 1);
                    CHECK(insertion_range.from == 0);
                    CHECK(insertion_range.to == 2);
                }
            }
        }

        SECTION("dictionaries") {
            SECTION("realm_get_dictionary() errors") {
                CHECK(!realm_get_dictionary(obj1.get(), foo_properties["int"]));
                CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);

                CHECK(!realm_get_dictionary(obj1.get(), 123123123));
                CHECK_ERR(RLM_ERR_INVALID_PROPERTY);
            }

            SECTION("nullable strings") {
                auto strings = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_string_dict"]));
                CHECK(strings);
                CHECK(!realm_is_frozen(strings.get()));

                realm_value_t a = rlm_str_val("a");
                realm_value_t b = rlm_str_val("b");
                realm_value_t c = rlm_null();
                realm_value_t key_a = rlm_str_val("key_a");
                realm_value_t key_b = rlm_str_val("key_b");
                realm_value_t key_c = rlm_str_val("key_c");

                SECTION("realm_equals() type check") {
                    CHECK(!realm_equals(strings.get(), obj1.get()));
                }

                SECTION("realm_clone()") {
                    auto dict2 = clone_cptr(strings.get());
                    CHECK(realm_equals(strings.get(), dict2.get()));
                    CHECK(strings.get() != dict2.get());
                }

                SECTION("insert, then get, then erase") {
                    write([&]() {
                        bool inserted = false;
                        CHECK(checked(realm_dictionary_insert(strings.get(), key_a, a, nullptr, &inserted)));
                        CHECK(inserted);
                        CHECK(checked(realm_dictionary_insert(strings.get(), key_b, b, nullptr, &inserted)));
                        CHECK(inserted);
                        CHECK(checked(realm_dictionary_insert(strings.get(), key_c, c, nullptr, &inserted)));
                        CHECK(inserted);

                        realm_value_t a2, b2, c2;
                        bool found = false;
                        CHECK(checked(realm_dictionary_find(strings.get(), key_a, &a2, &found)));
                        CHECK(found);
                        CHECK(checked(realm_dictionary_find(strings.get(), key_b, &b2, &found)));
                        CHECK(found);
                        CHECK(checked(realm_dictionary_find(strings.get(), key_c, &c2, &found)));
                        CHECK(found);

                        CHECK(rlm_stdstr(a2) == "a");
                        CHECK(rlm_stdstr(b2) == "b");
                        CHECK(c2.type == RLM_TYPE_NULL);

                        bool erased = false;
                        CHECK(checked(realm_dictionary_erase(strings.get(), key_a, &erased)));
                        CHECK(erased);
                        CHECK(checked(realm_dictionary_erase(strings.get(), rlm_int_val(987), &erased)));
                        CHECK(!erased);
                    });
                }

                SECTION("equality") {
                    auto strings2 =
                        cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_string_dict"]));
                    CHECK(strings2);
                    CHECK(realm_equals(strings.get(), strings2.get()));

                    write([&]() {
                        auto obj3 = cptr_checked(realm_object_create(realm, class_foo.key));
                        CHECK(obj3);
                        auto strings3 =
                            cptr_checked(realm_get_dictionary(obj3.get(), foo_properties["nullable_string_dict"]));
                        CHECK(!realm_equals(strings.get(), strings3.get()));
                    });
                }
            }

            SECTION("get/insert all property types") {
                realm_value_t key = rlm_str_val("k");
                realm_value_t key2 = rlm_str_val("k2");

                realm_value_t null = rlm_null();
                realm_value_t integer = rlm_int_val(987);
                realm_value_t boolean = rlm_bool_val(true);
                realm_value_t string = rlm_str_val("My string");
                const uint8_t binary_data[] = {0, 1, 2, 3, 4, 5, 6, 7};
                realm_value_t binary = rlm_binary_val(binary_data, sizeof(binary_data));
                realm_value_t timestamp = rlm_timestamp_val(1000000, 123123123);
                realm_value_t fnum = rlm_float_val(123.f);
                realm_value_t dnum = rlm_double_val(456.0);
                realm_value_t decimal = rlm_decimal_val(999.0);
                realm_value_t object_id = rlm_object_id_val("abc123abc123");
                realm_value_t uuid = rlm_uuid_val("01234567-9abc-4def-9012-3456789abcde");

                auto int_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["int_dict"]));
                auto bool_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["bool_dict"]));
                auto string_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["string_dict"]));
                auto binary_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["binary_dict"]));
                auto timestamp_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["timestamp_dict"]));
                auto float_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["float_dict"]));
                auto double_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["double_dict"]));
                auto decimal_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["decimal_dict"]));
                auto object_id_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["object_id_dict"]));
                auto uuid_dict = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["uuid_dict"]));
                auto nullable_int_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_int_dict"]));
                auto nullable_bool_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_bool_dict"]));
                auto nullable_string_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_string_dict"]));
                auto nullable_binary_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_binary_dict"]));
                auto nullable_timestamp_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_timestamp_dict"]));
                auto nullable_float_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_float_dict"]));
                auto nullable_double_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_double_dict"]));
                auto nullable_decimal_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_decimal_dict"]));
                auto nullable_object_id_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_object_id_dict"]));
                auto nullable_uuid_dict =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_uuid_dict"]));

                write([&]() {
                    size_t index;
                    bool inserted;
                    CHECK(!realm_dictionary_insert(int_dict.get(), rlm_int_val(987), integer, &index, &inserted));

                    CHECK(realm_dictionary_insert(int_dict.get(), key, integer, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(bool_dict.get(), key, boolean, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(string_dict.get(), key, string, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(binary_dict.get(), key, binary, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(timestamp_dict.get(), key, timestamp, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(float_dict.get(), key, fnum, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(double_dict.get(), key, dnum, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(decimal_dict.get(), key, decimal, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(object_id_dict.get(), key, object_id, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(uuid_dict.get(), key, uuid, &index, &inserted));
                    CHECK((inserted && index == 0));

                    CHECK(realm_dictionary_insert(nullable_int_dict.get(), key, integer, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_bool_dict.get(), key, boolean, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_string_dict.get(), key, string, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_binary_dict.get(), key, binary, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_timestamp_dict.get(), key, timestamp, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_float_dict.get(), key, fnum, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_double_dict.get(), key, dnum, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_decimal_dict.get(), key, decimal, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_object_id_dict.get(), key, object_id, &index, &inserted));
                    CHECK((inserted && index == 0));
                    CHECK(realm_dictionary_insert(nullable_uuid_dict.get(), key, uuid, &index, &inserted));
                    CHECK((inserted && index == 0));

                    CHECK(realm_dictionary_insert(nullable_int_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_bool_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_string_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_binary_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_timestamp_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_float_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_double_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_decimal_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_object_id_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                    CHECK(realm_dictionary_insert(nullable_uuid_dict.get(), key2, null, &index, &inserted));
                    CHECK(inserted);
                });

                realm_value_t k, value;

                CHECK(realm_dictionary_get(int_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_dictionary_get(bool_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_dictionary_get(string_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_dictionary_get(binary_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_dictionary_get(timestamp_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_dictionary_get(float_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_dictionary_get(double_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_dictionary_get(decimal_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_dictionary_get(object_id_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_dictionary_get(uuid_dict.get(), 0, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, uuid));
                CHECK(realm_dictionary_get(nullable_int_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_dictionary_get(nullable_bool_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_dictionary_get(nullable_string_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_dictionary_get(nullable_binary_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_dictionary_get(nullable_timestamp_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_dictionary_get(nullable_float_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_dictionary_get(nullable_double_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_dictionary_get(nullable_decimal_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_dictionary_get(nullable_object_id_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_dictionary_get(nullable_uuid_dict.get(), 1, &k, &value));
                CHECK(rlm_val_eq(k, key));
                CHECK(rlm_val_eq(value, uuid));

                write([&]() {
                    size_t index;
                    bool inserted;
                    CHECK(realm_dictionary_insert(nullable_int_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_bool_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_string_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_binary_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_timestamp_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_float_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_double_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_decimal_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_object_id_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                    CHECK(realm_dictionary_insert(nullable_uuid_dict.get(), key2, null, &index, &inserted));
                    CHECK(!inserted);
                });

                CHECK(realm_dictionary_find(int_dict.get(), rlm_int_val(987), &value, &found));
                CHECK(!found);
                CHECK(realm_dictionary_find(int_dict.get(), rlm_str_val("Boogeyman"), &value, &found));
                CHECK(!found);
                CHECK(realm_dictionary_find(int_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_dictionary_find(bool_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_dictionary_find(string_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_dictionary_find(binary_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_dictionary_find(timestamp_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_dictionary_find(float_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_dictionary_find(double_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_dictionary_find(decimal_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_dictionary_find(object_id_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_dictionary_find(uuid_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, uuid));
                CHECK(realm_dictionary_find(nullable_int_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, integer));
                CHECK(realm_dictionary_find(nullable_bool_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, boolean));
                CHECK(realm_dictionary_find(nullable_string_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, string));
                CHECK(realm_dictionary_find(nullable_binary_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, binary));
                CHECK(realm_dictionary_find(nullable_timestamp_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, timestamp));
                CHECK(realm_dictionary_find(nullable_float_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, fnum));
                CHECK(realm_dictionary_find(nullable_double_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, dnum));
                CHECK(realm_dictionary_find(nullable_decimal_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, decimal));
                CHECK(realm_dictionary_find(nullable_object_id_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, object_id));
                CHECK(realm_dictionary_find(nullable_uuid_dict.get(), key, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, uuid));

                CHECK(realm_dictionary_find(nullable_int_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_bool_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_string_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_binary_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_timestamp_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_float_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_double_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_decimal_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_object_id_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
                CHECK(realm_dictionary_find(nullable_uuid_dict.get(), key2, &value, &found));
                CHECK(found);
                CHECK(rlm_val_eq(value, null));
            }

            SECTION("links") {
                CPtr<realm_dictionary_t> bars;
                realm_value_t key = rlm_str_val("k");

                write([&]() {
                    bars = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["link_dict"]));
                    auto bar_link = realm_object_as_link(obj2.get());
                    realm_value_t bar_link_val;
                    bar_link_val.type = RLM_TYPE_LINK;
                    bar_link_val.link = bar_link;
                    size_t index;
                    bool inserted;
                    CHECK(checked(realm_dictionary_insert(bars.get(), key, bar_link_val, &index, &inserted)));
                    CHECK((index == 0 && inserted));
                    CHECK(checked(realm_dictionary_insert(bars.get(), key, bar_link_val, &index, &inserted)));
                    CHECK((index == 0 && !inserted));
                    size_t size;
                    CHECK(checked(realm_dictionary_size(bars.get(), &size)));
                    CHECK(size == 1);
                });

                SECTION("get") {
                    realm_value_t k, val;
                    CHECK(checked(realm_dictionary_get(bars.get(), 0, &k, &val)));
                    CHECK(rlm_val_eq(k, key));
                    CHECK(val.type == RLM_TYPE_LINK);
                    CHECK(val.link.target_table == class_bar.key);
                    CHECK(val.link.target == realm_object_get_key(obj2.get()));

                    auto result = realm_dictionary_get(bars.get(), 1, &k, &val);
                    CHECK(!result);
                    CHECK_ERR(RLM_ERR_INDEX_OUT_OF_BOUNDS);
                }

                SECTION("insert wrong type") {
                    write([&]() {
                        auto foo2 = cptr(realm_object_create(realm, class_foo.key));
                        CHECK(foo2);
                        realm_value_t foo2_link_val;
                        foo2_link_val.type = RLM_TYPE_LINK;
                        foo2_link_val.link = realm_object_as_link(foo2.get());

                        CHECK(!realm_dictionary_insert(bars.get(), key, foo2_link_val, nullptr, nullptr));
                        CHECK_ERR(RLM_ERR_PROPERTY_TYPE_MISMATCH);
                    });
                }

                SECTION("realm_dictionary_clear()") {
                    write([&]() {
                        CHECK(realm_dictionary_clear(bars.get()));
                    });
                    size_t size;
                    CHECK(realm_dictionary_size(bars.get(), &size));
                    CHECK(size == 0);

                    size_t num_bars;
                    CHECK(realm_get_num_objects(realm, class_bar.key, &num_bars));
                    CHECK(num_bars != 0);
                }
            }

            SECTION("notifications") {
                struct State {
                    CPtr<realm_collection_changes_t> changes;
                    CPtr<realm_async_error_t> error;
                    bool destroyed = false;
                };

                State state;

                auto on_change = [](void* userdata, const realm_collection_changes_t* changes) {
                    auto* state = static_cast<State*>(userdata);
                    state->changes = clone_cptr(changes);
                };

                auto on_error = [](void* userdata, const realm_async_error_t* err) {
                    auto* state = static_cast<State*>(userdata);
                    state->error = clone_cptr(err);
                };

                CPtr<realm_dictionary_t> strings =
                    cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["nullable_string_dict"]));

                auto str1 = rlm_str_val("a");
                auto str2 = rlm_str_val("b");
                auto null = rlm_null();

                auto require_change = [&]() {
                    auto token = cptr_checked(realm_dictionary_add_notification_callback(
                        strings.get(), &state, nullptr, nullptr, on_change, on_error, nullptr));
                    checked(realm_refresh(realm));
                    return token;
                };

                SECTION("userdata is freed when the token is destroyed") {
                    auto token = cptr_checked(realm_dictionary_add_notification_callback(
                        strings.get(), &state,
                        [](void* p) {
                            static_cast<State*>(p)->destroyed = true;
                        },
                        nullptr, nullptr, nullptr, nullptr));
                    CHECK(!state.destroyed);
                    token.reset();
                    CHECK(state.destroyed);
                }

                SECTION("insertion, deletions sends a change callback") {
                    write([&]() {
                        checked(realm_dictionary_insert(strings.get(), rlm_str_val("a"), str1, nullptr, nullptr));
                    });
                    auto token = require_change();
                    write([&]() {
                        checked(realm_dictionary_erase(strings.get(), rlm_str_val("a"), nullptr));
                        checked(realm_dictionary_insert(strings.get(), rlm_str_val("b"), str2, nullptr, nullptr));
                        checked(realm_dictionary_insert(strings.get(), rlm_str_val("c"), null, nullptr, nullptr));
                    });
                    CHECK(!state.error);
                    CHECK(state.changes);

                    size_t num_deletion_ranges, num_insertion_ranges, num_modification_ranges, num_moves;
                    realm_collection_changes_get_num_ranges(state.changes.get(), &num_deletion_ranges,
                                                            &num_insertion_ranges, &num_modification_ranges,
                                                            &num_moves);
                    CHECK(num_deletion_ranges == 1);
                    CHECK(num_insertion_ranges == 1);
                    CHECK(num_modification_ranges == 0);
                    CHECK(num_moves == 0);

                    realm_index_range_t deletion_range, insertion_range;
                    realm_collection_changes_get_ranges(state.changes.get(), &deletion_range, 1, &insertion_range, 1,
                                                        nullptr, 0, nullptr, 0, nullptr, 0);
                    CHECK(deletion_range.from == 0);
                    CHECK(deletion_range.to == 1);
                    CHECK(insertion_range.from == 0);
                    CHECK(insertion_range.to == 2);
                }
            }
        }

        SECTION("notifications") {
            struct State {
                CPtr<realm_object_changes_t> changes;
                CPtr<realm_async_error_t> error;
                bool called;
            };

            State state;

            auto on_change = [](void* userdata, const realm_object_changes_t* changes) {
                auto state = static_cast<State*>(userdata);
                state->changes = clone_cptr(changes);
                state->called = true;
            };

            auto on_error = [](void* userdata, const realm_async_error_t* err) {
                auto state = static_cast<State*>(userdata);
                state->error = clone_cptr(err);
            };

            auto require_change = [&]() {
                auto token = cptr(realm_object_add_notification_callback(obj1.get(), &state, nullptr, nullptr,
                                                                         on_change, on_error, nullptr));
                checked(realm_refresh(realm));
                return token;
            };

            SECTION("deleting the object sends a change notification") {
                auto token = require_change();
                write([&]() {
                    checked(realm_object_delete(obj1.get()));
                });
                CHECK(!state.error);
                CHECK(state.changes);
                bool deleted = realm_object_changes_is_deleted(state.changes.get());
                CHECK(deleted);
            }

            SECTION("modifying the object sends a change notification for the object, and for the changed column") {
                auto token = require_change();
                write([&]() {
                    checked(realm_set_value(obj1.get(), foo_int_key, rlm_int_val(999), false));
                    checked(realm_set_value(obj1.get(), foo_str_key, rlm_str_val("aaa"), false));
                });
                CHECK(!state.error);
                CHECK(state.changes);
                bool deleted = realm_object_changes_is_deleted(state.changes.get());
                CHECK(!deleted);
                size_t num_modified = realm_object_changes_get_num_modified_properties(state.changes.get());
                CHECK(num_modified == 2);
                realm_property_key_t modified_keys[2];
                size_t n = realm_object_changes_get_modified_properties(state.changes.get(), modified_keys, 2);
                CHECK(n == 2);
                CHECK(modified_keys[0] == foo_int_key);
                CHECK(modified_keys[1] == foo_str_key);

                n = realm_object_changes_get_modified_properties(state.changes.get(), nullptr, 2);
                CHECK(n == 2);

                n = realm_object_changes_get_modified_properties(state.changes.get(), modified_keys, 0);
                CHECK(n == 0);
            }
            SECTION("modifying the object while observing a specific value") {
                realm_key_path_elem_t origin_value[] = {{class_foo.key, foo_int_key}};
                realm_key_path_t key_path_origin_value[] = {{1, origin_value}};
                realm_key_path_array_t key_path_array = {1, key_path_origin_value};
                auto token = cptr(realm_object_add_notification_callback(obj1.get(), &state, nullptr, &key_path_array,
                                                                         on_change, on_error, nullptr));
                checked(realm_refresh(realm));
                state.called = false;
                write([&]() {
                    checked(realm_set_value(obj1.get(), foo_int_key, rlm_int_val(999), false));
                });
                REQUIRE(state.called);
                CHECK(!state.error);
                CHECK(state.changes);
                realm_property_key_t modified_keys[2];
                size_t n = realm_object_changes_get_modified_properties(state.changes.get(), modified_keys, 2);
                CHECK(n == 1);
                CHECK(modified_keys[0] == foo_int_key);

                state.called = false;
                write([&]() {
                    // checked(realm_set_value(obj1.get(), foo_int_key, rlm_int_val(999), false));
                    checked(realm_set_value(obj1.get(), foo_str_key, rlm_str_val("aaa"), false));
                });
                REQUIRE(!state.called);
            }
        }
    }

    SECTION("threads") {
        CPtr<realm_object_t> foo_obj, bar_obj;
        write([&]() {
            foo_obj = cptr_checked(realm_object_create(realm, class_foo.key));
            realm_set_value(foo_obj.get(), foo_int_key, rlm_int_val(123), false);
            bar_obj = cptr_checked(realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(123)));
        });

        auto list = cptr_checked(realm_get_list(foo_obj.get(), foo_properties["int_list"]));
        auto set = cptr_checked(realm_get_set(foo_obj.get(), foo_properties["int_set"]));
        auto dictionary = cptr_checked(realm_get_dictionary(foo_obj.get(), foo_properties["int_dict"]));
        auto results = cptr_checked(realm_object_find_all(realm, class_foo.key));

        SECTION("wrong thread") {
            std::thread t{[&]() {
                realm_value_t val;
                CHECK(!realm_get_value(foo_obj.get(), foo_int_key, &val));
                CHECK_ERR(RLM_ERR_WRONG_THREAD);
            }};

            t.join();
        }

        SECTION("thread-safe references") {
            auto foo_obj_tsr = cptr_checked(realm_create_thread_safe_reference(foo_obj.get()));
            auto bar_obj_tsr = cptr_checked(realm_create_thread_safe_reference(bar_obj.get()));
            auto list_tsr = cptr_checked(realm_create_thread_safe_reference(list.get()));
            auto set_tsr = cptr_checked(realm_create_thread_safe_reference(set.get()));
            auto dict_tsr = cptr_checked(realm_create_thread_safe_reference(dictionary.get()));
            auto results_tsr = cptr_checked(realm_create_thread_safe_reference(results.get()));

            SECTION("resolve") {
                std::thread t{[&]() {
                    auto config = make_config(test_file.path.c_str());
                    auto realm2 = cptr_checked(realm_open(config.get()));
                    auto foo_obj2 =
                        cptr_checked(realm_object_from_thread_safe_reference(realm2.get(), foo_obj_tsr.get()));
                    auto bar_obj2 =
                        cptr_checked(realm_object_from_thread_safe_reference(realm2.get(), bar_obj_tsr.get()));
                    auto results2 =
                        cptr_checked(realm_results_from_thread_safe_reference(realm2.get(), results_tsr.get()));
                    auto list2 = cptr_checked(realm_list_from_thread_safe_reference(realm2.get(), list_tsr.get()));
                    auto set2 = cptr_checked(realm_set_from_thread_safe_reference(realm2.get(), set_tsr.get()));
                    auto dict2 =
                        cptr_checked(realm_dictionary_from_thread_safe_reference(realm2.get(), dict_tsr.get()));

                    realm_value_t foo_obj_int;
                    CHECK(realm_get_value(foo_obj2.get(), foo_int_key, &foo_obj_int));
                    CHECK(rlm_val_eq(foo_obj_int, rlm_int_val(123)));

                    size_t count;
                    CHECK(realm_results_count(results2.get(), &count));
                    CHECK(count == 1);
                }};

                t.join();
            }

            SECTION("resolve in frozen") {
                auto realm2 = cptr_checked(realm_freeze(realm));
                CHECK(realm_is_frozen(realm2.get()));
                CHECK(realm != realm2.get());

                auto foo_obj2 =
                    cptr_checked(realm_object_from_thread_safe_reference(realm2.get(), foo_obj_tsr.get()));
                CHECK(realm_is_frozen(foo_obj2.get()));
            }

            SECTION("type error") {
                CHECK(!realm_object_from_thread_safe_reference(realm, list_tsr.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
                CHECK(!realm_list_from_thread_safe_reference(realm, foo_obj_tsr.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
                CHECK(!realm_set_from_thread_safe_reference(realm, list_tsr.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
                CHECK(!realm_dictionary_from_thread_safe_reference(realm, set_tsr.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
                CHECK(!realm_results_from_thread_safe_reference(realm, list_tsr.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
                CHECK(!realm_from_thread_safe_reference(list_tsr.get(), nullptr));
                CHECK_ERR(RLM_ERR_LOGIC);
            }

            SECTION("non-sendable") {
                auto c = cptr(realm_config_new());
                CHECK(!realm_create_thread_safe_reference(c.get()));
                CHECK_ERR(RLM_ERR_LOGIC);
            }
        }
    }

    SECTION("freeze and thaw") {
        SECTION("realm") {
            auto frozen_realm = cptr_checked(realm_freeze(realm));
            CHECK(!realm_is_frozen(realm));
            CHECK(realm_is_frozen(frozen_realm.get()));
        }

        SECTION("objects") {
            CPtr<realm_object_t> obj1;
            realm_value_t value;

            write([&]() {
                obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
                CHECK(obj1);
            });
            CHECK(checked(realm_get_value(obj1.get(), foo_str_key, &value)));
            CHECK(value.type == RLM_TYPE_STRING);
            CHECK(strncmp(value.string.data, "", value.string.size) == 0);

            auto frozen_realm = cptr_checked(realm_freeze(realm));
            realm_object_t* frozen_obj1;
            CHECK(realm_object_resolve_in(obj1.get(), frozen_realm.get(), &frozen_obj1));

            write([&]() {
                CHECK(checked(realm_set_value(obj1.get(), foo_str_key, rlm_str_val("Hello, World!"), false)));
            });

            CHECK(checked(realm_get_value(obj1.get(), foo_str_key, &value)));
            CHECK(value.type == RLM_TYPE_STRING);
            CHECK(strncmp(value.string.data, "Hello, World!", value.string.size) == 0);

            CHECK(checked(realm_get_value(frozen_obj1, foo_str_key, &value)));
            CHECK(value.type == RLM_TYPE_STRING);
            CHECK(strncmp(value.string.data, "", value.string.size) == 0);
            realm_object_t* thawed_obj1;
            CHECK(realm_object_resolve_in(obj1.get(), realm, &thawed_obj1));
            CHECK(thawed_obj1);
            CHECK(checked(realm_get_value(thawed_obj1, foo_str_key, &value)));
            CHECK(value.type == RLM_TYPE_STRING);
            CHECK(strncmp(value.string.data, "Hello, World!", value.string.size) == 0);

            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            realm_object_t* deleted_obj;
            auto b = realm_object_resolve_in(frozen_obj1, realm, &deleted_obj);
            CHECK(b);
            CHECK(deleted_obj == nullptr);
            realm_release(frozen_obj1);
            realm_release(thawed_obj1);
        }

        SECTION("results") {
            auto results = cptr_checked(realm_object_find_all(realm, class_foo.key));
            realm_results_delete_all(results.get());

            write([&]() {
                // Ensure that we start from a known initial state
                CHECK(realm_results_delete_all(results.get()));

                auto obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
                CHECK(obj1);
            });

            size_t count;
            realm_results_count(results.get(), &count);
            CHECK(count == 1);

            auto frozen_realm = cptr_checked(realm_freeze(realm));
            auto frozen_results = cptr_checked(realm_results_resolve_in(results.get(), frozen_realm.get()));
            write([&]() {
                auto obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
                CHECK(obj1);
            });
            realm_results_count(frozen_results.get(), &count);
            CHECK(count == 1);
            realm_results_count(results.get(), &count);
            CHECK(count == 2);

            auto thawed_results = cptr_checked(realm_results_resolve_in(frozen_results.get(), realm));
            realm_results_count(thawed_results.get(), &count);
            CHECK(count == 2);
        }

        SECTION("lists") {
            CPtr<realm_object_t> obj1;
            size_t count;

            write([&]() {
                obj1 = cptr_checked(realm_object_create_with_primary_key(realm, class_bar.key, rlm_int_val(1)));
                CHECK(obj1);
            });

            auto list = cptr_checked(realm_get_list(obj1.get(), bar_properties["strings"]));
            realm_list_size(list.get(), &count);
            CHECK(count == 0);

            auto frozen_realm = cptr_checked(realm_freeze(realm));
            realm_list_t* frozen_list;
            CHECK(realm_list_resolve_in(list.get(), frozen_realm.get(), &frozen_list));
            realm_list_size(frozen_list, &count);
            CHECK(count == 0);

            write([&]() {
                checked(realm_list_insert(list.get(), 0, rlm_str_val("Hello")));
            });

            realm_list_size(frozen_list, &count);
            CHECK(count == 0);
            realm_list_size(list.get(), &count);
            CHECK(count == 1);

            realm_list_t* thawed_list;
            CHECK(realm_list_resolve_in(frozen_list, realm, &thawed_list));
            realm_list_size(thawed_list, &count);
            CHECK(count == 1);

            CHECK(realm_list_is_valid(thawed_list));
            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            CHECK(!realm_list_is_valid(thawed_list));
            realm_release(thawed_list);
            CHECK(realm_list_resolve_in(frozen_list, realm, &thawed_list));
            CHECK(thawed_list == nullptr);
            realm_release(frozen_list);
        }

        SECTION("sets") {
            CPtr<realm_object_t> obj1;
            size_t count;

            write([&]() {
                obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
                CHECK(obj1);
            });

            auto set = cptr_checked(realm_get_set(obj1.get(), foo_properties["string_set"]));
            realm_set_size(set.get(), &count);
            CHECK(count == 0);

            auto frozen_realm = cptr_checked(realm_freeze(realm));
            realm_set_t* frozen_set;
            CHECK(realm_set_resolve_in(set.get(), frozen_realm.get(), &frozen_set));
            realm_set_size(frozen_set, &count);
            CHECK(count == 0);

            write([&]() {
                checked(realm_set_insert(set.get(), rlm_str_val("Hello"), nullptr, nullptr));
            });

            realm_set_size(frozen_set, &count);
            CHECK(count == 0);
            realm_set_size(set.get(), &count);
            CHECK(count == 1);

            realm_set_t* thawed_set;
            CHECK(realm_set_resolve_in(frozen_set, realm, &thawed_set));
            realm_set_size(thawed_set, &count);
            CHECK(count == 1);

            CHECK(realm_set_is_valid(thawed_set));
            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            CHECK(!realm_set_is_valid(thawed_set));
            realm_release(thawed_set);
            CHECK(realm_set_resolve_in(frozen_set, realm, &thawed_set));
            CHECK(thawed_set == nullptr);
            realm_release(frozen_set);
        }

        SECTION("dictionaries") {
            CPtr<realm_object_t> obj1;
            size_t count;

            write([&]() {
                obj1 = cptr_checked(realm_object_create(realm, class_foo.key));
                CHECK(obj1);
            });

            auto dictionary = cptr_checked(realm_get_dictionary(obj1.get(), foo_properties["string_dict"]));
            realm_dictionary_size(dictionary.get(), &count);
            CHECK(count == 0);

            auto frozen_realm = cptr_checked(realm_freeze(realm));
            realm_dictionary_t* frozen_dictionary;
            CHECK(realm_dictionary_resolve_in(dictionary.get(), frozen_realm.get(), &frozen_dictionary));
            realm_dictionary_size(frozen_dictionary, &count);
            CHECK(count == 0);

            write([&]() {
                checked(realm_dictionary_insert(dictionary.get(), rlm_str_val("Hello"), rlm_str_val("world"), nullptr,
                                                nullptr));
            });

            realm_dictionary_size(frozen_dictionary, &count);
            CHECK(count == 0);
            realm_dictionary_size(dictionary.get(), &count);
            CHECK(count == 1);

            realm_dictionary_t* thawed_dictionary;
            CHECK(realm_dictionary_resolve_in(frozen_dictionary, realm, &thawed_dictionary));
            realm_dictionary_size(thawed_dictionary, &count);
            CHECK(count == 1);

            CHECK(realm_dictionary_is_valid(thawed_dictionary));
            write([&]() {
                CHECK(checked(realm_object_delete(obj1.get())));
            });
            CHECK(!realm_dictionary_is_valid(thawed_dictionary));
            realm_release(thawed_dictionary);
            CHECK(realm_dictionary_resolve_in(frozen_dictionary, realm, &thawed_dictionary));
            CHECK(thawed_dictionary == nullptr);
            realm_release(frozen_dictionary);
        }
    }

    realm_close(realm);
    REQUIRE(realm_is_closed(realm));
    realm_release(realm);
}

#ifdef REALM_ENABLE_AUTH_TESTS
TEST_CASE("app: flx-sync basic tests", "[c_api][flx][syc]") {
    using namespace realm::app;

    auto make_schema = []() -> auto
    {
        Schema schema{ObjectSchema("Obj", {{"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                                           {"name", PropertyType::String | PropertyType::Nullable},
                                           {"value", PropertyType::Int | PropertyType::Nullable}})};

        return FLXSyncTestHarness::ServerSchema{std::move(schema), {"name", "value"}};
    };

    FLXSyncTestHarness harness("c_api_flx_sync_test", make_schema());
    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();

    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        realm->begin_transaction();
        Object::create(c, realm, "Obj",
                       util::Any(AnyDict{
                           {"_id", foo_obj_id}, {"name", std::string{"foo"}}, {"value", static_cast<int64_t>(5)}}));
        Object::create(c, realm, "Obj",
                       util::Any(AnyDict{
                           {"_id", bar_obj_id}, {"name", std::string{"bar"}}, {"value", static_cast<int64_t>(10)}}));

        realm->commit_transaction();
        wait_for_upload(*realm);
    });

    harness.do_with_new_realm([&](SharedRealm realm) {
        realm_t c_wrap_realm(realm);

        wait_for_download(*realm);
        {
            auto empty_subs = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            CHECK(realm_sync_subscription_set_size(empty_subs) == 0);
            CHECK(realm_sync_subscription_set_version(empty_subs) == 0);
            realm_sync_on_subscription_set_state_change_wait(
                empty_subs, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(empty_subs);
        }
        realm_class_info_t table_info;
        bool found;
        CHECK(realm_find_class(&c_wrap_realm, "Obj", &found, &table_info));
        auto c_wrap_query_foo = realm_query_parse(&c_wrap_realm, table_info.key, "name = 'foo'", 0, nullptr);
        auto c_wrap_query_bar = realm_query_parse(&c_wrap_realm, table_info.key, "name = 'bar'", 0, nullptr);
        {
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            CHECK(sub != nullptr);
            auto new_subs = realm_sync_make_subscription_set_mutable(sub);
            std::size_t index = -1;
            bool inserted = false;
            auto res =
                realm_sync_subscription_set_insert_or_assign(new_subs, c_wrap_query_foo, nullptr, &index, &inserted);
            CHECK(inserted == true);
            CHECK(index == 0);
            CHECK(res);
            auto subs = realm_sync_subscription_set_commit(new_subs);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                subs, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(sub);
            realm_release(new_subs);
            realm_release(subs);
        }

        wait_for_download(*realm);
        {
            realm_refresh(&c_wrap_realm);
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = 0;
            realm_results_count(results, &count);
            CHECK(count == 1);
            auto object = realm_results_get_object(results, 0);
            REQUIRE(realm_object_is_valid(object));
            REQUIRE(object->get_column_value<ObjectId>("_id") == foo_obj_id);
            realm_release(object);
            realm_release(results);
        }

        {
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            std::size_t index = -1;
            bool inserted = false;
            realm_sync_subscription_set_insert_or_assign(mut_sub, c_wrap_query_bar, nullptr, &index, &inserted);
            CHECK(inserted);
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
        }

        {
            realm_refresh(&c_wrap_realm);
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = 0;
            realm_results_count(results, &count);
            CHECK(count == 2);
            realm_release(results);
        }

        {
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            auto s = realm_sync_find_subscription_by_query(sub, c_wrap_query_foo);
            CHECK(s != nullptr);
            auto erased = realm_sync_subscription_set_erase_by_query(mut_sub, c_wrap_query_foo);
            CHECK(erased);
            auto c_wrap_new_query_bar = realm_query_parse(&c_wrap_realm, table_info.key, "name = 'bar'", 0, nullptr);
            std::size_t index = -1;
            bool inserted = false;
            bool updated = realm_sync_subscription_set_insert_or_assign(mut_sub, c_wrap_new_query_bar, nullptr,
                                                                        &index, &inserted);
            CHECK(!inserted);
            CHECK(updated);
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(s);
            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
            realm_release(c_wrap_new_query_bar);
        }

        {
            realm_refresh(&c_wrap_realm);
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = 0;
            realm_results_count(results, &count);
            CHECK(count == 1);
            auto object = realm_results_get_object(results, 0);
            REQUIRE(realm_object_is_valid(object));
            REQUIRE(object->get_column_value<ObjectId>("_id") == bar_obj_id);
            realm_release(object);
            realm_release(results);
        }

        {
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            auto cleared = realm_sync_subscription_set_clear(mut_sub);
            CHECK(cleared);
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
        }

        {
            realm_refresh(&c_wrap_realm);
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = std::numeric_limits<std::size_t>::max();
            realm_results_count(results, &count);
            CHECK(count == 0);
            realm_release(results);
        }

        {
            auto c_wrap_new_query_bar = realm_query_parse(&c_wrap_realm, table_info.key, "name = 'bar'", 0, nullptr);
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            std::size_t index = -1;
            bool inserted = false;
            bool success =
                realm_sync_subscription_set_insert_or_assign(mut_sub, c_wrap_new_query_bar, "bar", &index, &inserted);
            CHECK(inserted);
            CHECK(success);
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
            realm_release(c_wrap_new_query_bar);
        }

        {
            realm->refresh();
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = std::numeric_limits<std::size_t>::max();
            realm_results_count(results, &count);
            CHECK(count == 1);
            realm_release(results);
        }

        {
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            realm_sync_subscription_set_erase_by_name(mut_sub, "bar");
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            auto state = realm_sync_on_subscription_set_state_change_wait(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            CHECK(state == realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);
            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
        }

        {
            realm_refresh(&c_wrap_realm);
            auto results = realm_object_find_all(&c_wrap_realm, table_info.key);
            size_t count = std::numeric_limits<std::size_t>::max();
            realm_results_count(results, &count);
            CHECK(count == 0);
            realm_release(results);
        }

        {
            auto test_query = realm_query_parse(&c_wrap_realm, table_info.key, "name = 'bar'", 0, nullptr);
            auto sub = realm_sync_get_latest_subscription_set(&c_wrap_realm);
            auto mut_sub = realm_sync_make_subscription_set_mutable(sub);
            std::size_t index = -1;
            bool inserted = false;
            bool success =
                realm_sync_subscription_set_insert_or_assign(mut_sub, c_wrap_query_bar, nullptr, &index, &inserted);
            CHECK(inserted);
            CHECK(success);
            auto sub_c = realm_sync_subscription_set_commit(mut_sub);
            // lambdas with state cannot be easily converted to function pointers, add a simple singleton that syncs
            // the state among threads
            struct SyncObject {
                std::mutex m_mutex;
                std::condition_variable m_cv;
                realm_flx_sync_subscription_set_state m_state{RLM_SYNC_SUBSCRIPTION_UNCOMMITTED};

                static SyncObject& create()
                {
                    static SyncObject sync_object;
                    return sync_object;
                }

                void set_state_and_notify(realm_flx_sync_subscription_set_state state)
                {
                    {
                        std::lock_guard<std::mutex> guard{m_mutex};
                        m_state = state;
                    }
                    m_cv.notify_one();
                }

                realm_flx_sync_subscription_set_state wait_state()
                {
                    using namespace std::chrono_literals;
                    std::unique_lock<std::mutex> lock{m_mutex};
                    m_cv.wait_for(lock, 300ms, [this]() {
                        return m_state == RLM_SYNC_SUBSCRIPTION_COMPLETE;
                    });
                    return m_state;
                }
            };

            auto callback = [](auto, realm_flx_sync_subscription_set_state_e sub_state) {
                SyncObject::create().set_state_and_notify(sub_state);
            };
            realm_sync_on_subscription_set_state_change_async(
                sub_c, realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE, callback);
            CHECK(SyncObject::create().wait_state() ==
                  realm_flx_sync_subscription_set_state_e::RLM_SYNC_SUBSCRIPTION_COMPLETE);

            realm_release(sub);
            realm_release(mut_sub);
            realm_release(sub_c);
            realm_release(test_query);
        }
        realm_release(c_wrap_query_foo);
        realm_release(c_wrap_query_bar);
    });
}
#endif // REALM_ENABLE_AUTH_TESTS
