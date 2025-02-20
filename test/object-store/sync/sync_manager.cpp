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

#include "sync_test_utils.hpp"

#include <realm/sync/config.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include "sync/session/session_util.hpp"
#include "util/event_loop.hpp"
#include "util/test_utils.hpp"

#include <realm/util/logger.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

static const std::string base_path =
    fs::path{util::make_temp_dir() + "/realm_objectstore_sync_manager"}.make_preferred().string();
static const std::string dummy_device_id = "123400000000000000000000";

namespace {
bool validate_user_in_vector(std::vector<std::shared_ptr<SyncUser>> vector, const std::string& identity,
                             const std::string& provider_type, const std::string& refresh_token,
                             const std::string& access_token, const std::string& device_id)
{
    for (auto& user : vector) {
        if (user->identity() == identity && user->refresh_token() == refresh_token &&
            provider_type == user->provider_type() && user->access_token() == access_token && user->has_device_id() &&
            user->device_id() == device_id) {
            return true;
        }
    }
    return false;
}
} // anonymous namespace

TEST_CASE("sync_manager: basic properties and APIs", "[sync]") {
    TestSyncManager init_sync_manager;
    auto app = init_sync_manager.app();

    SECTION("should work for log level") {
        app->sync_manager()->set_log_level(util::Logger::Level::info);
        REQUIRE(app->sync_manager()->log_level() == util::Logger::Level::info);
        app->sync_manager()->set_log_level(util::Logger::Level::error);
        REQUIRE(app->sync_manager()->log_level() == util::Logger::Level::error);
    }

    SECTION("should not crash on 'reconnect()'") {
        app->sync_manager()->reconnect();
    }
}

TEST_CASE("sync_manager: `path_for_realm` API", "[sync]") {
    const std::string auth_server_url = "https://realm.example.org";
    const std::string raw_url = "realms://realm.example.org/a/b/~/123456/xyz";
    using Cfg = TestSyncManager::Config;

    // Get a sync user
    TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
    auto sync_manager = init_sync_manager.app()->sync_manager();
    const std::string identity = "foobarbaz";
    auto user = sync_manager->get_user(identity, ENCODE_FAKE_JWT("dummy_token"), ENCODE_FAKE_JWT("not_a_real_token"),
                                       auth_server_url, dummy_device_id);
    auto server_identity = user->identity();
    REQUIRE(server_identity == identity);

    SECTION("should work properly without metadata") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const auto expected =
            fs::path{
                base_path +
                "/mongodb-realm/app_id/foobarbaz/realms%3A%2F%2Frealm.example.org%2Fa%2Fb%2F%7E%2F123456%2Fxyz.realm"}
                .make_preferred()
                .string();
        auto user = init_sync_manager.app()->sync_manager()->get_user(identity, ENCODE_FAKE_JWT("dummy_token"),
                                                                      ENCODE_FAKE_JWT("not_a_real_token"),
                                                                      auth_server_url, dummy_device_id);
        SyncConfig config(user, bson::Bson{});
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should work properly with metadata") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoEncryption));
        const auto expected = fs::path{base_path + "/mongodb-realm/app_id/" + server_identity +
                                       "/realms%3A%2F%2Frealm.example.org%2Fa%2Fb%2F%7E%2F123456%2Fxyz.realm"}
                                  .make_preferred()
                                  .string();
        SyncConfig config(user, bson::Bson{});
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config, raw_url) == expected);

        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(
            fs::path{base_path + "/mongodb-realm/app_id/" + server_identity}.make_preferred().string());
    }

    SECTION("should produce the expected path for a string partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition("string-partition-value&^#");
        SyncConfig config(user, partition);
        const auto expected =
            fs::path(base_path + "/mongodb-realm/app_id/foobarbaz/s_string-partition-value%26%5E%23.realm")
                .make_preferred()
                .string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should produce a hashed path for string partitions which exceed file system path length limits") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const std::string name_too_long(500, 'b');
        REQUIRE(name_too_long.length() == 500);
        const bson::Bson partition(name_too_long);
        SyncConfig config(user, partition);
        const std::string expected_prefix = fs::path{base_path + "/mongodb-realm/app_id"}.make_preferred().string();
        const std::string expected_suffix = ".realm";
        std::string actual = init_sync_manager.app()->sync_manager()->path_for_realm(config);
        size_t expected_length = expected_prefix.length() + 64 + expected_suffix.length();
        // std::fs does not include path terminator
        REQUIRE(actual.length() == 1 + expected_length);
        REQUIRE(actual.find(expected_prefix) == 0);
        REQUIRE(actual.find(expected_suffix) != std::string::npos);
    }

    SECTION("should produce the expected path for a int32 partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition(int32_t(-25));
        SyncConfig config(user, partition);
        const auto expected =
            fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/i_-25.realm"}.make_preferred().string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should produce the expected path for a int64 partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition(int64_t(1.15e18)); // > 32 bits
        SyncConfig config(user, partition);
        const auto expected = fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/l_1150000000000000000.realm"}
                                  .make_preferred()
                                  .string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should produce the expected path for a ObjectId partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition(ObjectId("0123456789abcdefffffffff"));
        SyncConfig config(user, partition);
        const auto expected = fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/o_0123456789abcdefffffffff.realm"}
                                  .make_preferred()
                                  .string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path(base_path + "/mongodb-realm/app_id/foobarbaz").make_preferred().string());
    }

    SECTION("should produce the expected path for a UUID partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition(UUID("3b241101-e2bb-4255-8caf-4136c566a961"));
        SyncConfig config(user, partition);
        const auto expected =
            fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/u_3b241101-e2bb-4255-8caf-4136c566a961.realm"}
                .make_preferred()
                .string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should produce the expected path for a Null partition") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        const bson::Bson partition;
        REQUIRE(partition.type() == bson::Bson::Type::Null);
        SyncConfig config(user, partition);
        const auto expected =
            fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/null.realm"}.make_preferred().string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }

    SECTION("should produce the FLX sync path when FLX sync is enabled") {
        TestSyncManager init_sync_manager(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
        SyncConfig config(user, SyncConfig::FLXSyncEnabled{});
        const auto expected =
            fs::path{base_path + "/mongodb-realm/app_id/foobarbaz/flx_sync_default.realm"}.make_preferred().string();
        REQUIRE(init_sync_manager.app()->sync_manager()->path_for_realm(config) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(fs::path{base_path + "/mongodb-realm/app_id/foobarbaz"}.make_preferred().string());
    }
}

TEST_CASE("sync_manager: user state management", "[sync]") {
    TestSyncManager init_sync_manager(TestSyncManager::Config(base_path, SyncManager::MetadataMode::NoEncryption));
    auto sync_manager = init_sync_manager.app()->sync_manager();

    const std::string url_1 = "https://realm.example.org/1/";
    const std::string url_2 = "https://realm.example.org/2/";
    const std::string url_3 = "https://realm.example.org/3/";

    const std::string r_token_1 = ENCODE_FAKE_JWT("foo_token");
    const std::string r_token_2 = ENCODE_FAKE_JWT("bar_token");
    const std::string r_token_3 = ENCODE_FAKE_JWT("baz_token");

    const std::string a_token_1 = ENCODE_FAKE_JWT("wibble");
    const std::string a_token_2 = ENCODE_FAKE_JWT("wobble");
    const std::string a_token_3 = ENCODE_FAKE_JWT("wubble");

    const std::string identity_1 = "user-foo";
    const std::string identity_2 = "user-bar";
    const std::string identity_3 = "user-baz";

    SECTION("should get all users that are created during run time") {
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id);
        sync_manager->get_user(identity_2, r_token_2, a_token_2, url_2, dummy_device_id);
        auto users = sync_manager->all_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2, dummy_device_id));
    }

    SECTION("should be able to distinguish users based solely on URL") {
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id);
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_2, dummy_device_id);
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_3, dummy_device_id);
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id); // existing
        auto users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_1, url_2, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_1, url_2, r_token_1, a_token_1, dummy_device_id));
    }

    SECTION("should be able to distinguish users based solely on user ID") {
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id);
        sync_manager->get_user(identity_2, r_token_1, a_token_1, url_1, dummy_device_id);
        sync_manager->get_user(identity_3, r_token_1, a_token_1, url_1, dummy_device_id);
        sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id); // existing
        auto users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_2, url_1, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_3, url_1, r_token_1, a_token_1, dummy_device_id));
    }

    SECTION("should properly update state in response to users logging in and out") {
        auto r_token_3a = ENCODE_FAKE_JWT("qwerty");
        auto a_token_3a = ENCODE_FAKE_JWT("ytrewq");

        auto u1 = sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id);
        auto u2 = sync_manager->get_user(identity_2, r_token_2, a_token_2, url_2, dummy_device_id);
        auto u3 = sync_manager->get_user(identity_3, r_token_3, a_token_3, url_3, dummy_device_id);
        auto users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3, a_token_3, dummy_device_id));
        // Log out users 1 and 3
        u1->log_out();
        u3->log_out();
        users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2, dummy_device_id));
        // Log user 3 back in
        u3 = sync_manager->get_user(identity_3, r_token_3a, a_token_3a, url_3, dummy_device_id);
        users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2, dummy_device_id));
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3a, a_token_3a, dummy_device_id));
        // Log user 2 out
        u2->log_out();
        users = sync_manager->all_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_3, url_3, r_token_3a, a_token_3a, dummy_device_id));
    }

    SECTION("should return current user that was created during run time") {
        auto u_null = sync_manager->get_current_user();
        REQUIRE(u_null == nullptr);

        auto u1 = sync_manager->get_user(identity_1, r_token_1, a_token_1, url_1, dummy_device_id);
        auto u_current = sync_manager->get_current_user();
        REQUIRE(u_current == u1);

        auto u2 = sync_manager->get_user(identity_2, r_token_2, a_token_2, url_2, dummy_device_id);
        // The current user has switched to return the most recently used: "u2"
        u_current = sync_manager->get_current_user();
        REQUIRE(u_current == u2);
    }
}

TEST_CASE("sync_manager: persistent user state management", "[sync]") {

    reset_test_directory(base_path);
    const std::string app_id = "test_app_id*$#@!%1";
    auto file_manager = SyncFileManager(base_path, app_id);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);
    using Cfg = TestSyncManager::Config;

    const std::string url_1 = "https://realm.example.org/1/";
    const std::string url_2 = "https://realm.example.org/2/";
    const std::string url_3 = "https://realm.example.org/3/";
    const std::string r_token_1 = ENCODE_FAKE_JWT("foo_token");
    const std::string r_token_2 = ENCODE_FAKE_JWT("bar_token");
    const std::string r_token_3 = ENCODE_FAKE_JWT("baz_token");
    const std::string a_token_1 = ENCODE_FAKE_JWT("wibble");
    const std::string a_token_2 = ENCODE_FAKE_JWT("wobble");
    const std::string a_token_3 = ENCODE_FAKE_JWT("wubble");

    SECTION("when users are persisted") {
        const std::string identity_1 = "foo-1";
        const std::string identity_2 = "bar-1";
        const std::string identity_3 = "baz-1";
        // First, create a few users and add them to the metadata.
        auto u1 = manager.get_or_make_user_metadata(identity_1, url_1);
        u1->set_access_token(a_token_1);
        u1->set_refresh_token(r_token_1);
        u1->set_device_id(dummy_device_id);
        auto u2 = manager.get_or_make_user_metadata(identity_2, url_2);
        u2->set_access_token(a_token_2);
        u2->set_refresh_token(r_token_2);
        u2->set_device_id(dummy_device_id);
        auto u3 = manager.get_or_make_user_metadata(identity_3, url_3);
        u3->set_access_token(a_token_3);
        u3->set_refresh_token(r_token_3);
        u3->set_device_id(dummy_device_id);
        // The fourth user is an "invalid" user: no token, so shouldn't show up.
        auto u_invalid = manager.get_or_make_user_metadata("invalid_user", url_1);
        REQUIRE(manager.all_unmarked_users().size() == 4);

        SECTION("they should be added to the active users list when metadata is enabled") {
            TestSyncManager tsm(Cfg(app_id, base_path, SyncManager::MetadataMode::NoEncryption));
            auto users = tsm.app()->sync_manager()->all_users();
            REQUIRE(users.size() == 3);
            REQUIRE(validate_user_in_vector(users, identity_1, url_1, r_token_1, a_token_1, dummy_device_id));
            REQUIRE(validate_user_in_vector(users, identity_2, url_2, r_token_2, a_token_2, dummy_device_id));
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, r_token_3, a_token_3, dummy_device_id));
        }

        SECTION("they should not be added to the active users list when metadata is disabled") {
            TestSyncManager tsm(Cfg(base_path, SyncManager::MetadataMode::NoMetadata));
            auto users = tsm.app()->sync_manager()->all_users();
            REQUIRE(users.size() == 0);
        }
    }

    const std::string expected_clean_app_id = "test_app_id%2A%24%23%40%21%251";
    const std::string manager_path =
        fs::path{base_path + "/mongodb-realm/" + expected_clean_app_id}.make_preferred().string();

    struct TestPath {
        std::string partition;
        std::string expected_path;
        bool pre_create = true;
    };
    std::vector<std::string> dirs_to_create;
    std::vector<TestPath> paths_under_test;

    SECTION("when users are marked") {
        const std::string provider_type = "user-pass";
        const std::string identity_1 = "foo-2";
        const std::string identity_2 = "bar-2";
        const std::string identity_3 = "baz-2";

        // Create the user metadata.
        auto u1 = manager.get_or_make_user_metadata(identity_1, provider_type);
        auto u2 = manager.get_or_make_user_metadata(identity_2, provider_type);
        // Don't mark this user for deletion.
        auto u3 = manager.get_or_make_user_metadata(identity_3, provider_type);

        {
            auto expected_u1_path = [&](std::string partition) {
                return ExpectedRealmPaths(base_path, app_id, u1->identity(), u1->local_uuid(), partition);
            };
            std::string partition = "partition1";
            auto expected_paths = expected_u1_path(partition);
            paths_under_test.push_back({partition, expected_paths.current_preferred_path, false});

            partition = "partition2";
            expected_paths = expected_u1_path(partition);
            paths_under_test.push_back({partition, expected_paths.current_preferred_path, true});

            partition = "partition3";
            expected_paths = expected_u1_path(partition);
            paths_under_test.push_back({partition, expected_paths.fallback_hashed_path});

            partition = "partition4";
            expected_paths = expected_u1_path(partition);
            paths_under_test.push_back({partition, expected_paths.legacy_local_id_path});
            dirs_to_create.insert(dirs_to_create.end(), expected_paths.legacy_sync_directories_to_make.begin(),
                                  expected_paths.legacy_sync_directories_to_make.end());

            partition = "partition5";
            expected_paths = expected_u1_path(partition);
            paths_under_test.push_back({partition, expected_paths.legacy_sync_path});
            dirs_to_create.insert(dirs_to_create.end(), expected_paths.legacy_sync_directories_to_make.begin(),
                                  expected_paths.legacy_sync_directories_to_make.end());
        }

        auto shared_test_config = Cfg(app_id, base_path, SyncManager::MetadataMode::NoEncryption);
        std::vector<std::string> paths;
        {
            shared_test_config.should_teardown_test_directory = false;
            TestSyncManager tsm(shared_test_config);

            // Pre-populate the user directories.
            auto user1 = tsm.app()->sync_manager()->get_user(u1->identity(), r_token_1, a_token_1,
                                                             u1->provider_type(), dummy_device_id);
            auto user2 = tsm.app()->sync_manager()->get_user(u2->identity(), r_token_2, a_token_2,
                                                             u2->provider_type(), dummy_device_id);
            auto user3 = tsm.app()->sync_manager()->get_user(u3->identity(), r_token_3, a_token_3,
                                                             u3->provider_type(), dummy_device_id);
            for (auto& dir : dirs_to_create) {
                try_make_dir(dir);
            }
            for (auto& test : paths_under_test) {
                if (test.pre_create) {
                    create_dummy_realm(test.expected_path);
                }
            }

            paths = {tsm.app()->sync_manager()->path_for_realm(SyncConfig{user1, "123456789"}),
                     tsm.app()->sync_manager()->path_for_realm(SyncConfig{user1, "foo"}),
                     tsm.app()->sync_manager()->path_for_realm(SyncConfig{user2, "partition"}, {"123456789"}),
                     tsm.app()->sync_manager()->path_for_realm(SyncConfig{user3, "foo"}),
                     tsm.app()->sync_manager()->path_for_realm(SyncConfig{user3, "bar"}),
                     tsm.app()->sync_manager()->path_for_realm(SyncConfig{user3, "baz"})};

            for (auto& test : paths_under_test) {
                std::string actual = tsm.app()->sync_manager()->path_for_realm(SyncConfig{user1, test.partition});
                REQUIRE(actual == test.expected_path);
                paths.push_back(actual);
            }

            for (auto& path : paths) {
                create_dummy_realm(path);
            }
            tsm.app()->sync_manager()->remove_user(u1->identity());
            tsm.app()->sync_manager()->remove_user(u2->identity());
        }
        for (auto& path : paths) {
            REQUIRE_REALM_EXISTS(path);
        }
        app::App::clear_cached_apps();

        SECTION("they should be cleaned up if metadata is enabled") {
            TestSyncManager tsm(shared_test_config);
            auto users = tsm.app()->sync_manager()->all_users();
            REQUIRE(users.size() == 1);
            REQUIRE(validate_user_in_vector(users, identity_3, provider_type, r_token_3, a_token_3, dummy_device_id));
            REQUIRE_REALM_DOES_NOT_EXIST(paths[0]);
            REQUIRE_REALM_DOES_NOT_EXIST(paths[1]);
            REQUIRE_REALM_DOES_NOT_EXIST(paths[2]);
            REQUIRE_REALM_EXISTS(paths[3]);
            REQUIRE_REALM_EXISTS(paths[4]);
            REQUIRE_REALM_EXISTS(paths[5]);
            // all the remaining user 1 realms should have been deleted
            for (size_t i = 6; i < paths.size(); ++i) {
                REQUIRE_REALM_DOES_NOT_EXIST(paths[i]);
            }
        }
        SECTION("they should be left alone if metadata is disabled") {
            shared_test_config.should_teardown_test_directory = true;
            shared_test_config.metadata_mode = SyncManager::MetadataMode::NoMetadata;
            TestSyncManager tsm(shared_test_config);
            auto users = tsm.app()->sync_manager()->all_users();
            for (auto& path : paths) {
                REQUIRE_REALM_EXISTS(path);
            }
        }
    }
}

TEST_CASE("sync_manager: file actions", "[sync]") {
    using Action = SyncFileActionMetadata::Action;
    using Cfg = TestSyncManager::Config;
    reset_test_directory(base_path);

    auto file_manager = SyncFileManager(base_path, "bar_app_id");
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string realm_url = "https://example.realm.com/~/1";
    const std::string partition = "partition_foo";
    const std::string uuid_1 = "uuid-foo-1";
    const std::string uuid_2 = "uuid-bar-1";
    const std::string uuid_3 = "uuid-baz-1";
    const std::string uuid_4 = "uuid-baz-2";

    const std::string local_uuid_1 = "foo-1";
    const std::string local_uuid_2 = "bar-1";
    const std::string local_uuid_3 = "baz-1";
    const std::string local_uuid_4 = "baz-2";

    // Realm paths
    const std::string realm_path_1 = file_manager.realm_file_path(uuid_1, local_uuid_1, realm_url, partition);
    const std::string realm_path_2 = file_manager.realm_file_path(uuid_2, local_uuid_2, realm_url, partition);
    const std::string realm_path_3 = file_manager.realm_file_path(uuid_3, local_uuid_3, realm_url, partition);
    const std::string realm_path_4 = file_manager.realm_file_path(uuid_4, local_uuid_4, realm_url, partition);

    SECTION("Action::DeleteRealm") {
        // Create some file actions
        manager.make_file_action_metadata(realm_path_1, realm_url, "user1", Action::DeleteRealm);
        manager.make_file_action_metadata(realm_path_2, realm_url, "user2", Action::DeleteRealm);
        manager.make_file_action_metadata(realm_path_3, realm_url, "user3", Action::DeleteRealm);

        SECTION("should properly delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // All Realms should be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
        }

        SECTION("should fail gracefully if the Realm is missing") {
            // Don't actually create the Realm files
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoMetadata));
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
        }
    }

    SECTION("Action::BackUpThenDeleteRealm") {
        const auto recovery_dir = file_manager.recovery_directory_path();
        // Create some file actions
        const std::string recovery_1 = util::file_path_by_appending_component(recovery_dir, "recovery-1");
        const std::string recovery_2 = util::file_path_by_appending_component(recovery_dir, "recovery-2");
        const std::string recovery_3 = util::file_path_by_appending_component(recovery_dir, "recovery-3");
        manager.make_file_action_metadata(realm_path_1, realm_url, "user1", Action::BackUpThenDeleteRealm,
                                          recovery_1);
        manager.make_file_action_metadata(realm_path_2, realm_url, "user2", Action::BackUpThenDeleteRealm,
                                          recovery_2);
        manager.make_file_action_metadata(realm_path_3, realm_url, "user3", Action::BackUpThenDeleteRealm,
                                          recovery_3);

        SECTION("should properly copy the Realm file and delete the Realm") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // All Realms should be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(recovery_1));
            CHECK(File::exists(recovery_2));
            CHECK(File::exists(recovery_3));
        }

        SECTION("should copy the Realm to the recovery_directory_path") {
            const std::string identity = "b241922032489d4836ecd0c82d0445f0";
            const auto realm_base_path = file_manager.realm_file_path(identity, "", "realmtasks", partition);
            std::string recovery_path = util::reserve_unique_file_name(
                file_manager.recovery_directory_path(), util::create_timestamped_template("recovered_realm"));
            create_dummy_realm(realm_base_path);
            REQUIRE_REALM_EXISTS(realm_base_path);
            REQUIRE(!File::exists(recovery_path));
            // Manually create a file action metadata entry to simulate a client reset.
            manager.make_file_action_metadata(realm_base_path, realm_url, identity, Action::BackUpThenDeleteRealm,
                                              recovery_path);
            auto pending_actions = manager.all_pending_actions();
            REQUIRE(pending_actions.size() == 4);

            // Simulate client launch.
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));

            CHECK(pending_actions.size() == 0);
            CHECK(File::exists(recovery_path));
            REQUIRE_REALM_DOES_NOT_EXIST(realm_base_path);
        }

        SECTION("should fail gracefully if the Realm is missing") {
            // Don't actually create the Realm files
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            // File actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 0);
            // There should not be recovery files.
            CHECK(!File::exists(recovery_1));
            CHECK(!File::exists(recovery_2));
            CHECK(!File::exists(recovery_3));
        }

        SECTION("should work properly when manually driven") {
            REQUIRE(!File::exists(recovery_1));
            // Create a Realm file
            create_dummy_realm(realm_path_4);
            // Configure the system
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            REQUIRE(manager.all_pending_actions().size() == 0);
            // Add a file action after the system is configured.
            REQUIRE_REALM_EXISTS(realm_path_4);
            REQUIRE(File::exists(file_manager.recovery_directory_path()));
            manager.make_file_action_metadata(realm_path_4, realm_url, "user4", Action::BackUpThenDeleteRealm,
                                              recovery_1);
            REQUIRE(manager.all_pending_actions().size() == 1);
            // Force the recovery. (In a real application, the user would have closed the files by now.)
            REQUIRE(tsm.app()->sync_manager()->immediately_run_file_actions(realm_path_4));
            // There should be recovery files.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_4);
            CHECK(File::exists(recovery_1));
            REQUIRE(manager.all_pending_actions().size() == 0);
        }

        SECTION("should fail gracefully if there is already a file at the destination") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            create_dummy_realm(recovery_1);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption));
            // Most file actions should be cleared.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 1);
            // Realms should be deleted.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(recovery_2));
            CHECK(File::exists(recovery_3));
        }

        SECTION("should change the action to delete if copy succeeds but delete fails") {
            if (!chmod_supported(base_path)) {
                return;
            }
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            // remove secondary files so the action doesn't throw when it can't read these
            File::try_remove(DB::get_core_file(realm_path_3, DB::CoreFileType::Note));
            File::try_remove(DB::get_core_file(realm_path_3, DB::CoreFileType::Log));
            util::try_remove_dir_recursive(DB::get_core_file(realm_path_3, DB::CoreFileType::Management));
            // remove write permissions of the parent directory so that removing realm3 will fail
            std::string realm3_dir = get_parent_directory(realm_path_3);
            int original_perms = get_permissions(realm3_dir);
            realm::chmod(realm3_dir, original_perms & (~0b010000000)); // without owner_write
            // run the actions
            TestSyncManager tsm({"bar_app_id", base_path, SyncManager::MetadataMode::NoEncryption});
            // restore write permissions to the directory
            realm::chmod(realm3_dir, original_perms);
            // Everything succeeded except deleting realm_path_3
            auto pending_actions = manager.all_pending_actions();
            REQUIRE(pending_actions.size() == 1);
            // the realm3 action changed from BackUpThenDeleteRealm to DeleteRealm
            CHECK(pending_actions.get(0).action() == Action::DeleteRealm);
            CHECK(pending_actions.get(0).original_name() == realm_path_3);
            CHECK(File::exists(recovery_3));   // the copy was successful
            CHECK(File::exists(realm_path_3)); // the delete failed
            // try again with proper permissions
            REQUIRE(tsm.app()->sync_manager()->immediately_run_file_actions(realm_path_3));
            REQUIRE(manager.all_pending_actions().size() == 0);
            // Realms should all be deleted.
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_1);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_2);
            REQUIRE_REALM_DOES_NOT_EXIST(realm_path_3);
            // There should be recovery files.
            CHECK(File::exists(recovery_2));
            CHECK(File::exists(recovery_3));
        }

        SECTION("should do nothing if metadata is disabled") {
            // Create some Realms
            create_dummy_realm(realm_path_1);
            create_dummy_realm(realm_path_2);
            create_dummy_realm(realm_path_3);
            TestSyncManager tsm(Cfg("bar_app_id", base_path, SyncManager::MetadataMode::NoMetadata));
            // All file actions should still be present.
            auto pending_actions = manager.all_pending_actions();
            CHECK(pending_actions.size() == 3);
            // All Realms should still be present.
            REQUIRE_REALM_EXISTS(realm_path_1);
            REQUIRE_REALM_EXISTS(realm_path_2);
            REQUIRE_REALM_EXISTS(realm_path_3);
            // There should not be recovery files.
            CHECK(!File::exists(recovery_1));
            CHECK(!File::exists(recovery_2));
            CHECK(!File::exists(recovery_3));
        }
    }
}

TEST_CASE("sync_manager: has_active_sessions", "[active_sessions]") {
    reset_test_directory(base_path);

    TestSyncManager init_sync_manager(TestSyncManager::Config(base_path, SyncManager::MetadataMode::NoEncryption),
                                      {false});
    auto sync_manager = init_sync_manager.app()->sync_manager();

    SECTION("no active sessions") {
        REQUIRE(!sync_manager->has_existing_sessions());
    }

    auto schema = Schema{
        {"object",
         {
             {"_id", PropertyType::Int, Property::IsPrimary{true}},
             {"value", PropertyType::Int},
         }},
    };

    std::atomic<bool> error_handler_invoked(false);
    Realm::Config config;
    auto user = sync_manager->get_user("user-name", ENCODE_FAKE_JWT("not_a_real_token"), ENCODE_FAKE_JWT("samesies"),
                                       "https://realm.example.org", dummy_device_id);
    auto create_session = [&](SyncSessionStopPolicy stop_policy) {
        std::shared_ptr<SyncSession> session = sync_session(
            user, "/test-dying-state",
            [&](auto, auto) {
                error_handler_invoked = true;
            },
            stop_policy, nullptr, schema, &config);
        EventLoop::main().run_until([&] {
            return sessions_are_active(*session);
        });
        return session;
    };

    SECTION("active sessions") {
        {
            auto session = create_session(SyncSessionStopPolicy::Immediately);
            REQUIRE(sync_manager->has_existing_sessions());
            session->close();
        }
        EventLoop::main().run_until([&] {
            return !sync_manager->has_existing_sessions();
        });
        REQUIRE(!sync_manager->has_existing_sessions());
    }
}
