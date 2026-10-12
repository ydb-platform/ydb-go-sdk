cmake_minimum_required(VERSION 3.25)

project(ydb-transactional-writer-benchmark-cpp LANGUAGES C CXX)

include(CTest)
include(FetchContent)

# Homebrew's gRPC exports its own upb headers from a common include prefix.
# Prefer every dependency's private include directories so the bundled gRPC
# sources do not accidentally compile against a different installed upb ABI.
set(CMAKE_INCLUDE_DIRECTORIES_BEFORE ON)
if(APPLE)
    # The current macOS SDK declares pipe2 for a future deployment target.
    # c-ares' configure probe sees the declaration but its guarded-availability
    # build correctly rejects the call, so force the portable pipe fallback.
    set(HAVE_PIPE2 0 CACHE INTERNAL "pipe2 is unavailable for this target" FORCE)
endif()

set(YDB_CPP_SDK_TAG "v3.23.0" CACHE STRING "ydb-cpp-sdk git tag or commit")

FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz
    URL_HASH SHA256=42f6e95cad6ec532fd372391373363b62a14af6d771056dbfc86160e6dfff7aa
    SYSTEM
)

FetchContent_Declare(
    ydb_cpp_sdk
    GIT_REPOSITORY https://github.com/ydb-platform/ydb-cpp-sdk.git
    GIT_TAG "${YDB_CPP_SDK_TAG}"
    GIT_SHALLOW TRUE
    BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/ydb-cpp-sdk"
    SYSTEM
)

FetchContent_MakeAvailable(nlohmann_json ydb_cpp_sdk)

if(APPLE)
    foreach(upb_target IN ITEMS
        upb_base_lib
        upb_json_lib
        upb_mem_lib
        upb_message_lib
        upb_textformat_lib
    )
        if(TARGET ${upb_target})
            target_include_directories(
                ${upb_target}
                BEFORE PRIVATE
                    "${grpc_SOURCE_DIR}/src/core/ext/upb-gen"
                    "${grpc_SOURCE_DIR}/src/core/ext/upbdefs-gen"
                    "${grpc_SOURCE_DIR}/third_party/upb"
            )
        endif()
    endforeach()
    if(TARGET grpc_plugin_support)
        # Homebrew also installs protobuf headers. gRPC's compiler support must
        # use the protobuf revision fetched by the SDK build. The YDB SDK is a
        # SYSTEM FetchContent dependency, so use a distinct symlink path to
        # keep CMake from deduplicating this non-system include with protobuf's
        # transitive -isystem directory.
        set(
            bundled_protobuf_include
            "${CMAKE_CURRENT_BINARY_DIR}/bundled-protobuf-overlay"
        )
        file(MAKE_DIRECTORY "${bundled_protobuf_include}")
        if(NOT EXISTS "${bundled_protobuf_include}/google")
            file(
                CREATE_LINK
                "${protobuf_SOURCE_DIR}/src/google"
                "${bundled_protobuf_include}/google"
                SYMBOLIC
                RESULT bundled_protobuf_link_result
            )
            if(NOT bundled_protobuf_link_result STREQUAL "0")
                message(
                    FATAL_ERROR
                    "Could not create bundled protobuf include link: "
                    "${bundled_protobuf_link_result}"
                )
            endif()
        endif()
        if(IS_SYMLINK "${bundled_protobuf_include}/absl" AND
           NOT EXISTS "${bundled_protobuf_include}/absl")
            file(REMOVE "${bundled_protobuf_include}/absl")
        endif()
        if(NOT EXISTS "${bundled_protobuf_include}/absl")
            file(
                CREATE_LINK
                "${CPM_PACKAGE_abseil-cpp_SOURCE_DIR}/absl"
                "${bundled_protobuf_include}/absl"
                SYMBOLIC
                RESULT bundled_abseil_link_result
            )
            if(NOT bundled_abseil_link_result STREQUAL "0")
                message(
                    FATAL_ERROR
                    "Could not create bundled Abseil include link: "
                    "${bundled_abseil_link_result}"
                )
            endif()
        endif()
        # Apply the overlay to every fetched build target. Homebrew's common
        # include prefix can otherwise leak into later YDB SDK targets through
        # unrelated imported libraries and mix two protobuf/Abseil revisions.
        function(prepend_bundled_protobuf_includes directory)
            get_property(
                directory_targets
                DIRECTORY "${directory}"
                PROPERTY BUILDSYSTEM_TARGETS
            )
            foreach(dependency_target IN LISTS directory_targets)
                get_target_property(
                    dependency_target_type
                    "${dependency_target}"
                    TYPE
                )
                if(dependency_target_type STREQUAL "EXECUTABLE" OR
                   dependency_target_type STREQUAL "STATIC_LIBRARY" OR
                   dependency_target_type STREQUAL "SHARED_LIBRARY" OR
                   dependency_target_type STREQUAL "MODULE_LIBRARY" OR
                   dependency_target_type STREQUAL "OBJECT_LIBRARY")
                    target_include_directories(
                        "${dependency_target}"
                        BEFORE PRIVATE "${bundled_protobuf_include}"
                    )
                endif()
            endforeach()

            get_property(
                child_directories
                DIRECTORY "${directory}"
                PROPERTY SUBDIRECTORIES
            )
            foreach(child_directory IN LISTS child_directories)
                prepend_bundled_protobuf_includes("${child_directory}")
            endforeach()
        endfunction()

        prepend_bundled_protobuf_includes("${CMAKE_CURRENT_SOURCE_DIR}")
    endif()
endif()

add_executable(transactional-writer-benchmark-cpp src/main.cpp)
if(APPLE AND DEFINED bundled_protobuf_include)
    target_include_directories(
        transactional-writer-benchmark-cpp
        BEFORE PRIVATE "${bundled_protobuf_include}"
    )
endif()
target_compile_features(transactional-writer-benchmark-cpp PRIVATE cxx_std_20)
set_target_properties(transactional-writer-benchmark-cpp PROPERTIES CXX_EXTENSIONS OFF)
target_compile_definitions(
    transactional-writer-benchmark-cpp
    PRIVATE YDB_CPP_SDK_VERSION="${YDB_CPP_SDK_TAG}"
)
target_compile_options(
    transactional-writer-benchmark-cpp
    PRIVATE -Wall -Wextra -Wpedantic -Werror
)
target_link_libraries(
    transactional-writer-benchmark-cpp
    PRIVATE
        YDB-CPP-SDK::Helpers
        YDB-CPP-SDK::Query
        YDB-CPP-SDK::Topic
        nlohmann_json::nlohmann_json
)

if(BUILD_TESTING)
    add_test(
        NAME transactional-writer-benchmark-cpp-self-test
        COMMAND transactional-writer-benchmark-cpp --self-test
    )
endif()
