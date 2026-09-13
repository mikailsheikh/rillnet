cmake_minimum_required(VERSION 3.24)

project(
  rillnet
  VERSION 0.1.0
  DESCRIPTION "A stateful protocol framework"
  LANGUAGES CXX
)

option(RILLNET_BUILD_TESTS "Build rillnet tests" ON)
option(RILLNET_ENABLE_WARNINGS "Enable project compiler warnings" ON)
option(RILLNET_WARNINGS_AS_ERRORS "Treat project compiler warnings as errors" OFF)
set(RILLNET_SANITIZER "none" CACHE STRING "Sanitizer instrumentation to enable: none, address-undefined, thread")
set_property(CACHE RILLNET_SANITIZER PROPERTY STRINGS none address-undefined thread)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Boost 1.83 REQUIRED)

set(rillnet_format_sources
  include/rillnet/version.hpp
  include/rillnet/identifiers.hpp
  include/rillnet/protocol_version.hpp
  include/rillnet/frame_flags.hpp
  include/rillnet/frame.hpp
  include/rillnet/frame_codec.hpp
  include/rillnet/frame_decoder.hpp
  include/rillnet/codec.hpp
  include/rillnet/message_registry.hpp
  include/rillnet/message_codec.hpp
  include/rillnet/error.hpp
  include/rillnet/protocol_violation.hpp
  include/rillnet/lifecycle.hpp
  include/rillnet/operation.hpp
  include/rillnet/stream_id_allocator.hpp
  include/rillnet/status_code.hpp
  include/rillnet/transport.hpp
  include/rillnet/tcp_transport.hpp
  include/rillnet/tcp_client.hpp
  include/rillnet/tcp_server.hpp
  include/rillnet/write_queue.hpp
  include/rillnet/client_connection.hpp
  include/rillnet/server_connection.hpp
  src/rillnet.cpp
  tests/unit/version_test.cpp
  tests/unit/identifiers_test.cpp
  tests/unit/protocol_version_test.cpp
  tests/unit/frame_flags_test.cpp
  tests/unit/frame_test.cpp
  tests/unit/frame_codec_test.cpp
  tests/unit/frame_decoder_test.cpp
  tests/unit/codec_test.cpp
  tests/unit/message_registry_test.cpp
  tests/unit/message_codec_test.cpp
  tests/unit/error_test.cpp
  tests/unit/lifecycle_test.cpp
  tests/unit/operation_test.cpp
  tests/unit/stream_id_allocator_test.cpp
  tests/unit/status_code_test.cpp
  tests/unit/transport_test.cpp
  tests/unit/tcp_transport_test.cpp
  tests/unit/tcp_client_test.cpp
  tests/unit/tcp_server_test.cpp
  tests/unit/write_queue_test.cpp
  tests/unit/in_memory_transport.hpp
  tests/unit/client_connection_test.cpp
  tests/unit/server_connection_test.cpp
)

set(rillnet_clang_tidy_sources ${rillnet_format_sources})
list(FILTER rillnet_clang_tidy_sources INCLUDE REGEX "\\.cpp$")

function(rillnet_apply_project_warnings target_name)
  if(NOT RILLNET_ENABLE_WARNINGS)
    return()
  endif()

  set(rillnet_warning_flags)

  if(MSVC)
    list(APPEND rillnet_warning_flags /W4 /permissive-)
    if(RILLNET_WARNINGS_AS_ERRORS)
      list(APPEND rillnet_warning_flags /WX)
    endif()
  else()
    list(APPEND rillnet_warning_flags -Wall -Wextra -Wpedantic)
    if(RILLNET_WARNINGS_AS_ERRORS)
      list(APPEND rillnet_warning_flags -Werror)
    endif()
  endif()

  target_compile_options(${target_name} PRIVATE ${rillnet_warning_flags})
endfunction()

function(rillnet_apply_project_sanitizers target_name)
  if(RILLNET_SANITIZER STREQUAL "none")
    return()
  endif()

  if(MSVC)
    message(FATAL_ERROR "RILLNET_SANITIZER is not supported with MSVC")
  endif()

  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    message(FATAL_ERROR "RILLNET_SANITIZER requires Clang, AppleClang, or GCC")
  endif()

  set(rillnet_sanitizer_flags -fno-omit-frame-pointer)

  if(RILLNET_SANITIZER STREQUAL "address-undefined")
    list(APPEND rillnet_sanitizer_flags -fsanitize=address,undefined)
  elseif(RILLNET_SANITIZER STREQUAL "thread")
    list(APPEND rillnet_sanitizer_flags -fsanitize=thread)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      list(APPEND rillnet_sanitizer_flags -Wno-tsan)
    endif()
  else()
    message(FATAL_ERROR "Unknown RILLNET_SANITIZER value: ${RILLNET_SANITIZER}")
  endif()

  target_compile_options(${target_name} PRIVATE ${rillnet_sanitizer_flags})
  target_link_options(${target_name} PRIVATE ${rillnet_sanitizer_flags})
endfunction()

add_library(rillnet
  src/rillnet.cpp
)
add_library(rillnet::rillnet ALIAS rillnet)

target_compile_features(rillnet PUBLIC cxx_std_20)
set_target_properties(rillnet PROPERTIES
  CXX_STANDARD 20
  CXX_STANDARD_REQUIRED ON
  CXX_EXTENSIONS OFF
)

target_include_directories(rillnet
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_link_libraries(rillnet PUBLIC Boost::headers)

rillnet_apply_project_warnings(rillnet)
rillnet_apply_project_sanitizers(rillnet)

include(CTest)

if(RILLNET_BUILD_TESTS AND BUILD_TESTING)
  find_package(GTest REQUIRED)
  add_subdirectory(tests)
endif()

find_program(CLANG_FORMAT_EXE NAMES clang-format)
if(CLANG_FORMAT_EXE)
  add_custom_target(clang-format-check
    COMMAND ${CLANG_FORMAT_EXE} --dry-run --Werror ${rillnet_format_sources}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Checking C++ formatting with clang-format"
  )
else()
  add_custom_target(clang-format-check
    COMMAND ${CMAKE_COMMAND} -E echo "clang-format not found"
    COMMAND ${CMAKE_COMMAND} -E false
  )
endif()

find_program(CLANG_TIDY_EXE NAMES clang-tidy)
if(CLANG_TIDY_EXE)
  add_custom_target(clang-tidy
    COMMAND ${CLANG_TIDY_EXE} -p ${PROJECT_BINARY_DIR} ${rillnet_clang_tidy_sources}
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    COMMENT "Running clang-tidy"
  )
else()
  add_custom_target(clang-tidy
    COMMAND ${CMAKE_COMMAND} -E echo "clang-tidy not found"
    COMMAND ${CMAKE_COMMAND} -E false
  )
endif()
