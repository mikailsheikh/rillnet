add_executable(rillnet_unit_tests
  version_test.cpp
  identifiers_test.cpp
  protocol_version_test.cpp
  frame_flags_test.cpp
  frame_test.cpp
  frame_codec_test.cpp
  frame_decoder_test.cpp
  codec_test.cpp
  message_registry_test.cpp
  message_codec_test.cpp
  error_test.cpp
  lifecycle_test.cpp
  operation_test.cpp
  stream_id_allocator_test.cpp
  status_code_test.cpp
  transport_test.cpp
  tcp_transport_test.cpp
  tcp_client_test.cpp
  tcp_server_test.cpp
  write_queue_test.cpp
  client_connection_test.cpp
  server_connection_test.cpp
  diagnostics_test.cpp
)

target_link_libraries(rillnet_unit_tests
  PRIVATE
    rillnet::rillnet
    GTest::gmock_main
)

rillnet_apply_project_warnings(rillnet_unit_tests)
rillnet_apply_project_sanitizers(rillnet_unit_tests)

include(GoogleTest)
gtest_discover_tests(rillnet_unit_tests)
