# This must be AFTER link_directories
add_executable(test-sdb
    test_sdb.cpp
    test_sdb_io.cpp
)

target_compile_definitions(test-sdb PUBLIC -DBOOST_TEST_DYN_LINK -D_REENTRANT)

target_link_libraries(
    test-sdb
    ${Boost_LIBRARIES}
    ${Utxx_LIBRARIES}
    sdb
    boost_unit_test_framework
)

add_dependencies(test-sdb sdb)

install(TARGETS test-sdb  RUNTIME DESTINATION test)
