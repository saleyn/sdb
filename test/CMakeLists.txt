# This must be AFTER link_directories
add_executable(test-secdb
    test_secdb.cpp
    test_secdb_io.cpp
)

target_compile_definitions(test-secdb PUBLIC -DBOOST_TEST_DYN_LINK -D_REENTRANT)

target_link_libraries(
    test-secdb
    ${Boost_LIBRARIES}
    ${Utxx_LIBRARIES}
    secdb
    boost_unit_test_framework
)

ADD_DEPENDENCIES(test-secdb secdb)

install(TARGETS test-secdb  RUNTIME DESTINATION test)
