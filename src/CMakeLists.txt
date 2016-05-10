# vim:ts=2:sw=2:et

# This must be AFTER link_directories
add_executable(sdb-krx sdb_krx_fmt.cpp)
add_executable(sdbdump sdbdump.cpp)

target_link_libraries(
  sdb-krx
  ${Boost_LIBRARIES}
  ${Utxx_LIBRARIES}
  sdb
)

target_link_libraries(
  sdbdump
  ${Boost_LIBRARIES}
  ${Utxx_LIBRARIES}
  sdb
)

# The SecDB library:
list(APPEND SECDB_LIB_SRCS
  sdb_fmt.cpp
)

add_library(sdb SHARED ${SECDB_LIB_SRCS})

set_target_properties(
  sdb PROPERTIES OUTPUT_NAME sdb
  DEBUG_POSTFIX   "${LIB_SUFFIX}"
  RELEASE_POSTFIX ""
)

#set_target_properties(sdb-bin PROPERTIES OUTPUT_NAME sdb)

# SET_TARGET_PROPERTIES(
#     sdb PROPERTIES OUTPUT_NAME sdb
#     DEBUG_POSTFIX   "_${TOOLCHAIN}_d"
#     RELEASE_POSTFIX "_${TOOLCHAIN}"
# )

install(TARGETS sdb     LIBRARY DESTINATION lib)
install(TARGETS sdbdump RUNTIME DESTINATION bin)
install(TARGETS sdb-krx RUNTIME DESTINATION bin)

# In the install below we split library installation in a separate library clause
# so that it's possible to build/install both Release and Debug versions of the
# library and then include that into a package

if (${CMAKE_BUILD_TYPE} STREQUAL "release")
  install(
    FILES ${CMAKE_BINARY_DIR}/src/lib${PROJECT_NAME}_d.so.${PROJECT_VERSION}
    DESTINATION lib
  )
  install(
    FILES ${CMAKE_BINARY_DIR}/src/lib${PROJECT_NAME}_d.so
    DESTINATION lib
  )
endif()


