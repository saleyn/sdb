# vim:ts=2:sw=2:et

# This must be AFTER link_directories
add_executable(sdb-krx secdb_krx_fmt.cpp)
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
  secdb_fmt.cpp
)

add_library(sdb SHARED ${SECDB_LIB_SRCS})

set_target_properties(
  sdb PROPERTIES OUTPUT_NAME sdb
  DEBUG_POSTFIX   "${lib_suffix}"
  RELEASE_POSTFIX ""
)

#set_target_properties(secdb-bin PROPERTIES OUTPUT_NAME secdb)

# SET_TARGET_PROPERTIES(
#     sdb PROPERTIES OUTPUT_NAME secdb
#     DEBUG_POSTFIX   "_${TOOLCHAIN}_d"
#     RELEASE_POSTFIX "_${TOOLCHAIN}"
# )

install(TARGETS sdb     LIBRARY DESTINATION lib)
install(TARGETS sdbdump RUNTIME DESTINATION bin)
install(TARGETS sdb-krx RUNTIME DESTINATION bin)
