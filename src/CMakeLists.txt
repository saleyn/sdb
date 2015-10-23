# vim:ts=2:sw=2:et

# This must be AFTER link_directories
add_executable(secdb-krx secdb_krx_fmt.cpp)
add_executable(secdb-bin secdb.cpp)

target_link_libraries(
  secdb-krx
  ${Boost_LIBRARIES}
  ${Utxx_LIBRARIES}
  secdb
)

target_link_libraries(
  secdb-bin
  ${Boost_LIBRARIES}
  ${Utxx_LIBRARIES}
  secdb
)

# The SecDB library:
list(APPEND SECDB_LIB_SRCS
  secdb_fmt.cpp
)

add_library(secdb SHARED ${SECDB_LIB_SRCS})

set_target_properties(
  secdb PROPERTIES OUTPUT_NAME secdb
  DEBUG_POSTFIX   "_d"
  RELEASE_POSTFIX ""
)

set_target_properties(secdb-bin PROPERTIES OUTPUT_NAME secdb)

# SET_TARGET_PROPERTIES(
#     secdb PROPERTIES OUTPUT_NAME secdb
#     DEBUG_POSTFIX   "_${TOOLCHAIN}_d"
#     RELEASE_POSTFIX "_${TOOLCHAIN}"
# )

install(TARGETS secdb     LIBRARY DESTINATION lib)
install(TARGETS secdb-bin RUNTIME DESTINATION bin)
install(TARGETS secdb-krx RUNTIME DESTINATION bin)