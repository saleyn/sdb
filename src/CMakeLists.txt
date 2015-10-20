# vim:ts=2:sw=2:et

# This must be AFTER link_directories
add_executable(secdb-krx secdb_krx_fmt.cpp)

target_link_libraries(
  secdb-krx
  ${Boost_LIBRARIES}
  ${Utxx_LIBRARIES}
  secdb
)

# The SecDB library:
list(APPEND SECDB_LIB_SRCS
  secdb_fmt.cpp
)

add_library(secdb SHARED ${SECDB_LIB_SRCS})

SET_TARGET_PROPERTIES(
  secdb PROPERTIES OUTPUT_NAME secdb
  DEBUG_POSTFIX   "_d"
  RELEASE_POSTFIX ""
)

# SET_TARGET_PROPERTIES(
#     secdb PROPERTIES OUTPUT_NAME secdb
#     DEBUG_POSTFIX   "_${TOOLCHAIN}_d"
#     RELEASE_POSTFIX "_${TOOLCHAIN}"
# )

install(TARGETS secdb     LIBRARY DESTINATION lib)
install(TARGETS secdb-krx RUNTIME DESTINATION bin)