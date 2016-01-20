# vim:ts=4:sw=4:et
cmake_minimum_required(VERSION 3.0)
project(secdb VERSION 0.1)

#-------------------------------------------------------------------------------
# CMAKE options customization
#-------------------------------------------------------------------------------
option(WITH_THRIFT          "Enable when UTXX was compiled with Thrift"  ON)
option(VERBOSE              "Turn verbosity on|off"                      OFF)
#set(DEBUG "vars")

if(VERBOSE)
    set(CMAKE_VERBOSE_MAKEFILE ON)
endif()

string(TOLOWER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE)
string(TOLOWER "${TOOLCHAIN}"        TOOLCHAIN)

#-------------------------------------------------------------------------------
# Toolchain
#-------------------------------------------------------------------------------
# See also build/CMakeInit.txt
if(TOOLCHAIN STREQUAL "gcc")
    set(CMAKE_C_COMPILER   "gcc")
    set(CMAKE_CXX_COMPILER "g++")
    add_definitions(-Wno-unused-local-typedefs -Wno-strict-aliasing -fopenmp
                    -std=c++1y)

    if(CMAKE_BUILD_TYPE STREQUAL "release")
        add_definitions(-flto -funroll-loops -fomit-frame-pointer)

        # The following will omit all symbol information from the build:
        #add_definitions(-Wl,-s)
        #set(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -s")
    endif()
elseif(TOOLCHAIN STREQUAL  "intel")
    set(CMAKE_C_COMPILER   "icc")
    set(CMAKE_CXX_COMPILER "icpc")
    add_definitions(-openmp)

elseif(TOOLCHAIN STREQUAL  "clang")
    set(CMAKE_C_COMPILER   "clang")
    set(CMAKE_CXX_COMPILER "clang++")
    add_definitions(-std=c++1y)
else()
    message(FATAL_ERROR "Invalid toolchain: ${TOOLCHAIN}")
endif()

message(STATUS "Configuring for the ${TOOLCHAIN} toolchain")

if(CMAKE_BUILD_TYPE STREQUAL "debug")
    set(lib_suffix "_d")
endif()

#-------------------------------------------------------------------------------
# Policies
#-------------------------------------------------------------------------------
# Don't curse at non-existing dependencies (since we use code generation)
cmake_policy(SET CMP0046 OLD)

# RPATH configuration
# ===================
# Don't skip the full RPATH for the build tree
set(CMAKE_SKIP_BUILD_RPATH  FALSE)
# When building, don't use the install RPATH already
# (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
# Add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

#-------------------------------------------------------------------------------
# Dependent packages and their directory locations
#-------------------------------------------------------------------------------
find_package(PkgConfig)

# create a new package root variable and initialize it to 
# the cache with no FORCE
set(PKG_ROOT_DIR "$ENV{PKG_ROOT_DIR}" CACHE PATH "Package root directory")
if (NOT PKG_ROOT_DIR)
    set(PKG_ROOT_DIR "/opt/pkg" CACHE PATH "Package root directory")
endif()

set(Utxx_DIR             "${PKG_ROOT_DIR}/utxx/current")

if(WITH_THRIFT)
    set(Thrift_DIR       "${PKG_ROOT_DIR}/thrift/current")
    # Determine thrift include/libs
    set(ENV{PKG_CONFIG_PATH} "${Thrift_DIR}/gcc/lib/pkgconfig")
    pkg_search_module(Thrift REQUIRED thrift)
    message(STATUS "Thrift found: ${Thrift_DIR}")
endif()

# Boost (with local modifications):
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREAD ON)
set(Boost_NO_SYSTEM_PATHS ON)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/build")

find_package(Boost 1.55.0 REQUIRED COMPONENTS
             system filesystem date_time program_options thread)

if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
     link_directories(${Boost_LIBRARY_DIRS})
endif()

set(Boost_LIBRARIES
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_THREAD_LIBRARY}
    ${Boost_REGEX_LIBRARY}
    ${Boost_DATE_TIME_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_PROGRAM_OPTIONS_LIBRARY}
    ${Boost_IOSTREAMS_LIBRARY}
)

# Determine utxx include/libs
set(ENV{PKG_CONFIG_PATH} "${Utxx_DIR}/gcc/lib/pkgconfig:${Utxx_DIR}/lib/pkgconfig")
pkg_search_module(Utxx REQUIRED utxx)

message(STATUS "Boost found: ${BOOST_ROOT}")
message(STATUS "Utxx  found: ${Utxx_DIR}")

#-------------------------------------------------------------------------------
# MAKE custom targets
#-------------------------------------------------------------------------------

add_definitions(-DBOOST_SYSTEM_NO_DEPRECATED -Wno-unused-local-typedef)

#-------------------------------------------------------------------------------
# Configure files
#-------------------------------------------------------------------------------
configure_file("${CMAKE_SOURCE_DIR}/${PROJECT_NAME}.pc.in"
               "${CMAKE_BINARY_DIR}/${PROJECT_NAME}.pc"
               @ONLY)

#-------------------------------------------------------------------------------
# MAKE options
#-------------------------------------------------------------------------------

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${BOOST_INCLUDE_DIRS}
    ${Thrift_INCLUDE_DIRS}
    ${Utxx_INCLUDE_DIRS}
)
link_directories(
    ${BOOST_LIBRARY_DIRS}
    ${Thrift_LIBRARY_DIRS}
    ${Utxx_LIBRARY_DIRS}
)

add_subdirectory(src)
add_subdirectory(test)

#-------------------------------------------------------------------------------
# INSTALL options
#-------------------------------------------------------------------------------
install(
    DIRECTORY      ${CMAKE_SOURCE_DIR}/include/${PROJECT_NAME}/
    DESTINATION    ${CMAKE_INSTALL_PREFIX}/include/${PROJECT_NAME}
    FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp" PATTERN "*.hxx"
)
install(
    FILES       ${CMAKE_SOURCE_DIR}/LICENSE.txt
                ${CMAKE_SOURCE_DIR}/README.md
    DESTINATION ${CMAKE_INSTALL_PREFIX}
)
install(
	FILES 		${CMAKE_BINARY_DIR}/${PROJECT_NAME}.pc
    DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/pkgconfig
)

#-------------------------------------------------------------------------------
# Post-install script (installation of symlinks):
#-------------------------------------------------------------------------------
install(SCRIPT ${CMAKE_SOURCE_DIR}/build-aux/install-symlinks.cmake)

#-------------------------------------------------------------------------------
# CTEST options
#-------------------------------------------------------------------------------
enable_testing()

add_test(test-sdb/io     test/test-sdb  -l test_suite)
