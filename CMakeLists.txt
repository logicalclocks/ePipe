project (ePipe)

cmake_minimum_required (VERSION 3.5.0)

set(EPIPE_VERSION_MAJOR 0)
set(EPIPE_VERSION_MINOR 12)
set(EPIPE_VERSION_BUILD 0)
configure_file(Version.h.in ${CMAKE_SOURCE_DIR}/include/Version.h)

set (CMAKE_CXX_STANDARD 14)

set(Boost_USE_STATIC_LIBS ON)

find_package(Boost 1.70 REQUIRED COMPONENTS system date_time program_options thread log)
find_package(RapidJSON 1.1.0)
find_package(OpenSSL REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${RAPIDJSON_INCLUDE_DIRS})
include_directories (${CMAKE_SOURCE_DIR}/include)
include_directories(${OPENSSL_INCLUDE_DIR})

if(NOT NDB_DIR)
  set(NDB_DIR "/usr/local/mysql")
  message(STATUS "NDB_DIR was not defined. Default: ${NDB_DIR} ")
endif()

include_directories(${NDB_DIR}/include ${NDB_DIR}/include/storage/ndb ${NDB_DIR}/include/storage/ndb/ndbapi ${NDB_DIR}/include/storage/ndb/mgmapi)
link_directories(${NDB_DIR}/lib)

SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DBOOST_SPIRIT_USE_PHOENIX_V3 -Wall -Wextra -Werror -Wformat-security  -Woverloaded-virtual -Wno-unused-variable -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-strict-aliasing -Wno-switch -O3 -DDBUG_OFF" )

file(GLOB SOURCE ${CMAKE_SOURCE_DIR}/src/*.cpp)

add_executable(ePipe ${SOURCE})

target_link_libraries(ePipe ${Boost_LIBRARIES} ndbclient pthread OpenSSL::SSL)
