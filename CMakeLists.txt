project (hopsfs-elastic-notifier)

cmake_minimum_required (VERSION 2.8)

find_package(Boost 1.54 REQUIRED COMPONENTS system date_time log log_setup program_options thread)
find_package(cppnetlib 0.11.0 REQUIRED uri client-connections)
find_package(RapidJSON 1.0.2)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CPPNETLIB_INCLUDE_DIRS})
include_directories(${RAPIDJSON_INCLUDE_DIRS})
include_directories (${CMAKE_SOURCE_DIR}/include)

if(NOT NDB_DIR)
  set(NDB_DIR "/usr/local/mysql")
  message(STATUS "NDB_DIR was not defined. Default: ${NDB_DIR} ")
endif()

include_directories(${NDB_DIR}/include ${NDB_DIR}/include/storage/ndb ${NDB_DIR}/include/storage/ndb/ndbapi ${NDB_DIR}/include/storage/ndb/mgmapi)
link_directories(${NDB_DIR}/lib)


SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DBOOST_LOG_DYN_LINK -DBOOST_SPIRIT_USE_PHOENIX_V3 -Wall -Wextra -Wformat-security  -Woverloaded-virtual -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-strict-aliasing -Wno-switch -O3 -DDBUG_OFF" )

file(GLOB SOURCE ${CMAKE_SOURCE_DIR}/src/*.cpp)

add_executable(hopsfs-elastic-notifer ${SOURCE})

target_link_libraries(hopsfs-elastic-notifer ${Boost_LIBRARIES} ${CPPNETLIB_LIBRARIES} ndbclient pthread)