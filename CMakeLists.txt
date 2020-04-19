cmake_minimum_required (VERSION 3.15)
project(xredis CXX)

find_path(hiredis_INCLUDE_DIR hiredis/hiredis.h)
if (hiredis_INCLUDE_DIR)
  find_library(hiredis_LIBRARY hiredis)
  add_library(hiredis STATIC IMPORTED)
  set_target_properties(hiredis PROPERTIES
      IMPORTED_LOCATION ${hiredis_LIBRARY}
  )
  target_include_directories(
    hiredis
    INTERFACE ${hiredis_INCLUDE_DIR}
  )
endif()
option(xredis_BUILD_TEST "Build tests?" OFF)

file(GLOB xredis_SOURCES
  ${xredis_SOURCE_DIR}/src/*.cpp
)

add_library (xredis ${xredis_SOURCES})

target_include_directories(
  xredis
  PUBLIC ${xredis_SOURCE_DIR}/include
  PRIVATE ${xredis_SOURCE_DIR}/include/xredis
)

target_link_libraries(
  xredis
  PRIVATE hiredis
) 

if (xredis_BUILD_TEST)
  enable_testing()

  add_executable(xredis_test EXCLUDE_FROM_ALL test/xredis-test.cpp)

  target_link_libraries(
    xredis_test
    PRIVATE xredis
  ) 

  add_test(
    NAME xredis_test
    COMMAND $<TARGET_FILE:xredis_test>
  )
endif()
