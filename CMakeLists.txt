cmake_minimum_required (VERSION 3.15)
project(xredis CXX)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    include(CTest)
endif()

find_package(fmt CONFIG REQUIRED)
find_package(hiredis)

if (NOT (TARGET hiredis OR ${hiredis_FOUND}))
  message("Dependency 'hiredis' is not within CMake package sources, attempting to add it manually")
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
    message("Successfully found dependency 'hiredis' on ${hiredis_LIBRARY}")
  else()
    message(FATAL_ERROR "Dependency 'hiredis' not found, aborting")
  endif()
endif()


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
  PRIVATE hiredis fmt::fmt fmt::fmt-header-only
) 

if (MSVC)
  target_link_libraries(
    xredis
    PUBLIC ws2_32
  ) 
endif()

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND BUILD_TESTING)
  add_executable(xredis_test EXCLUDE_FROM_ALL test/xredis-test.cpp)

  target_link_libraries(
    xredis_test
    PRIVATE xredis hiredis
  ) 

  add_test(
    NAME xredis_test
    COMMAND $<TARGET_FILE:xredis_test>
  )
endif()
