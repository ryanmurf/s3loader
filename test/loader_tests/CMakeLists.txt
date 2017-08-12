set(aws-sdk-cpp_DIR ${CMAKE_SOURCE_DIR}/../aws-sdk-cpp/)

IF(APPLE)
    LINK_DIRECTORIES(/usr/local/Cellar/gettext/0.19.8.1/lib/)
    find_package (Intl)
    if (Intl_FOUND)
        include_directories (${Intl_INCLUDE_DIRS})
    endif()
endif()

find_package(aws-sdk-cpp)

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
include_directories(${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})
include_directories(/opt/vertica/sdk/include/)
include_directories(${CMAKE_SOURCE_DIR}/src/)

add_executable(runLoaderTests download.cpp)

target_link_libraries(runLoaderTests gmock gtest gtest_main aws-cpp-sdk-s3 s3loader)