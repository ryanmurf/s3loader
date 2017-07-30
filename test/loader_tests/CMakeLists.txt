include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

include_directories(/home/ryan/Downloads/opt/vertica/sdk/include)
include_directories(/home/ryan/Downloads/opt/veritca/sdk/examples/HelperLibraries)

add_executable(runLoaderTests download.cpp)

target_link_libraries(runLoaderTests gtest gtest_main s3loader)