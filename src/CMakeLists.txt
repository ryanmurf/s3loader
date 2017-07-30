project(s3loader)
set(CMAKE_CXX_STANDARD 11)
set(aws-sdk-cpp_DIR /home/ryan/Documents/github/aws-sdk-cpp/)

find_package(aws-sdk-cpp)
add_definitions(-DUSE_IMPORT_EXPORT)

include_directories(/home/ryan/Downloads/opt/vertica/sdk/include)
include_directories(/home/ryan/Downloads/opt/veritca/sdk/examples/HelperLibraries)

set(SOURCE_FILES library.cpp library.h)
add_library(s3loader SHARED ${SOURCE_FILES})
target_link_libraries(s3loader aws-cpp-sdk-s3)