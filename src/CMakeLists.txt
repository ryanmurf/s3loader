project(s3loader)
set(CMAKE_CXX_STANDARD 11)
set(aws-sdk-cpp_DIR ${CMAKE_SOURCE_DIR}/../aws-sdk-cpp/)

find_package(aws-sdk-cpp)
add_definitions(-DUSE_IMPORT_EXPORT)

IF(APPLE)
    LINK_DIRECTORIES(/usr/local/Cellar/gettext/0.19.8.1/lib/)
    find_package (Intl)
    if (Intl_FOUND)
        include_directories (${Intl_INCLUDE_DIRS})
    endif()
endif()

include_directories(/opt/vertica/sdk/include/)

set(SOURCE_FILES s3loader.cpp s3loader.h)
add_library(s3loader SHARED ${SOURCE_FILES} /opt/vertica/sdk/include/Vertica.cpp /opt/vertica/sdk/include/BuildInfo.h)
target_link_libraries(s3loader aws-cpp-sdk-s3)
IF(APPLE)
    target_link_libraries(s3loader Intl)
endif()