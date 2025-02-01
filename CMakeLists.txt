cmake_minimum_required(VERSION 3.20)
project(AdvancedCacheManager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ------------------------------------------------------------------------------------
# 1) Anwendung "AdvancedCacheManager"
# ------------------------------------------------------------------------------------
add_executable(AdvancedCacheManager
        src/main.cpp
)

target_include_directories(AdvancedCacheManager
        PUBLIC include
)

target_link_libraries(AdvancedCacheManager
        sqlite3
        nlohmann_json
        pthread
        rt
)

# ------------------------------------------------------------------------------------
# 2) GoogleTest suchen und binden
# ------------------------------------------------------------------------------------
# Diese Zeilen suchen ein installiertes GoogleTest-Paket auf deinem System.
find_package(GTest REQUIRED)

# ------------------------------------------------------------------------------------
# 3) Test-Executable "AdvancedCacheManagerTests"
# ------------------------------------------------------------------------------------
add_executable(AdvancedCacheManagerTests
        tests/main.cpp
        tests/test_eventbus.cpp
        tests/test_sockethandler.cpp
        tests/test_ramhandler.cpp
        tests/test_diskhandler.cpp
)

target_include_directories(AdvancedCacheManagerTests
        PUBLIC include
)

target_link_libraries(AdvancedCacheManagerTests
        sqlite3
        pthread
        rt
        GTest::gtest
        GTest::gtest_main
)

# ------------------------------------------------------------------------------------
# 4) CTest-Integration
# ------------------------------------------------------------------------------------
enable_testing()
add_test(NAME MyEventBusTests COMMAND AdvancedCacheManagerTests)
