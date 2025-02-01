cmake_minimum_required(VERSION 3.20)
project(AdvancedCacheManager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Finde benötigte Pakete (z. B. nlohmann_json und GTest)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(GTest REQUIRED)

# Hauptanwendung
add_executable(AdvancedCacheManager
        src/main.cpp
)

target_include_directories(AdvancedCacheManager
        PUBLIC ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(AdvancedCacheManager
        sqlite3
        nlohmann_json::nlohmann_json
        pthread
        rt
)

# Test-Executable
add_executable(AdvancedCacheManagerTests
        tests/main.cpp
        tests/test_eventbus.cpp
        tests/test_sockethandler.cpp
        tests/test_ramhandler.cpp
        tests/test_diskhandler.cpp
        tests/test_storagehandler.cpp
        tests/test_complete.cpp
)

target_include_directories(AdvancedCacheManagerTests
        PUBLIC ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(AdvancedCacheManagerTests
        sqlite3
        pthread
        rt
        GTest::gtest
        GTest::gtest_main
        nlohmann_json::nlohmann_json
)

# CTest-Integration
enable_testing()
add_test(NAME AdvancedCacheManagerTests COMMAND AdvancedCacheManagerTests)

# Konfigurationsdatei in das Build-Verzeichnis kopieren
configure_file(${CMAKE_SOURCE_DIR}/config.json ${CMAKE_BINARY_DIR}/config.json COPYONLY)
