cmake_minimum_required(VERSION 3.20)
project(AdvancedCacheManager LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Setzt das Binärverzeichnis
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Pakete finden
find_package(nlohmann_json CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)

# Hauptanwendung
add_executable(AdvancedCacheManager src/main.cpp)

target_include_directories(AdvancedCacheManager PRIVATE ${PROJECT_SOURCE_DIR}/include)

target_link_libraries(AdvancedCacheManager PRIVATE
        SQLite::SQLite3
        nlohmann_json::nlohmann_json
        Threads::Threads
)

# 📌 Sicherstellen, dass `bin/`, `db/` und `socket/` existieren
foreach(DIR bin db socket)
    execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/${DIR})
endforeach()

if(CMAKE_BUILD_TYPE MATCHES "Debug")
    add_custom_command(
            TARGET AdvancedCacheManager POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config.json
            ${CMAKE_BINARY_DIR}/bin/config.json
            COMMENT "Kopiere config.json in bin/"
    )
endif()

# 📌 Installationsregeln für Release
install(TARGETS AdvancedCacheManager RUNTIME DESTINATION bin)

# 📌 `config.json` für Release in `etc/` kopieren
install(FILES ${CMAKE_SOURCE_DIR}/config.json DESTINATION bin)

# 📌 Erstelle `db/` und `socket/`, aber kopiere nichts hinein
install(DIRECTORY DESTINATION db)
install(DIRECTORY DESTINATION socket)

# 📌 TESTS MIT GOOGLE TEST AKTIVIEREN
include(CTest)
option(ENABLE_TESTS "Baue Unit-Tests mit GoogleTest" ON)

if (ENABLE_TESTS)
    find_package(GTest REQUIRED)
    enable_testing()

    add_executable(AdvancedCacheManagerTests
            tests/test_eventbus.cpp
            tests/test_diskhandler.cpp
            tests/test_ramhandler.cpp
            tests/test_sockethandler.cpp
            tests/test_storagehandler.cpp
            tests/test_complete.cpp
    )

    target_include_directories(AdvancedCacheManagerTests PRIVATE ${PROJECT_SOURCE_DIR}/include)

    target_link_libraries(AdvancedCacheManagerTests PRIVATE
            SQLite::SQLite3
            GTest::gtest
            GTest::gtest_main
            nlohmann_json::nlohmann_json
            Threads::Threads
    )

    add_test(NAME AdvancedCacheManagerTests COMMAND AdvancedCacheManagerTests)
endif()

# 📦 CPack Konfiguration für Release
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(CPACK_PACKAGE_NAME "AdvancedCacheManager")
    set(CPACK_PACKAGE_VERSION "1.0.0")
    set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-Linux")
    set(CPACK_GENERATOR "TGZ")
    set(CPACK_PACKAGE_CONTACT "Dustin.Imholze@icloud.com")

    include(CPack)

    add_custom_target(build_package
            COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target install
            COMMAND ${CMAKE_COMMAND} -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/package -P cmake_install.cmake
            COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target package
            DEPENDS AdvancedCacheManager
            COMMENT "Erstelle Installationspaket mit CPack"
    )
endif()
