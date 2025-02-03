cmake_minimum_required(VERSION 3.20)
project(AdvancedCacheManager LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Setze das Standard-Ausgabeverzeichnis für Binärdateien
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

# Erstelle Verzeichnisse, die zur Laufzeit benötigt werden
foreach(DIR bin db socket)
    execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/${DIR})
endforeach()

# ---------------------------
# Konfigurationsverhalten an Build-Typ anpassen
# ---------------------------
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message(STATUS "Konfiguration für Debug-Build")
    # Im Debug-Build: Einfaches Kopieren der Konfigurationsdatei ins bin-Verzeichnis
    add_custom_command(
            TARGET AdvancedCacheManager POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${CMAKE_SOURCE_DIR}/config.json
            ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/config.json
            COMMENT "Kopiere config.json in ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/"
    )
else() # Annahme: Release-Build
    message(STATUS "Konfiguration für Release-Build")
    # Für Release empfiehlt es sich, Optimierungen zu aktivieren und Debug-Informationen zu entfernen.
    # Viele dieser Flags werden automatisch gesetzt, wenn du CMAKE_BUILD_TYPE=Release verwendest.
    # Zusätzlich definieren wir ein Makro, um ggf. den Pfad zur Konfigurationsdatei anzugeben.
    target_compile_definitions(AdvancedCacheManager PRIVATE "CONFIG_FILE_PATH=\"/etc/AdvancedCacheManager/config.json\"")
endif()

# ---------------------------
# Installationsregeln
# ---------------------------
# Installiere die Binärdatei in das "bin"-Verzeichnis
install(TARGETS AdvancedCacheManager RUNTIME DESTINATION bin)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    # Debug: Kopiere config.json ins bin-Verzeichnis, damit die Debug-Umgebung vollständig ist
    install(FILES ${CMAKE_SOURCE_DIR}/config.json DESTINATION bin)
else()
    # Release: Nach Best Practices gehört eine Konfigurationsdatei nicht ins bin-Verzeichnis.
    # Stattdessen legen wir sie in ein dediziertes "etc"-Verzeichnis ab.
    install(FILES ${CMAKE_SOURCE_DIR}/config.json DESTINATION etc/AdvancedCacheManager)
endif()

# Erstelle leere Verzeichnisse (z.B. für Datenbanken und Sockets)
install(DIRECTORY DESTINATION db)
install(DIRECTORY DESTINATION socket)

# ---------------------------
# Tests mit GoogleTest aktivieren (optional)
# ---------------------------
include(CTest)
option(ENABLE_TESTS "Baue Unit-Tests mit GoogleTest" ON)

if (ENABLE_TESTS)
    find_package(GTest REQUIRED)
    enable_testing()

    add_executable(AdvancedCacheManagerTests tests/test_complete.cpp)
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

# ---------------------------
# CPack Konfiguration für Release
# ---------------------------
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    set(CPACK_PACKAGE_NAME "AdvancedCacheManager")
    set(CPACK_PACKAGE_VERSION "1.0.1")
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
