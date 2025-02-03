#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <string>
#include <cstdlib>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <nlohmann/json.hpp>
#include <filesystem>

// Projekt‑spezifische Header (achte auf korrekte Pfade in deinem Projekt)
#include "config/ConfigHandler.h"
#include "eventbus/EventBus.h"
#include "storage/RamHandler.h"
#include "storage/DiskHandler.h"
#include "storage/StorageHandler.h"
#include "network/UnixSocket.h" // Enthält unter anderem die Definition von SocketHandler

namespace fs = std::filesystem;
using json = nlohmann::json;

// Hilfsfunktion, die eine JSON-Anfrage über den Unix‑Socket sendet und die Antwort zurückgibt.
std::string sendRequest(const std::string& socketPath, const std::string& request) {
    int clientSock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (clientSock < 0) {
        perror("socket");
        std::exit(EXIT_FAILURE);
    }

    sockaddr_un addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    if (connect(clientSock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("connect");
        close(clientSock);
        std::exit(EXIT_FAILURE);
    }

    // Füge ein Newline-Zeichen hinzu, falls der Server dies als Trenner erwartet.
    std::string reqWithNewline = request + "\n";
    ssize_t sent = write(clientSock, reqWithNewline.c_str(), reqWithNewline.size());
    if (sent < 0) {
        perror("write");
        close(clientSock);
        std::exit(EXIT_FAILURE);
    }

    std::string response;
    char buffer[1024];
    ssize_t n;
    while ((n = read(clientSock, buffer, sizeof(buffer))) > 0) {
        response.append(buffer, n);
    }
    close(clientSock);
    return response;
}

// Funktion zum Starten des Servers in einem separaten Thread.
// Hier wird die Konfiguration geladen, die eventuell vorhandene Datenbank gelöscht und
// die Handler initialisiert.
void startServer() {
    fs::path configFile = fs::absolute("config.json");
    try {
        std::cout << "Lade Konfiguration von: " << configFile << std::endl;
        ConfigHandler configHandler(configFile.string());
        const Config& config = configHandler.getConfig();

        std::cout << "Konfiguration geladen:" << std::endl;
        std::cout << "  RAM max size (MB): " << config.maxSizeMB << std::endl;
        std::cout << "  Disk DB file:      " << config.dbFile << std::endl;
        std::cout << "  Socket path:       " << config.socketPath << std::endl;

        // Falls die Datenbank bereits existiert, lösche sie
        fs::path dbPath = config.dbFile;
        if (fs::exists(dbPath)) {
            std::cout << "Alte Datenbank " << dbPath << " gefunden. Lösche sie." << std::endl;
            fs::remove(dbPath);
        }

        // Initialisiere die benötigten Handler
        EventBus eventBus;
        RamHandler ramHandler(eventBus, config.maxSizeMB);
        DiskHandler diskHandler(eventBus, config.dbFile);
        StorageHandler storageHandler(eventBus);
        SocketHandler socketHandler(config.socketPath, eventBus);

        std::cout << "AdvancedCacheManager startet..." << std::endl;
        // Blockierender Aufruf: Wartet auf Client-Verbindungen.
        socketHandler.run();
    }
    catch (const std::exception& ex) {
        std::cerr << "Fehler beim Laden der Konfiguration: " << ex.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main() {
    // Starte den Server in einem eigenen Thread.
    std::thread serverThread(startServer);

    // Warte kurz, damit der Server gestartet ist (100-200 ms können ausreichend sein)
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verwende den in der Konfiguration festgelegten Socket-Pfad.
    const std::string socketPath = "/tmp/cache_socket";

    try {
        // -----------------------------
        // Test 1: SET-Event Persistent
        // -----------------------------
        {
            json req = {
                {"id", "test1"},
                {"event", "SET"},
                {"flags", {{"persistent", true}, {"ttl", 3600}}},
                {"key", "keyPersistent"},
                {"value", "valuePersistent"},
                {"group", "groupPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test1 - SET Persistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test1");
            assert(resp["response"] == true);
        }

        // -----------------------------
        // Test 2: SET-Event NonPersistent
        // -----------------------------
        {
            json req = {
                {"id", "test2"},
                {"event", "SET"},
                {"flags", {{"persistent", false}, {"ttl", 3600}}},
                {"key", "keyNonPersistent"},
                {"value", "valueNonPersistent"},
                {"group", "groupNonPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test2 - SET NonPersistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test2");
            assert(resp["response"] == true);
        }

        // -----------------------------
        // Test 3: GET KEY-Event Persistent (exists)
        // -----------------------------
        {
            json req = {
                {"id", "test3"},
                {"event", "GET KEY"},
                {"key", "keyPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test3 - GET KEY Persistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test3");
            assert(resp["response"] == "valuePersistent");
        }

        // -----------------------------
        // Test 4: GET KEY-Event NonPersistent (exists)
        // -----------------------------
        {
            json req = {
                {"id", "test4"},
                {"event", "GET KEY"},
                {"key", "keyNonPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test4 - GET KEY NonPersistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test4");
            assert(resp["response"] == "valueNonPersistent");
        }

        // -----------------------------
        // Test 5: GET GROUP-Event für Gruppe "groupPersistent"
        // -----------------------------
        {
            json req = {
                {"id", "test5"},
                {"event", "GET GROUP"},
                {"group", "groupPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test5 - GET GROUP Persistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test5");
            assert(resp["response"].is_array());
            assert(resp["response"].size() == 1);
            json kv = resp["response"][0];
            assert(kv["key"] == "keyPersistent");
            assert(kv["value"] == "valuePersistent");
        }

        // -----------------------------
        // Test 6: GET GROUP-Event für Gruppe "groupNonPersistent"
        // -----------------------------
        {
            json req = {
                {"id", "test6"},
                {"event", "GET GROUP"},
                {"group", "groupNonPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test6 - GET GROUP NonPersistent Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test6");
            assert(resp["response"].is_array());
            assert(resp["response"].size() == 1);
            json kv = resp["response"][0];
            assert(kv["key"] == "keyNonPersistent");
            assert(kv["value"] == "valueNonPersistent");
        }

        // -----------------------------
        // Test 7: SET-Event Persistent Update (Änderung des Wertes und der Gruppe)
        // -----------------------------
        {
            json req = {
                {"id", "test7"},
                {"event", "SET"},
                {"flags", {{"persistent", true}, {"ttl", 3600}}},
                {"key", "keyPersistent"},
                {"value", "valuePersistent_1"},
                {"group", "groupA"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test7 - SET Persistent Update Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test7");
            assert(resp["response"] == true);
        }

        // -----------------------------
        // Test 8: SET-Event NonPersistent Update (Änderung des Wertes und der Gruppe)
        // -----------------------------
        {
            json req = {
                {"id", "test8"},
                {"event", "SET"},
                {"flags", {{"persistent", false}, {"ttl", 3600}}},
                {"key", "keyNonPersistent"},
                {"value", "valueNonPersistent_1"},
                {"group", "groupA"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test8 - SET NonPersistent Update Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test8");
            assert(resp["response"] == true);
        }

        // -----------------------------
        // Test 9: GET GROUP-Event für Gruppe "groupA"
        // -----------------------------
        {
            json req = {
                {"id", "test9"},
                {"event", "GET GROUP"},
                {"group", "groupA"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test9 - GET GROUP groupA Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test9");
            assert(resp["response"].is_array());
            // Es sollten zwei Einträge zurückgeliefert werden.
            assert(resp["response"].size() == 2);
            // Prüfe, ob beide Keys vorhanden sind:
            bool foundPersistent = false, foundNonPersistent = false;
            for (const auto& entry : resp["response"]) {
                if (entry["key"] == "keyPersistent" && entry["value"] == "valuePersistent_1") {
                    foundPersistent = true;
                }
                if (entry["key"] == "keyNonPersistent" && entry["value"] == "valueNonPersistent_1") {
                    foundNonPersistent = true;
                }
            }
            assert(foundPersistent && foundNonPersistent);
        }

        // -----------------------------
        // Test 10a: DELETE KEY-Event (bestehender Key)
        // -----------------------------
        {
            json req = {
                {"id", "test10a"},
                {"event", "DELETE KEY"},
                {"key", "keyPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test10 - DELETE KEY Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test10a");
            // Bei erfolgreichem Löschen sollte 1 zurückgegeben werden.
            assert(resp["response"] == 1);
        }

        // -----------------------------
        // Test 10b: DELETE KEY-Event (bestehender Key)
        // -----------------------------
        {
            json req = {
                {"id", "test10b"},
                {"event", "DELETE KEY"},
                {"key", "keyNonPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test10 - DELETE KEY Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test10b");
            // Bei erfolgreichem Löschen sollte 1 zurückgegeben werden.
            assert(resp["response"] == 1);
        }

        // -----------------------------
        // Test 11a: DELETE KEY-Event (nicht vorhandener Key)
        // -----------------------------
        {
            json req = {
                {"id", "test11a"},
                {"event", "DELETE KEY"},
                {"key", "keyPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test11 - DELETE KEY (nicht vorhanden) Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test11a");
            // Da der Key nicht existiert, sollte 0 zurückgegeben werden.
            assert(resp["response"] == 0);
        }

        // -----------------------------
        // Test 11b: DELETE KEY-Event (nicht vorhandener Key)
        // -----------------------------
        {
            json req = {
                {"id", "test11b"},
                {"event", "DELETE KEY"},
                {"key", "keyNonPersistent"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test11 - DELETE KEY (nicht vorhanden) Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test11b");
            // Da der Key nicht existiert, sollte 0 zurückgegeben werden.
            assert(resp["response"] == 0);
        }

        // -----------------------------
        // Test 12a: SET MULTIPLE-Event (Setze 10 Key Values")
        // -----------------------------
        {
            for (int i=0; i < 10; i++) {
                json req = {
                        {"id", "test_" + std::to_string(i)},
                        {"event", "SET"},
                        {"flags", {{"persistent", (i%2==0)}, {"ttl", 3600}}},
                        {"key", "key_" + std::to_string(i)},
                        {"value", "value_" + std::to_string(i)},
                        {"group", "groupB"}
                };
                std::string respStr = sendRequest(socketPath, req.dump());
                json resp = json::parse(respStr);
                std::cout << "Test12 - SET MULTIPLE Response: " << resp.dump() << std::endl;

                assert(resp["id"] == "test_" + std::to_string(i));
                assert(resp["response"] == true);
            }
        }

        // -----------------------------
        // Test 12b: DELETE GROUP-Event (Lösche Gruppe "groupA")
        // -----------------------------
        {
            json req = {
                {"id", "test12b"},
                {"event", "DELETE GROUP"},
                {"group", "groupB"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test12 - DELETE GROUP Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test12b");
            // Es sollten 1 oder 2 Einträge gelöscht werden, je nach Implementierung.
            // Hier wird vorausgesetzt, dass es mindestens einen Eintrag gibt.
            assert(resp["response"].get<int>() == 10);
        }

        // -----------------------------
        // Test 13: GET KEY-Event für einen Key, der gelöscht wurde
        // -----------------------------
        {
            json req = {
                {"id", "test13"},
                {"event", "GET GROUP"},
                {"group", "groupB"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test13 - GET KEY nach DELETE Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test13");
            assert(resp["response"].size() == 0);
        }

        // -----------------------------
        // Test 14: Ungültiger Event-Typ
        // -----------------------------
        {
            json req = {
                {"id", "test14"},
                {"event", "UNKNOWN EVENT"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test14 - Unknown Event Response: " << resp.dump() << std::endl;
            // Hier gehen wir davon aus, dass ein Fehler zurückgemeldet wird.
            assert(resp.contains("error"));
        }

        // -----------------------------
        // Test 15: SET-Event ohne erforderliche Felder (z. B. key fehlt)
        // -----------------------------
        {
            json req = {
                {"id", "test15"},
                {"event", "SET"},
                {"flags", {{"persistent", true}, {"ttl", 3600}}},
                // {"key", "missingKey"}, // fehlt absichtlich
                {"value", "someValue"},
                {"group", "someGroup"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test15 - SET ohne Key Response: " << resp.dump() << std::endl;
            // Je nach Implementierung erwarten wir einen Fehler
            assert(resp.contains("error"));
        }

        // -----------------------------
        // Test 16: GET KEY-Event ohne erforderliches Feld (z. B. key fehlt)
        // -----------------------------
        {
            json req = {
                {"id", "test16"},
                {"event", "GET KEY"}
                // key fehlt
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test16 - GET KEY ohne Key Response: " << resp.dump() << std::endl;
            assert(resp.contains("error"));
        }

        // -----------------------------
        // Test 17: Ungültige JSON-Nachricht
        // -----------------------------
        {
            json req = "qwe";
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test17 - Ungültige JSON-Nachricht: " << resp.dump() << std::endl;
            assert(resp.contains("error"));
        }

        // -----------------------------
        // Test 18: TTL-Test
        // -----------------------------
        {
            // SET-Event mit einer kurzen TTL (2 Sekunden)
            json reqTTLSet = {
                {"id", "test18_set"},
                {"event", "SET"},
                {"flags", {{"persistent", false}, {"ttl", 2}}},
                {"key", "keyTTL"},
                {"value", "valueTTL"},
                {"group", "groupTTL"}
            };
            std::string respTTLSetStr = sendRequest(socketPath, reqTTLSet.dump());
            json respTTLSet = json::parse(respTTLSetStr);
            std::cout << "Test18 - TTL SET Response: " << respTTLSet.dump() << std::endl;
            assert(respTTLSet["id"] == "test18_set");
            assert(respTTLSet["response"] == true);

            // Warte länger als die TTL (z. B. 3 Sekunden)
            std::this_thread::sleep_for(std::chrono::seconds(5));

            // GET KEY-Event: Der Key "keyTTL" sollte nun nicht mehr verfügbar sein.
            json reqTTLGet = {
                {"id", "test18_get"},
                {"event", "GET KEY"},
                {"key", "keyTTL"}
            };
            std::string respTTLGetStr = sendRequest(socketPath, reqTTLGet.dump());
            json respTTLGet = json::parse(respTTLGetStr);
            std::cout << "Test18 - TTL GET Response: " << respTTLGet.dump() << std::endl;
            assert(respTTLGet["id"] == "test18_get");
            // Hier gehen wir davon aus, dass nach Ablauf der TTL ein leerer String zurückkommt.
            assert(respTTLGet["response"] == "");
        }

        // -----------------------------
        // Test 19: Multiple gleichzeitige Clients
        // -----------------------------
        {
            const int numClients = 20;
            std::vector<std::thread> clientThreads;
            std::mutex printMutex; // zum synchronisierten Ausgeben
            for (int i = 0; i < numClients; i++) {
                clientThreads.emplace_back([i, socketPath, &printMutex](){
                    // Jeder Client setzt einen eigenen Key und ruft ihn anschließend ab.
                    std::string idSet = "multi_set_" + std::to_string(i);
                    std::string key = "multi_key_" + std::to_string(i);
                    std::string value = "multi_value_" + std::to_string(i);
                    json reqSet = {
                        {"id", idSet},
                        {"event", "SET"},
                        {"flags", {{"persistent", true}, {"ttl", 3600}}},
                        {"key", key},
                        {"value", value},
                        {"group", "multiGroup"}
                    };
                    std::string respSetStr = sendRequest(socketPath, reqSet.dump());
                    json respSet = json::parse(respSetStr);
                    {
                        std::lock_guard<std::mutex> lock(printMutex);
                        std::cout << "Test19 - Client " << i << " SET Response: " << respSet.dump() << std::endl;
                    }
                    assert(respSet["response"] == true);

                    // GET-Request des gleichen Keys
                    std::string idGet = "multi_get_" + std::to_string(i);
                    json reqGet = {
                        {"id", idGet},
                        {"event", "GET KEY"},
                        {"key", key}
                    };
                    std::string respGetStr = sendRequest(socketPath, reqGet.dump());
                    json respGet = json::parse(respGetStr);
                    {
                        std::lock_guard<std::mutex> lock(printMutex);
                        std::cout << "Test19 - Client " << i << " GET Response: " << respGet.dump() << std::endl;
                    }
                    assert(respGet["response"] == value);
                });
            }
            // Warte, bis alle Threads abgeschlossen sind.
            for (auto &t : clientThreads) {
                if (t.joinable()) {
                    t.join();
                }
            }
        }

        // -----------------------------
        // Test 20: RAM Eviction
        // -----------------------------
        {
            // Wir definieren hier 12 Einträge, um über die 10 MB-Grenze zu kommen.
            int numEntries = 12;
            // Erzeugen eines großen Wertes von ca. 1 MB.
            std::string largeValue(1024 * 1024, 'A'); // 1 MB großer String

            for (int i = 0; i < numEntries; i++) {
                json req = {
                    {"id", "ram_set_" + std::to_string(i)},
                    {"event", "SET"},
                    {"flags", {{"persistent", false}, {"ttl", 3600}}},
                    {"key", "ram_key_" + std::to_string(i)},
                    {"value", largeValue},
                    {"group", "ramGroup"}
                };
                std::string respStr = sendRequest(socketPath, req.dump());
                json resp = json::parse(respStr);
                std::cout << "Test20 - RAM Eviction SET " << i << " Response: " << resp.dump() << std::endl;
                assert(resp["response"] == true);
            }

            // Warte 5 Sekunden
            std::this_thread::sleep_for(std::chrono::seconds(5));

            for (int i = 0; i < numEntries; i++) {
                json req = {
                    {"id", "ram_get_" + std::to_string(i)},
                    {"event", "GET KEY"},
                    {"key", "ram_key_" + std::to_string(i)}
                };
                std::string respStr = sendRequest(socketPath, req.dump());
                json resp = json::parse(respStr);
                std::cout << "Test20 - RAM Eviction GET " << i << std::endl;
                if (i < 3) { // Angenommen, diese Keys wurden evakuiert
                    assert(resp["response"] == "");
                } else {
                    std::string expectedValue = largeValue;
                    assert(resp["response"] == expectedValue);
                }
            }
        }

        // -----------------------------
        // Test 21: Compressed value test
        // -----------------------------
        {
            // Hinweis: Hier verwenden wir einen Beispiel-String, der einen "komprimierten" Wert simulieren soll.
            // Falls du echte Binärdaten testest, solltest du diese vorher z.B. in Base64 kodieren.
            json req = {
                {"id", "test21"},
                {"event", "SET"},
                {"flags", {{"persistent", true}, {"ttl", 3600}}},
                {"key", "compressed"},
                {"value", "\"Mdp�  �RAMAlamaD1ngD0ng    i�*"},
                {"group", "compressed"}
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test21 - SET Compressed Response: " << resp.dump() << std::endl;
            assert(resp["id"] == "test21");
            assert(resp["response"] == true);
        }

        // -----------------------------
        // Test 22: GET KEY mit Key als empty value
        // -----------------------------
        {
            json req = {
                {"id", "test22"},
                {"event", "GET KEY"},
                {"key", ""},
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test22 - GET KEY with empty key Response: " << resp.dump() << std::endl;
            // Es wird erwartet, dass der Server einen Fehler zurückliefert, da ein leerer Key nicht gültig ist.
            assert(resp.contains("error"));
        }

        // -----------------------------
        // Test 22: GET GROUP mit Group als empty value
        // -----------------------------
        {
            json req = {
                {"id", "test22"},
                {"event", "GET GROUP"},
                {"group", ""},
            };
            std::string respStr = sendRequest(socketPath, req.dump());
            json resp = json::parse(respStr);
            std::cout << "Test22 - GET GROUP with empty group Response: " << resp.dump() << std::endl;
            // Es wird erwartet, dass der Server einen Fehler zurückliefert, da ein leerer Key nicht gültig ist.
            assert(resp.contains("error"));
        }

        std::cout << "Alle erweiterten Client-Tests erfolgreich bestanden!" << std::endl;
    }
    catch (const std::exception& ex) {
        std::cerr << "Testfehler: " << ex.what() << std::endl;
    }

    // Da der Server-Thread in einer Endlosschleife läuft, beenden wir das Programm nun.
    // In einem echten Test-Szenario sollten geeignete Mechanismen zur Terminierung implementiert werden.
    std::exit(EXIT_SUCCESS);
}
