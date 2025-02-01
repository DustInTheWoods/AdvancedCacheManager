// SocketHandlerTest.cpp

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <memory>
#include <string>
#include <nlohmann/json.hpp>
#include "eventbus/EventBus.h"
#include "network/UnixSocket.h"

using json = nlohmann::json;

// --- Test Fixture ---
class SocketHandlerTest : public ::testing::Test {
protected:
    // Verwende einen temporären Socket-Pfad
    std::string socketPath = "/tmp/test_socket_handler.sock";
    EventBus eventBus;
    std::unique_ptr<SocketHandler> socketHandler;
    std::thread serverThread;

    virtual void SetUp() override {
        // Registriere Dummy-Handler für den StorageHandler (HandlerID::StorageHandler)
        // SET EVENT
        eventBus.subscribe<SetEventMessage, SetResponseMessage>(
            HandlerID::StorageHandler,
            [](const SetEventMessage& msg) -> SetResponseMessage {
                SetResponseMessage resp;
                resp.id = msg.id;
                resp.response = true;
                return resp;
            }
        );

        // GET KEY EVENT
        eventBus.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(
            HandlerID::StorageHandler,
            [](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
                GetKeyResponseMessage resp;
                resp.id = msg.id;
                resp.response = "dummy_value";
                return resp;
            }
        );

        // GET GROUP EVENT
        eventBus.subscribe<GetGroupEventMessage, GetGroupResponseMessage>(
            HandlerID::StorageHandler,
            [](const GetGroupEventMessage& msg) -> GetGroupResponseMessage {
                GetGroupResponseMessage resp;
                resp.id = msg.id;
                // Für Testzwecke: ein einzelnes Key/Value-Paar
                resp.response.push_back({"Hallo", "world!"});
                return resp;
            }
        );

        // DELETE KEY EVENT
        eventBus.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(
            HandlerID::StorageHandler,
            [](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
                DeleteKeyResponseMessage resp;
                resp.id = msg.id;
                resp.response = 1;
                return resp;
            }
        );

        // DELETE GROUP EVENT
        eventBus.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(
            HandlerID::StorageHandler,
            [](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
                DeleteGroupResponseMessage resp;
                resp.id = msg.id;
                resp.response = 1;
                return resp;
            }
        );

        // Erstelle den SocketHandler und starte ihn in einem eigenen Thread.
        socketHandler = std::make_unique<SocketHandler>(socketPath, eventBus);
        serverThread = std::thread([this]() {
            socketHandler->run();
        });

        // Kurze Wartezeit, damit der Server gestartet ist.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    virtual void TearDown() override {
        // Versuche, den Server zu stoppen.
        // Da der SocketHandler in einer Endlosschleife läuft, wird hier durch den Destruktor
        // und das Entfernen der Socket-Datei "aufgeräumt".
        if (socketHandler) {
            socketHandler.reset();
        }
        if (serverThread.joinable()) {
            // In diesem Beispiel lässt sich der Thread nicht sauber beenden.
            // Daher detach-en wir ihn, da der Prozess beim Testende sowieso terminiert.
            serverThread.detach();
        }
        // Entferne die temporäre Socket-Datei.
        unlink(socketPath.c_str());
    }

    // Hilfsfunktion: Sendet eine Anfrage an den Unix-Socket und gibt die Antwort zurück.
    std::string sendRequest(const std::string& request) {
        int clientSock = socket(AF_UNIX, SOCK_STREAM, 0);
        if (clientSock < 0) {
            throw std::runtime_error("Failed to create client socket");
        }

        sockaddr_un addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

        if (connect(clientSock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(clientSock);
            throw std::runtime_error("Failed to connect to server socket");
        }

        // Sende die Anfrage.
        std::string requestWithNewline = request + "\n";
        ssize_t sent = write(clientSock, requestWithNewline.c_str(), requestWithNewline.size());
        if (sent < 0) {
            close(clientSock);
            throw std::runtime_error("Failed to write to server socket");
        }

        // Lese die Antwort.
        std::string response;
        char buffer[1024];
        ssize_t n;
        while ((n = read(clientSock, buffer, sizeof(buffer))) > 0) {
            response.append(buffer, n);
        }
        close(clientSock);
        return response;
    }
};

// --- Testfälle ---

// Test 1: SET Event
TEST_F(SocketHandlerTest, TestSetEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "SET"},
        {"flags", {{"persistent", true}, {"ttl", 3600}}},
        {"key", "unique_key"},
        {"value", "example_value"},
        {"group", "example_group"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    EXPECT_EQ(respJson["id"], "qwe123");
    EXPECT_TRUE(respJson["response"].get<bool>());
}

// Test 2: GET KEY Event
TEST_F(SocketHandlerTest, TestGetKeyEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "GET KEY"},
        {"key", "unique_key"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    EXPECT_EQ(respJson["id"], "qwe123");
    EXPECT_EQ(respJson["response"], "dummy_value");
}

// Test 3: GET GROUP Event
TEST_F(SocketHandlerTest, TestGetGroupEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "GET GROUP"},
        {"group", "example_group"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    EXPECT_EQ(respJson["id"], "qwe123");
    ASSERT_TRUE(respJson["response"].is_array());
    ASSERT_EQ(respJson["response"].size(), 1);
    json kv = respJson["response"][0];
    EXPECT_EQ(kv["key"], "Hallo");
    EXPECT_EQ(kv["value"], "world!");
}

// Test 4: DELETE KEY Event
TEST_F(SocketHandlerTest, TestDeleteKeyEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "DELETE KEY"},
        {"key", "unique_key"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    EXPECT_EQ(respJson["id"], "qwe123");
    EXPECT_EQ(respJson["response"], 1);
}

// Test 5: DELETE GROUP Event
TEST_F(SocketHandlerTest, TestDeleteGroupEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "DELETE GROUP"},
        {"group", "example_group"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    EXPECT_EQ(respJson["id"], "qwe123");
    EXPECT_EQ(respJson["response"], 1);
}

// Test 6: Unbekannter Event-Typ
TEST_F(SocketHandlerTest, TestUnknownEvent) {
    json req = {
        {"id", "qwe123"},
        {"event", "UNKNOWN EVENT"}
    };

    std::string response = sendRequest(req.dump());
    json respJson = json::parse(response);
    ASSERT_TRUE(respJson.contains("error"));
    EXPECT_EQ(respJson["error"], "Unbekannter event type");
}

// --- Performance-Test ---
// In diesem Test werden viele Anfragen hintereinander gesendet, um die durchschnittliche Verarbeitungszeit und
// die Verarbeitungsgeschwindigkeit des SocketHandlers zu messen.
TEST_F(SocketHandlerTest, PerformanceTest) {
    const int numEvents = 1000; // Anzahl der zu sendenden Events; passe diesen Wert je nach Bedarf an.

    // Verwende beispielsweise das GET KEY Event für den Performance-Test.
    json req = {
        {"id", "perf_test"},
        {"event", "GET KEY"},
        {"key", "unique_key"}
    };

    // Optional: Warte kurz, um den Server stabilisiert zu haben.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        std::string response = sendRequest(req.dump());
        json respJson = json::parse(response);
        // Optional: Überprüfe, ob die Antwort den erwarteten Wert enthält.
        EXPECT_EQ(respJson["id"], "perf_test");
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "\nPerformanceTest:" << std::endl;
    std::cout << "  Anzahl der Events: " << numEvents << std::endl;
    std::cout << "  Gesamtdauer: " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "  Durchschnittliche Dauer pro Event: " << nsPerEvent << " ns" << std::endl;
    std::cout << "  Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
}
