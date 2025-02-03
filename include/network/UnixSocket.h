#ifndef SOCKET_HANDLER_HPP
#define SOCKET_HANDLER_HPP

// System-Header
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <nlohmann/json.hpp>
#include "eventbus/EventBus.h"
#include "storage/Message.h"


using json = nlohmann::json;

class SocketHandler {
public:
    SocketHandler(const std::string& socketPath, EventBus& eventBus)
        : socketPath_(socketPath), eventBus_(eventBus), serverSocket_(-1)
    {}

    ~SocketHandler() {
        if (serverSocket_ != -1) {
            close(serverSocket_);
        }
        // Entferne den Socket-Dateipfad, falls vorhanden.
        unlink(socketPath_.c_str());
    }

    // Startet den Listener (blockierend)
    void run() {
        // Erstelle einen Unix–Socket
        serverSocket_ = socket(AF_UNIX, SOCK_STREAM, 0);
        if (serverSocket_ < 0) {
            perror("socket");
            return;
        }

        sockaddr_un addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, socketPath_.c_str(), sizeof(addr.sun_path) - 1);

        // Entferne ggf. vorhandene Socket-Datei
        unlink(socketPath_.c_str());

        if (bind(serverSocket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind");
            return;
        }

        if (listen(serverSocket_, 5) < 0) {
            perror("listen");
            return;
        }

        // Endlosschleife zum Akzeptieren von Client-Verbindungen
        while (true) {
            int clientSocket = accept(serverSocket_, nullptr, nullptr);
            if (clientSocket < 0) {
                perror("accept");
                continue;
            }

            std::cout << "new Client connection,  " << clientSocket << std::endl;
            // Starte einen neuen Thread zur Bearbeitung der Client-Verbindung
            std::thread(&SocketHandler::handleClient, this, clientSocket).detach();
        }
    }

private:
// Bearbeitet einen einzelnen Client und hält die Verbindung offen,
// bis der Client diese explizit schließt oder ein Fehler auftritt.
void handleClient(int clientSocket) {
    try {
        while (true) {
            std::string data;
            char buffer[1024];
            ssize_t n;

            // Lese die Anfrage (bis zum Socket-Schluss oder maximal 1024 Bytes pro Leseoperation)
            // Wir beenden die Lese-Schleife, sobald ein Newline ('\n') im Datenstrom auftaucht.
            while ((n = read(clientSocket, buffer, sizeof(buffer))) > 0) {
                data.append(buffer, n);
                if (data.find('\n') != std::string::npos)
                    break;
            }

            // Falls n <= 0 ist oder gar nichts gelesen wurde, hat der Client die Verbindung beendet
            if (n <= 0 || data.empty()) {
                std::cout << "Client hat die Verbindung beendet oder es ist ein Fehler aufgetreten." << std::endl;
                break;
            }

            std::cout << "Received Message: " << data.substr(0, 100) << "..." << std::endl;

            // Bearbeite die empfangene Nachricht
            try {
                json j = json::parse(data);
                std::string eventType = j.value("event", "");

                if (eventType == "SET") {
                    SetEventMessage msg;
                    msg.id = j.at("id").get<std::string>();
                    json flags = j.at("flags");
                    msg.persistent = flags.at("persistent").get<bool>();
                    msg.ttl = flags.at("ttl").get<int>();
                    msg.key = j.at("key").get<std::string>();
                    msg.value = j.at("value").get<std::string>();
                    msg.group = j.at("group").get<std::string>();

                    auto result = eventBus_.send<SetResponseMessage>(HandlerID::StorageHandler, msg).get();

                    json respJson;
                    respJson["id"] = result.id;
                    respJson["response"] = result.response;
                    std::string respStr = respJson.dump()+ "\n";

                    std::cout << "Sending response: " << respStr.substr(0, 100) << "..." << std::endl;
                    write(clientSocket, respStr.c_str(), respStr.size());

                } else if (eventType == "GET KEY") {
                    GetKeyEventMessage msg;
                    msg.id = j.at("id").get<std::string>();
                    msg.key = j.at("key").get<std::string>();

                    auto result = eventBus_.send<GetKeyResponseMessage>(HandlerID::StorageHandler, msg).get();

                    json respJson;
                    respJson["id"] = result.id;
                    respJson["response"] = result.response;
                    std::string respStr = respJson.dump()+ "\n";

                    std::cout << "Sending response: " << respStr.substr(0, 100) << "..." << std::endl;
                    write(clientSocket, respStr.c_str(), respStr.size());

                } else if (eventType == "GET GROUP") {
                    GetGroupEventMessage msg;
                    msg.id = j.value("id", "");
                    msg.group = j.at("group").get<std::string>();

                    auto result = eventBus_.send<GetGroupResponseMessage>(HandlerID::StorageHandler, msg).get();

                    json respJson;
                    respJson["id"] = result.id;
                    json arr = json::array();
                    for (const auto& kv : result.response) {
                        arr.push_back({ {"key", kv.key}, {"value", kv.value} });
                    }
                    respJson["response"] = arr;
                    std::string respStr = respJson.dump()+ "\n";

                    std::cout << "Sending response: " << respStr.substr(0, 100) << "..." << std::endl;
                    write(clientSocket, respStr.c_str(), respStr.size());

                } else if (eventType == "DELETE KEY") {
                    DeleteKeyEventMessage msg;
                    msg.id = j.at("id").get<std::string>();
                    msg.key = j.at("key").get<std::string>();

                    auto result = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::StorageHandler, msg).get();

                    json respJson;
                    respJson["id"] = result.id;
                    respJson["response"] = result.response;
                    std::string respStr = respJson.dump()+ "\n";

                    std::cout << "Sending response: " << respStr.substr(0, 100) << "..." << std::endl;
                    write(clientSocket, respStr.c_str(), respStr.size());

                } else if (eventType == "DELETE GROUP") {
                    DeleteGroupEventMessage msg;
                    msg.id = j.at("id").get<std::string>();
                    msg.group = j.at("group").get<std::string>();

                    auto result = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::StorageHandler, msg).get();

                    json respJson;
                    respJson["id"] = result.id;
                    respJson["response"] = result.response;
                    std::string respStr = respJson.dump()+ "\n";

                    std::cout << "Sending response: " << respStr.substr(0, 100) << "..." << std::endl;
                    write(clientSocket, respStr.c_str(), respStr.size());

                } else {
                    // Unbekannter Event-Typ
                    json errorJson;
                    errorJson["error"] = "Unbekannter event type";
                    std::string respStr = errorJson.dump()+ "\n";
                    write(clientSocket, respStr.c_str(), respStr.size());
                }
            }
            catch (const std::exception &e) {
                json errorJson;
                errorJson["error"] = e.what();
                std::string respStr = errorJson.dump() + "\n";

                std::cout << "Sending error response: " << respStr.substr(0, 100) << "..." << std::endl;
                write(clientSocket, respStr.c_str(), respStr.size());
            }
        }
    }
    catch (std::exception &e) {
        std::cerr << "Fehler in handleClient: " << e.what() << std::endl;
    }
    // Schließe den Socket, wenn die Schleife beendet wird
    close(clientSocket);
}

    std::string socketPath_;
    EventBus& eventBus_;
    int serverSocket_;
};
#endif // SOCKET_HANDLER_HPP
