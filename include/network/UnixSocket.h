#ifndef SOCKET_HANDLER_HPP
#define SOCKET_HANDLER_HPP

// System Headers
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <nlohmann/json.hpp>
#include "eventbus/EventBus.h"
#include "storage/Message.h"

using json = nlohmann::json;

// Logging macros with a consistent layout.
#define LOG_INFO(component, message) \
    std::cout <<"[INFO]" << " [" << component << "] " << message << std::endl;

#define LOG_ERROR(component, message) \
    std::cout <<"[ERROR]" << " [" << component << "] " << message << std::endl;

class SocketHandler {
public:
    SocketHandler(const std::string& socketPath, EventBus& eventBus)
        : socketPath_(socketPath), eventBus_(eventBus), serverSocket_(-1)
    {}

    ~SocketHandler() {
        if (serverSocket_ != -1) {
            close(serverSocket_);
        }
        // Remove the socket file if it exists.
        unlink(socketPath_.c_str());
    }

    // Runs the listener (blocking call)
    void run() {
        // Create a Unix socket
        serverSocket_ = socket(AF_UNIX, SOCK_STREAM, 0);
        if (serverSocket_ < 0) {
            perror("socket");
            return;
        }

        sockaddr_un addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        std::strncpy(addr.sun_path, socketPath_.c_str(), sizeof(addr.sun_path) - 1);

        // Remove any existing socket file.
        unlink(socketPath_.c_str());

        if (bind(serverSocket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind");
            return;
        }

        if (listen(serverSocket_, 5) < 0) {
            perror("listen");
            return;
        }

        LOG_INFO("SocketHandler", "Socket bound and listening on " << socketPath_);

        // Loop indefinitely to accept client connections.
        while (true) {
            int clientSocket = accept(serverSocket_, nullptr, nullptr);
            if (clientSocket < 0) {
                perror("accept");
                continue;
            }

            LOG_INFO("SocketHandler", "New client connection accepted. Socket: " << clientSocket);
            // Launch a new thread to handle the client connection.
            std::thread(&SocketHandler::handleClient, this, clientSocket).detach();
        }
    }

private:
    // Handles an individual client connection and keeps it open until the client explicitly closes it or an error occurs.
    void handleClient(int clientSocket) {
        try {
            while (true) {
                std::string data;
                char buffer[1024];
                ssize_t n;

                // Read the request (until socket closure or up to 1024 bytes per read).
                // The read loop stops when a newline ('\n') is encountered.
                while ((n = read(clientSocket, buffer, sizeof(buffer))) > 0) {
                    data.append(buffer, n);
                    if (data.find('\n') != std::string::npos)
                        break;
                }

                // If n <= 0 or nothing was read, the client closed the connection or an error occurred.
                if (n <= 0 || data.empty()) {
                    LOG_INFO("SocketHandler", "Client closed the connection or an error occurred.");
                    break;
                }

                LOG_INFO("SocketHandler", "Received message: " << data.substr(0, 100) << "...");

                // Process the received message.
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
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
                        write(clientSocket, respStr.c_str(), respStr.size());

                    } else if (eventType == "GET KEY") {
                        GetKeyEventMessage msg;
                        msg.id = j.at("id").get<std::string>();
                        msg.key = j.at("key").get<std::string>();

                        auto result = eventBus_.send<GetKeyResponseMessage>(HandlerID::StorageHandler, msg).get();

                        json respJson;
                        respJson["id"] = result.id;
                        respJson["response"] = result.response;
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
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
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
                        write(clientSocket, respStr.c_str(), respStr.size());

                    } else if (eventType == "DELETE KEY") {
                        DeleteKeyEventMessage msg;
                        msg.id = j.at("id").get<std::string>();
                        msg.key = j.at("key").get<std::string>();

                        auto result = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::StorageHandler, msg).get();

                        json respJson;
                        respJson["id"] = result.id;
                        respJson["response"] = result.response;
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
                        write(clientSocket, respStr.c_str(), respStr.size());

                    } else if (eventType == "DELETE GROUP") {
                        DeleteGroupEventMessage msg;
                        msg.id = j.at("id").get<std::string>();
                        msg.group = j.at("group").get<std::string>();

                        auto result = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::StorageHandler, msg).get();

                        json respJson;
                        respJson["id"] = result.id;
                        respJson["response"] = result.response;
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
                        write(clientSocket, respStr.c_str(), respStr.size());

                    } else if (eventType == "LIST") {
                        ListEventMessage msg;
                        msg.id = j.at("id").get<std::string>();

                        auto result = eventBus_.send<ListEventReponseMessage>(HandlerID::StorageHandler, msg).get();

                        json respJson;
                        respJson["id"] = result.id;
                        json arr = json::array();
                        for (const auto& kv : result.response) {
                            arr.push_back({ {"key", kv.key}, {"value", kv.value}, {"group", kv.group}});
                        }

                        respJson["response"] = arr;
                        std::string respStr = respJson.dump() + "\n";

                        LOG_INFO("SocketHandler", "Sending response: " << respStr.substr(0, 100) << "...");
                        write(clientSocket, respStr.c_str(), respStr.size());
                    } else {
                        // Unknown event type
                        json errorJson;
                        errorJson["error"] = "Unknown event type";
                        std::string respStr = errorJson.dump() + "\n";
                        LOG_ERROR("SocketHandler", "Unknown event type received.");
                        write(clientSocket, respStr.c_str(), respStr.size());
                    }
                }
                catch (const std::exception &e) {
                    json errorJson;
                    errorJson["error"] = e.what();
                    std::string respStr = errorJson.dump() + "\n";
                    LOG_ERROR("SocketHandler", "Exception caught while processing message: " << e.what());
                    write(clientSocket, respStr.c_str(), respStr.size());
                }
            }
        }
        catch (std::exception &e) {
            LOG_ERROR("SocketHandler", "Error in handleClient: " << e.what());
        }
        // Close the client socket when done.
        LOG_INFO("SocketHandler", "Closing connection to client socket: " << clientSocket);
        close(clientSocket);
    }

    std::string socketPath_;
    EventBus& eventBus_;
    int serverSocket_;
};

#endif // SOCKET_HANDLER_HPP