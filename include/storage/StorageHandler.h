#ifndef STORAGEHANDLER_H
#define STORAGEHANDLER_H

#include "eventbus/EventBus.h"
#include "storage/Message.h"  // Contains definitions for SetEventMessage, SetResponseMessage, etc.
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <stdexcept>

// Logging macros with a consistent layout.
#define LOG_INFO(component, message) \
std::cout <<"[INFO]" << " [" << component << "] " << message << std::endl;

#define LOG_ERROR(component, message) \
std::cout <<"[ERROR]" << " [" << component << "] " << message << std::endl;

/*
  StorageHandler receives requests (SET, GET, DELETE) via the EventBus
  and forwards them to the RamHandler and DiskHandler.
  This way, modifications are stored both in fast, volatile memory (RAM)
  and persistently (on disk).
*/
class StorageHandler {
public:
    explicit StorageHandler(EventBus& eventBus)
        : eventBus_(eventBus)
    {
        // Register the handler functions for Storage events with the EventBus.
        eventBus_.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::StorageHandler,
            [this](const SetEventMessage& msg) -> SetResponseMessage {
                return handleSetEvent(msg);
            }
        );

        eventBus_.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::StorageHandler,
            [this](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
                return handleGetKeyEvent(msg);
            }
        );

        eventBus_.subscribe<GetGroupEventMessage, GetGroupResponseMessage>(HandlerID::StorageHandler,
            [this](const GetGroupEventMessage& msg) -> GetGroupResponseMessage {
                return handleGetGroupEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(HandlerID::StorageHandler,
            [this](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
                return handleDeleteKeyEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(HandlerID::StorageHandler,
            [this](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
                return handleDeleteGroupEvent(msg);
            }
        );

        eventBus_.subscribe<ListEventMessage, ListEventReponseMessage>(HandlerID::StorageHandler,
            [this](const ListEventMessage& msg) -> ListEventReponseMessage {
                return handleListEvent(msg);
            }
        );

        LOG_INFO("StorageHandler", "Initialized and subscribed to events.");
    }

    // SET event: Forwards the request to RAM or Disk depending on persistence flag.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        if (msg.key.empty() || msg.value.empty()) {
            LOG_ERROR("StorageHandler", "SetEventMessage received empty event.");
            throw std::runtime_error("Invalid key or value.");
        }

        if (msg.persistent) {
            LOG_INFO("StorageHandler", "Forwarding SET request to DiskHandler for key: " << msg.key);
            // Send request to DiskHandler.
            auto diskResult = eventBus_.send<SetResponseMessage>(HandlerID::DiskHandler, msg);
            SetResponseMessage diskResp = diskResult.get();
            diskResp.id = msg.id;
            return diskResp;
        } else {
            LOG_INFO("StorageHandler", "Forwarding SET request to RamHandler for key: " << msg.key);
            // Send request to RamHandler.
            auto ramResult = eventBus_.send<SetResponseMessage>(HandlerID::RamHandler, msg);
            SetResponseMessage ramResp = ramResult.get();
            ramResp.id = msg.id;
            return ramResp;
        }
    }

    // GET KEY event: First searches in RAM; if not found, then queries the DiskHandler.
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        if (msg.key.empty()) {
            LOG_ERROR("StorageHandler", "GetKeyResponseMessage key is empty.");
            throw std::invalid_argument("Invalid key name");
        }

        auto ramResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::RamHandler, msg);
        GetKeyResponseMessage ramResp = ramResult.get();
        if (!ramResp.response.empty()) {
            LOG_INFO("StorageHandler", "Key '" << msg.key << "' found in RamHandler.");
            return ramResp;
        } else {
            LOG_INFO("StorageHandler", "Key '" << msg.key << "' not found in RAM; querying DiskHandler.");
            // Fallback: query DiskHandler.
            auto diskResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::DiskHandler, msg);
            GetKeyResponseMessage diskResp = diskResult.get();
            if (!diskResp.response.empty()) {
                LOG_INFO("StorageHandler", "Key '" << msg.key << "' found in DiskHandler.");
            } else {
                LOG_INFO("StorageHandler", "Key '" << msg.key << "' not found in DiskHandler either.");
            }
            return diskResp;
        }
    }

    // GET GROUP event: Searches both RAM and Disk and combines the results.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        if (msg.group.empty()) {
            LOG_ERROR("StorageHandler", "GetGroupResponseMessage group is empty.");
            throw std::invalid_argument("Invalid group name");
        }

        auto ramResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::DiskHandler, msg);

        GetGroupResponseMessage ramResp = ramResult.get();
        GetGroupResponseMessage diskResp = diskResult.get();

        GetGroupResponseMessage result;
        result.id = msg.id;
        // Combine results: prepend RAM entries to the disk entries.
        diskResp.response.insert(diskResp.response.begin(), ramResp.response.begin(), ramResp.response.end());
        result.response = diskResp.response;

        LOG_INFO("StorageHandler", "GET GROUP for '" << msg.group << "' returned "
                 << result.response.size() << " total entries.");
        return result;
    }

    // DELETE KEY event: Forwards the deletion request to both storages.
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        if (msg.key.empty()) {
            LOG_ERROR("StorageHandler", "DeleteKeyResponseMessage key is empty.");
            throw std::invalid_argument("Invalid key name");
        }

        auto ramResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteKeyResponseMessage ramResp = ramResult.get();
        if (ramResp.response != 0) {
            LOG_INFO("StorageHandler", "Key '" << msg.key << "' deleted in RamHandler.");
        }
        DeleteKeyResponseMessage diskResp = diskResult.get();
        if (diskResp.response != 0) {
            LOG_INFO("StorageHandler", "Key '" << msg.key << "' deleted in DiskHandler.");
        }

        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        // Report success only if both storages confirm deletion (1 = success).
        resp.response = ramResp.response + diskResp.response;
        return resp;
    }

    // DELETE GROUP event: Forwards the request to both storages and aggregates the results.
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        if (msg.group.empty()) {
            LOG_ERROR("StorageHandler", "DeleteGroupResponseMessage group is empty.");
            throw std::invalid_argument("Invalid group name");
        }

        auto ramResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteGroupResponseMessage ramResp = ramResult.get();
        if (ramResp.response != 0) {
            LOG_INFO("StorageHandler", "Group '" << msg.group << "' deleted in RamHandler.");
        }
        DeleteGroupResponseMessage diskResp = diskResult.get();
        if (diskResp.response != 0) {
            LOG_INFO("StorageHandler", "Group '" << msg.group << "' deleted in DiskHandler.");
        }

        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        // Here we sum the number of deleted entries (alternative strategies are possible).
        resp.response = ramResp.response + diskResp.response;
        return resp;
    }

    // LIST event: Retrieves entries from both storages and merges them.
    ListEventReponseMessage handleListEvent(const ListEventMessage& msg) {
        auto diskResult = eventBus_.send<ListEventReponseMessage>(HandlerID::DiskHandler, msg);
        auto ramResult = eventBus_.send<ListEventReponseMessage>(HandlerID::RamHandler, msg);

        ListEventReponseMessage ramResp = ramResult.get();
        LOG_INFO("StorageHandler", "Found " << ramResp.response.size() << " entries in RamHandler.");
        ListEventReponseMessage diskResp = diskResult.get();
        LOG_INFO("StorageHandler", "Found " << diskResp.response.size() << " entries in DiskHandler.");

        ListEventReponseMessage result;
        result.id = msg.id;
        diskResp.response.insert(diskResp.response.begin(), ramResp.response.begin(), ramResp.response.end());
        result.response = diskResp.response;

        LOG_INFO("StorageHandler", "LIST event returned " << result.response.size() << " total entries.");
        return result;
    }

private:
    EventBus& eventBus_;
};

#endif // STORAGEHANDLER_H
