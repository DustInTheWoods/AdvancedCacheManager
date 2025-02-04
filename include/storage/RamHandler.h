#ifndef RAMHANDLER_H
#define RAMHANDLER_H

#include "eventbus/EventBus.h"
#include "storage/Message.h" // The corresponding Message classes for the RamHandler should be defined here.
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <algorithm>
#include <map>

// ------------------------------
// Logging Helpers and Macros
// ------------------------------

// Logging macros with a consistent layout.
#define LOG_INFO(component, message) \
std::cout <<"[INFO]" << " [" << component << "] " << message << std::endl;

#define LOG_ERROR(component, message) \
std::cout <<"[ERROR]" << " [" << component << "] " << message << std::endl;

// ------------------------------
// End Logging Helpers and Macros
// ------------------------------

using Clock = std::chrono::steady_clock;

// Forward declaration for the iterator type of the eviction queue.
using EvictionIterator = std::multimap<Clock::time_point, std::string>::iterator;

// Structure that stores an entry in RAM.
struct RamEntry {
    std::string value;
    // Group used for later searches.
    std::string group;
    // Insertion time (for eviction).
    Clock::time_point insertionTime;
    // Expiration time; if TTL <= 0, expirationTime is set to a distant future time.
    Clock::time_point expirationTime;
    // Iterator in the eviction queue.
    EvictionIterator evictionIt;
};

class RamHandler {
public:
    // Constructor: Besides the EventBus, the maximum size (in MB) is provided.
    explicit RamHandler(EventBus& eventBus, size_t maxSizeMB = 10)
        : eventBus_(eventBus)
        , maxSizeBytes_(maxSizeMB * 1024 * 1024)
        , currentUsage_(0)
        , stopThread_(false)
    {
        // Register handler functions with the EventBus.
        eventBus_.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::RamHandler,
            [this](const SetEventMessage& msg) -> SetResponseMessage {
                return handleSetEvent(msg);
            }
        );

        eventBus_.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::RamHandler,
            [this](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
                return handleGetKeyEvent(msg);
            }
        );

        eventBus_.subscribe<GetGroupEventMessage, GetGroupResponseMessage>(HandlerID::RamHandler,
            [this](const GetGroupEventMessage& msg) -> GetGroupResponseMessage {
                return handleGetGroupEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(HandlerID::RamHandler,
            [this](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
                return handleDeleteKeyEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(HandlerID::RamHandler,
            [this](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
                return handleDeleteGroupEvent(msg);
            }
        );

        eventBus_.subscribe<ListEventMessage, ListEventReponseMessage>(HandlerID::RamHandler,
            [this](const ListEventMessage& msg) -> ListEventReponseMessage {
                return handleListEvent(msg);
            }
        );

        // Start the background thread for TTL checking and eviction.
        bgThread_ = std::thread(&RamHandler::backgroundChecker, this);
        LOG_INFO("RamHandler", "Initialized with maximum size " << maxSizeBytes_ << " bytes.");
    }

    ~RamHandler() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopThread_ = true;
        }
        cv_.notify_all();
        if (bgThread_.joinable()) {
            bgThread_.join();
        }
        LOG_INFO("RamHandler", "Background thread stopped and resources cleaned up.");
    }

private:
    // Internal storage for key-value pairs.
    std::unordered_map<std::string, RamEntry> store_;
    // Mutex to protect the store and other member variables.
    std::mutex mutex_;
    // Eviction queue: sorted by insertion time (oldest first).
    std::multimap<Clock::time_point, std::string> evictionQueue_;
    // EventBus reference.
    EventBus& eventBus_;
    // Maximum size in bytes.
    size_t maxSizeBytes_;
    // Current (incrementally managed) memory usage (sum of key and value lengths).
    size_t currentUsage_;

    // Background thread and synchronization.
    std::thread bgThread_;
    std::condition_variable cv_;
    bool stopThread_;

    // ------------------------------
    // Handler Implementations
    // ------------------------------

    // Handles a SET event: stores the provided key and value in RAM.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = Clock::now();
        size_t entrySize = msg.key.size() + msg.value.size();

        // If the key already exists, remove the old entry and adjust currentUsage_.
        auto it = store_.find(msg.key);
        if (it != store_.end()) {
            currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
            evictionQueue_.erase(it->second.evictionIt);
            store_.erase(it);
            LOG_INFO("RamHandler", "Overwriting existing key: " << msg.key);
        }

        RamEntry entry;
        entry.value = msg.value;
        entry.insertionTime = now;
        entry.group = msg.group;
        if (msg.ttl > 0) {
            entry.expirationTime = now + std::chrono::seconds(msg.ttl);
        } else {
            // If ttl <= 0, set expirationTime to a distant future.
            entry.expirationTime = Clock::time_point::max();
        }
        // Insert into the eviction queue and store the iterator in the entry.
        auto evIt = evictionQueue_.insert({ entry.insertionTime, msg.key });
        entry.evictionIt = evIt;

        currentUsage_ += calculateExactEntryUsage(msg.key, entry);
        store_[msg.key] = std::move(entry);

        LOG_INFO("RamHandler", "SET event: Stored key '" << msg.key << "'; current usage: " << currentUsage_);
        SetResponseMessage resp;
        resp.id = msg.id;
        resp.response = true; // Success
        return resp;
    }

    // Handles a GET KEY event: returns the corresponding value (or an empty string if not found or expired).
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        GetKeyResponseMessage resp;
        resp.id = msg.id;
        auto now = Clock::now();
        auto it = store_.find(msg.key);
        if (it != store_.end()) {
            resp.response = it->second.value;
            LOG_INFO("RamHandler", "GET KEY event: Key '" << msg.key << "' found.");
        } else {
            resp.response = "";
            LOG_INFO("RamHandler", "GET KEY event: Key '" << msg.key << "' not found.");
        }
        return resp;
    }

    // Handles a GET GROUP event: returns all key-value pairs belonging to the specified group.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        GetGroupResponseMessage resp;
        resp.id = msg.id;

        for (auto it = store_.begin(); it != store_.end(); ++it) {
            if (it->second.group == msg.group) {
                resp.response.push_back({ it->first, it->second.value });
            }
        }
        LOG_INFO("RamHandler", "GET GROUP event: Found " << resp.response.size() << " entries for group '" << msg.group << "'.");
        return resp;
    }

    // Handles a DELETE KEY event: removes the specified key from RAM.
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        auto it = store_.find(msg.key);
        if (it != store_.end()) {
            currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
            evictionQueue_.erase(it->second.evictionIt);
            store_.erase(it);
            resp.response = 1;
            LOG_INFO("RamHandler", "DELETE KEY event: Key '" << msg.key << "' deleted.");
        } else {
            resp.response = 0;
            LOG_INFO("RamHandler", "DELETE KEY event: Key '" << msg.key << "' not found.");
        }
        return resp;
    }

    // Handles a DELETE GROUP event: removes all keys belonging to the specified group.
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        int count = 0;

        for (auto it = store_.begin(); it != store_.end(); ) {
            if (it->second.group == msg.group) {
                currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
                evictionQueue_.erase(it->second.evictionIt);
                it = store_.erase(it);
                ++count;
            } else {
                ++it;
            }
        }
        resp.response = count;
        LOG_INFO("RamHandler", "DELETE GROUP event: Removed " << count << " entries for group '" << msg.group << "'.");
        return resp;
    }

    // Handles a LIST event: retrieves all key-value entries stored in RAM.
    ListEventReponseMessage handleListEvent(const ListEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        ListEventReponseMessage resp;
        resp.id = msg.id;

        // Iterate over all entries in the store.
        for (auto it = store_.begin(); it != store_.end(); ++it) {
            StorageEntry entry;
            entry.key = it->first;
            entry.value = it->second.value;
            entry.group = it->second.group;
            resp.response.push_back(entry);
        }
        LOG_INFO("RamHandler", "LIST event: Returned " << resp.response.size() << " entries.");
        return resp;
    }

    // Calculates the (approximate) memory usage of a store entry.
    size_t calculateExactEntryUsage(const std::string& key, const RamEntry& entry) {
        size_t usage = 0;

        // 1. Key (std::string)
        usage += sizeof(key);
        usage += key.capacity() * sizeof(char);

        // 2. entry.value (std::string)
        usage += sizeof(entry.value);
        usage += entry.value.capacity() * sizeof(char);

        // 3. entry.group (std::string)
        usage += sizeof(entry.group);
        usage += entry.group.capacity() * sizeof(char);

        // 4. Additional fields in RamEntry:
        usage += sizeof(entry.insertionTime);
        usage += sizeof(entry.expirationTime);

        // 5. evictionIt (an iterator, typically a pointer or similar)
        usage += sizeof(entry.evictionIt);

        return usage;
    }

    // ------------------------------
    // Background Thread: TTL Checker and Size-based Eviction
    // ------------------------------
    void backgroundChecker() {
        // Interval (in milliseconds) between checks.
        const std::chrono::milliseconds interval(500);

        while (true) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (cv_.wait_for(lock, interval, [this] { return stopThread_; })) {
                    break;
                }
                auto now = Clock::now();

                // --- 1. TTL Check ---
                for (auto it = store_.begin(); it != store_.end(); ) {
                    if (now >= it->second.expirationTime) {
                        LOG_INFO("RamHandler", "TTL Check: Removing expired entry: " << it->first);
                        currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
                        evictionQueue_.erase(it->second.evictionIt);
                        it = store_.erase(it);
                    } else {
                        ++it;
                    }
                }

                // --- 2. Size-based Eviction ---
                while (currentUsage_ > maxSizeBytes_ && !evictionQueue_.empty()) {
                    // The oldest entry (by insertion time) is at the beginning of the queue.
                    auto evIt = evictionQueue_.begin();
                    std::string key = evIt->second;
                    LOG_INFO("RamHandler", "Size Eviction: Usage (" << currentUsage_
                             << ") exceeds limit (" << maxSizeBytes_
                             << "). Removing entry: " << key);
                    auto storeIt = store_.find(key);
                    if (storeIt != store_.end()) {
                        currentUsage_ -= calculateExactEntryUsage(storeIt->first, storeIt->second);
                        evictionQueue_.erase(evIt);
                        store_.erase(storeIt);
                    } else {
                        // Safety check: should not happen, but erase the iterator regardless.
                        evictionQueue_.erase(evIt);
                    }
                }
            } // Release lock
        }
        LOG_INFO("RamHandler", "Background checker thread exiting.");
    }
};

#endif // RAMHANDLER_H
