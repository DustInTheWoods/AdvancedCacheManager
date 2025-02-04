#ifndef RAMHANDLER_H
#define RAMHANDLER_H

#include "eventbus/EventBus.h"
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
#include "storage/Message.h" // Hier sollten die entsprechenden Message-Klassen für den RamHandler definiert sein

using Clock = std::chrono::steady_clock;

// Vorwärtsdeklaration für den Iteratortyp der Eviction-Queue
using EvictionIterator = std::multimap<Clock::time_point, std::string>::iterator;

// Struktur, die einen Eintrag im RAM speichert
struct RamEntry {
    std::string value;
    // Gruppe für spätere suche
    std::string group;
    // Zeitpunkt der Einfügung (für Eviction)
    Clock::time_point insertionTime;
    // Ablaufzeitpunkt; falls TTL <= 0, wird expirationTime auf einen weit entfernten Zeitpunkt gesetzt.
    Clock::time_point expirationTime;
    // Iterator in der Eviction-Queue
    EvictionIterator evictionIt;
};

class RamHandler {
public:
    // Konstruktor: Neben dem EventBus wird auch die maximale Größe (in MB) übergeben.
    explicit RamHandler(EventBus& eventBus, size_t maxSizeMB = 10)
        : eventBus_(eventBus)
        , maxSizeBytes_(maxSizeMB * 1024 * 1024)
        , currentUsage_(0)
        , stopThread_(false)
    {
        // Registrierung der Handler-Funktionen beim EventBus
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

        // Starte den Hintergrundthread zur TTL-Überprüfung und Eviction
        bgThread_ = std::thread(&RamHandler::backgroundChecker, this);
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
    }

private:
    // Interner Speicher für Schlüssel-Wert-Paare
    std::unordered_map<std::string, RamEntry> store_;
    // Mutex zum Schutz des Speichers und weiterer Membervariablen
    std::mutex mutex_;
    // Eviction-Queue: sortiert nach Einfügezeit (älteste zuerst)
    std::multimap<Clock::time_point, std::string> evictionQueue_;
    // EventBus-Referenz
    EventBus& eventBus_;
    // Maximale Größe in Byte
    size_t maxSizeBytes_;
    // Aktueller (inkrementell verwalteter) Speicherverbrauch (Summe der Schlüssel- und Wertlängen)
    size_t currentUsage_;

    // Hintergrundthread und Synchronisation
    std::thread bgThread_;
    std::condition_variable cv_;
    bool stopThread_;

    // ------------------------------
    // Handler-Implementierungen
    // ------------------------------

    // Verarbeitet ein SET‑Event: Speichert den übergebenen Schlüssel und Wert im RAM.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = Clock::now();
        size_t entrySize = msg.key.size() + msg.value.size();

        // Falls der Schlüssel bereits existiert, entferne den alten Eintrag (und passe currentUsage_ an)
        auto it = store_.find(msg.key);
        if (it != store_.end()) {
            currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
            evictionQueue_.erase(it->second.evictionIt);
            store_.erase(it);
        }

        RamEntry entry;
        entry.value = msg.value;
        entry.insertionTime = now;
        entry.group = msg.group;
        if (msg.ttl > 0) {
            entry.expirationTime = now + std::chrono::seconds(msg.ttl);
        } else {
            // Falls ttl <= 0, setze expirationTime auf einen weit entfernten Zeitpunkt
            entry.expirationTime = Clock::time_point::max();
        }
        // Füge in die Eviction-Queue ein und speichere den Iterator im Eintrag
        auto evIt = evictionQueue_.insert({ entry.insertionTime, msg.key});
        entry.evictionIt = evIt;

        currentUsage_ +=  calculateExactEntryUsage(msg.key, &entry);
        store_[msg.key] = std::move(entry);

        SetResponseMessage resp;
        resp.id = msg.id;
        resp.response = true; // Erfolg
        return resp;
    }

    // Verarbeitet ein GET KEY‑Event: Liefert den zugehörigen Wert zurück (oder einen leeren String, falls nicht gefunden oder abgelaufen).
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        GetKeyResponseMessage resp;
        resp.id = msg.id;
        auto it = store_.find(msg.key);
        auto now = Clock::now();
        if (it != store_.end()) {
            resp.response = it->second.value;
        } else {
            resp.response = "";
        }
        return resp;
    }

    // Verarbeitet ein GET GROUP‑Event: Liefert alle Schlüssel‑Wert-Paare der angegebenen Gruppe.
    // Annahme: Schlüssel werden im Format "gruppe:schluessel" abgelegt.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        GetGroupResponseMessage resp;
        resp.id = msg.id;

        auto now = Clock::now();
        // Durchlaufe alle Einträge im Store (Optimierung: Für sehr große Stores könnte ein zusätzlicher Index helfen)
        for (auto it = store_.begin(); it != store_.end(); ) {
            if (it->second.group == msg.group) {
                resp.response.push_back({ it->first, it->second.value });
                ++it;
            } else {
                ++it;
            }
        }
        return resp;
    }

    // Verarbeitet ein DELETE KEY‑Event: Löscht den angegebenen Schlüssel aus dem RAM.
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
        } else {
            resp.response = 0;
        }
        return resp;
    }

    // Verarbeitet ein DELETE GROUP‑Event: Löscht alle Schlüssel der angegebenen Gruppe.
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
        return resp;
    }

    ListEventReponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        ListEventReponseMessage resp;
        resp.id = msg.id;

        auto now = Clock::now();
        // Durchlaufe alle Einträge im Store (Optimierung: Für sehr große Stores könnte ein zusätzlicher Index helfen)
        for (auto& it = store_.begin(); it != store_.end(); ) {
            StorageEntry entry;

            entry.key = it->first;
            entry.value = it->second.value;
            entry.group = it->second.group;

            resp.response.push_back(entry)
        }
        return resp;
    }

    // Berechnet den (approximativen) Speicherverbrauch des internen Speichers (store_).
    size_t calculateExactEntryUsage(const std::string& key, const RamEntry& entry) {
        size_t usage = 0;
        
        // 1. Schlüssel (std::string)
        //    - Statischer Overhead des String-Objekts:
        usage += sizeof(key);
        //    - Dynamisch allokierter Speicher im internen Puffer:
        usage += key.capacity() * sizeof(char);

        // 2. entry.value (std::string)
        usage += sizeof(entry.value);
        usage += entry.value.capacity() * sizeof(char);

        // 3. entry.group (std::string)
        usage += sizeof(entry.group);
        usage += entry.group.capacity() * sizeof(char);

        // 4. Weitere Felder in RamEntry:
        //    insertionTime und expirationTime (hier nehmen wir deren sizeof() an,
        //    da sie intern typischerweise als std::chrono::time_point implementiert sind)
        usage += sizeof(entry.insertionTime);
        usage += sizeof(entry.expirationTime);

        // 5. evictionIt (ein Iterator, meist ein Zeiger oder ähnliches)
        usage += sizeof(entry.evictionIt);

        return usage;
    }

    // ------------------------------
    // Hintergrundthread: TTL-Checker und size-based Eviction
    // ------------------------------
    void backgroundChecker() {
        // Intervall (in Millisekunden) zwischen Überprüfungen
        const std::chrono::milliseconds interval(500);

        while (true) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (cv_.wait_for(lock, interval, [this] { return stopThread_; })) {
                    break;
                }
                auto now = Clock::now();

                // --- 1. TTL-Überprüfung ---
                for (auto it = store_.begin(); it != store_.end(); ) {
                    if (now >= it->second.expirationTime) {
                        std::cout << "[TTL Check] Removing expired entry: " << it->first << std::endl;
                        currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
                        evictionQueue_.erase(it->second.evictionIt);
                        it = store_.erase(it);
                    } else {
                        ++it;
                    }
                }

                // --- 2. Size-based Eviction ---
                while (currentUsage_ > maxSizeBytes_ && !evictionQueue_.empty()) {
                    // Der älteste Eintrag (gemäß Einfügezeit) befindet sich am Anfang der Queue
                    auto evIt = evictionQueue_.begin();
                    std::string key = evIt->second;
                    std::cout << "[Size Eviction] Usage (" << currentUsage_ 
                          << ") exceeds limit (" << maxSizeBytes_ 
                          << "). Removing entry: " << key << std::endl;
                    auto storeIt = store_.find(key);
                    if (storeIt != store_.end()) {
                        currentUsage_ -= calculateExactEntryUsage(it->first, it->second);
                        evictionQueue_.erase(evIt);
                        store_.erase(storeIt);
                    } else {
                        // Sollte nicht passieren, aber zur Sicherheit:
                        evictionQueue_.erase(evIt);
                    }
                }
            } // Lock freigeben
        }
    }
};

#endif // RAMHANDLER_H
