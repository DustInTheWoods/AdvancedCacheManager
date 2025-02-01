#include <gtest/gtest.h>
#include "eventbus/EventBus.h"
#include "storage/StorageHandler.h"
#include "storage/RamHandler.h"
#include "storage/DiskHandler.h"

#include <chrono>
#include <atomic>
#include <thread>
#include <future>
#include <iostream>
#include <cstdio>



TEST(PerformanceTest, SetPersistentPerformanceTest) {
    // Für den DiskHandler verwenden wir eine temporäre Datenbankdatei
    std::string testDbFile = "test_set_persistent.db";
    std::remove(testDbFile.c_str());

    // Erstelle den zentralen EventBus
    EventBus bus;

    // Instanziiere die Handler:
    RamHandler ramHandler(bus);
    DiskHandler diskHandler(bus, testDbFile);
    StorageHandler storageHandler(bus);

    const int numEvents = 10000; // Anzahl der SET-Events
    std::atomic<int> successCount{0};

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage msg;
        msg.id = std::to_string(i);
        msg.persistent = true;  // persistent: Eintrag soll in RAM UND Disk gespeichert werden
        msg.ttl = 3600;
        msg.key = "persistent:key" + std::to_string(i);
        msg.value = "value" + std::to_string(i);
        msg.group = "group1";

        auto futureResp = bus.send<SetResponseMessage>(HandlerID::StorageHandler, msg);
        SetResponseMessage resp = futureResp.get();
        if (resp.response) {
            successCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "\n=== SET Persistent Performance Test ===" << std::endl;
    std::cout << "Anzahl SET-Events:       " << numEvents << std::endl;
    std::cout << "Gesamtdauer:             " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "Durchschnitt pro Event:  " << nsPerEvent << " ns" << std::endl;
    std::cout << "Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
    std::cout << "Erfolgreiche SETs:       " << successCount.load() << std::endl;

    EXPECT_EQ(successCount.load(), numEvents);

    // Aufräumen
    std::remove(testDbFile.c_str());
}

// ----------------------------------------------------------------------------
// Test 2: SET nonpersistent Performance Test
// ----------------------------------------------------------------------------

TEST(PerformanceTest, SetNonPersistentPerformanceTest) {
    // Für diesen Test kann die Disk-Datenbank ignoriert werden. Dennoch erstellen wir alle Handler.
    std::string testDbFile = "test_set_nonpersistent.db";
    std::remove(testDbFile.c_str());

    EventBus bus;
    RamHandler ramHandler(bus);
    DiskHandler diskHandler(bus, testDbFile);
    StorageHandler storageHandler(bus);

    const int numEvents = 10000;
    std::atomic<int> successCount{0};

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage msg;
        msg.id = std::to_string(i);
        msg.persistent = false; // nonpersistent: Eintrag wird nur im RAM gespeichert
        msg.ttl = 3600;
        msg.key = "nonpersistent:key" + std::to_string(i);
        msg.value = "value" + std::to_string(i);
        msg.group = "group1";

        auto futureResp = bus.send<SetResponseMessage>(HandlerID::StorageHandler, msg);
        SetResponseMessage resp = futureResp.get();
        if (resp.response) {
            successCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "\n=== SET Nonpersistent Performance Test ===" << std::endl;
    std::cout << "Anzahl SET-Events:       " << numEvents << std::endl;
    std::cout << "Gesamtdauer:             " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "Durchschnitt pro Event:  " << nsPerEvent << " ns" << std::endl;
    std::cout << "Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
    std::cout << "Erfolgreiche SETs:       " << successCount.load() << std::endl;

    EXPECT_EQ(successCount.load(), numEvents);

    std::remove(testDbFile.c_str());
}

// ----------------------------------------------------------------------------
// Test 3: GET persistent Performance Test
// ----------------------------------------------------------------------------
//
// In diesem Test werden persistent gespeicherte Schlüssel (also SETs mit persistent==true)
// vorab in den Speicher eingefügt. Die Messung umfasst nur die GET-Operationen.
TEST(PerformanceTest, GetPersistentPerformanceTest) {
    std::string testDbFile = "test_get_persistent.db";
    std::remove(testDbFile.c_str());

    EventBus bus;
    RamHandler ramHandler(bus);
    DiskHandler diskHandler(bus, testDbFile);
    StorageHandler storageHandler(bus);

    const int numEvents = 10000;
    // Prepopulation: Setze persistent Schlüssel (ohne Messung)
    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage setMsg;
        setMsg.id = std::to_string(i);
        setMsg.persistent = true;
        setMsg.ttl = 3600;
        setMsg.key = "persistent:key" + std::to_string(i);
        setMsg.value = "value" + std::to_string(i);
        setMsg.group = "group1";
        auto futureSet = bus.send<SetResponseMessage>(HandlerID::StorageHandler, setMsg);
        SetResponseMessage setResp = futureSet.get();
        if (!setResp.response) {
            FAIL() << "Prepopulation failed for key: " << setMsg.key;
        }
    }

    // GET-Performance-Messung (nur GET, Prepopulation nicht eingerechnet)
    std::atomic<int> successCount{0};
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        GetKeyEventMessage getMsg;
        getMsg.id = std::to_string(i);
        getMsg.key = "persistent:key" + std::to_string(i);
        auto futureGet = bus.send<GetKeyResponseMessage>(HandlerID::StorageHandler, getMsg);
        GetKeyResponseMessage getResp = futureGet.get();
        if (getResp.response == "value" + std::to_string(i)) {
            successCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "\n=== GET Persistent Performance Test ===" << std::endl;
    std::cout << "Anzahl GET-Events:       " << numEvents << std::endl;
    std::cout << "Gesamtdauer:             " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "Durchschnitt pro Event:  " << nsPerEvent << " ns" << std::endl;
    std::cout << "Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
    std::cout << "Erfolgreiche GETs:       " << successCount.load() << std::endl;

    EXPECT_EQ(successCount.load(), numEvents);

    std::remove(testDbFile.c_str());
}

// ----------------------------------------------------------------------------
// Test 4: GET nonpersistent Performance Test
// ----------------------------------------------------------------------------
//
// Hier werden Schlüssel abgefragt, die nur im RAM abgelegt wurden (SET mit persistent==false).
// Auch hier wird die Prepopulation außerhalb der Messung durchgeführt.
TEST(PerformanceTest, GetNonPersistentPerformanceTest) {
    std::string testDbFile = "test_get_nonpersistent.db";
    std::remove(testDbFile.c_str());

    EventBus bus;
    RamHandler ramHandler(bus);
    DiskHandler diskHandler(bus, testDbFile);
    StorageHandler storageHandler(bus);

    const int numEvents = 10000;
    // Prepopulation: Setze nonpersistent Schlüssel
    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage setMsg;
        setMsg.id = std::to_string(i);
        setMsg.persistent = false;  // Nur im RAM
        setMsg.ttl = 3600;
        setMsg.key = "nonpersistent:key" + std::to_string(i);
        setMsg.value = "value" + std::to_string(i);
        setMsg.group = "group1";
        auto futureSet = bus.send<SetResponseMessage>(HandlerID::StorageHandler, setMsg);
        SetResponseMessage setResp = futureSet.get();
        if (!setResp.response) {
            FAIL() << "Prepopulation failed for key: " << setMsg.key;
        }
    }

    // GET-Performance-Messung (nur GET, Prepopulation nicht eingerechnet)
    std::atomic<int> successCount{0};
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        GetKeyEventMessage getMsg;
        getMsg.id = std::to_string(i);
        getMsg.key = "nonpersistent:key" + std::to_string(i);
        auto futureGet = bus.send<GetKeyResponseMessage>(HandlerID::StorageHandler, getMsg);
        GetKeyResponseMessage getResp = futureGet.get();
        if (getResp.response == "value" + std::to_string(i)) {
            successCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "\n=== GET Nonpersistent Performance Test ===" << std::endl;
    std::cout << "Anzahl GET-Events:       " << numEvents << std::endl;
    std::cout << "Gesamtdauer:             " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "Durchschnitt pro Event:  " << nsPerEvent << " ns" << std::endl;
    std::cout << "Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
    std::cout << "Erfolgreiche GETs:       " << successCount.load() << std::endl;

    EXPECT_EQ(successCount.load(), numEvents);

    std::remove(testDbFile.c_str());
}