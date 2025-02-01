#include <gtest/gtest.h>
#include "eventbus/EventBus.h"
#include "storage/DiskHandler.h"  // Enthält den DiskHandler (mit SQLite)
#include <cstdio>   // Für remove()
#include <string>
#include <vector>


// Test Fixture, um eine temporäre SQLite-Datenbank zu verwenden.
class DiskHandlerTestFixture : public ::testing::Test {
protected:
    void SetUp() override {
        // Vor jedem Test: Lösche ggf. vorhandene Testdatei.
        std::remove(testDbFile.c_str());
    }
    void TearDown() override {
        // Nach jedem Test: Lösche die Testdatei.
        std::remove(testDbFile.c_str());
    }
    std::string testDbFile = "test_disk_store.db";
};

TEST_F(DiskHandlerTestFixture, SetAndGetKey) {
    EventBus bus;
    // Erstelle den DiskHandler mit dem Test‑DB-Pfad.
    DiskHandler diskHandler(bus, testDbFile);

    SetEventMessage setMsg;
    setMsg.id = 1;
    setMsg.key = "group1:key1";
    setMsg.value = "diskValue1";
    auto setResp = bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();
    EXPECT_TRUE(setResp.response);

    GetKeyEventMessage getMsg;
    getMsg.id = 2;
    getMsg.key = "group1:key1";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::DiskHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "diskValue1");
}

TEST_F(DiskHandlerTestFixture, DeleteKey) {
    EventBus bus;
    DiskHandler diskHandler(bus, testDbFile);

    // Setze einen Schlüssel
    SetEventMessage setMsg;
    setMsg.id = 3;
    setMsg.key = "group1:keyToDelete";
    setMsg.value = "toDelete";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    // Lösche den Schlüssel
    DeleteKeyEventMessage delMsg;
    delMsg.id = 4;
    delMsg.key = "group1:keyToDelete";
    auto delResp = bus.send<DeleteKeyResponseMessage>(HandlerID::DiskHandler, delMsg).get();
    EXPECT_EQ(delResp.response, 1);

    // Versuche, den gelöschten Schlüssel abzurufen
    GetKeyEventMessage getMsg;
    getMsg.id = 5;
    getMsg.key = "group1:keyToDelete";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::DiskHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "");
}

TEST_F(DiskHandlerTestFixture, GetGroup) {
    EventBus bus;
    DiskHandler diskHandler(bus, testDbFile);

    // Füge mehrere Schlüssel ein.
    SetEventMessage setMsg;
    setMsg.id = 6;
    setMsg.key = "groupA:key1";
    setMsg.value = "val1";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    setMsg.id = 7;
    setMsg.key = "groupA:key2";
    setMsg.value = "val2";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    setMsg.id = 8;
    setMsg.key = "groupB:key1";
    setMsg.value = "valB1";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    GetGroupEventMessage getGroupMsg;
    getGroupMsg.id = 9;
    getGroupMsg.group = "groupA";
    auto groupResp = bus.send<GetGroupResponseMessage>(HandlerID::DiskHandler, getGroupMsg).get();
    EXPECT_EQ(groupResp.response.size(), 2);
}

TEST_F(DiskHandlerTestFixture, DeleteGroup) {
    EventBus bus;
    DiskHandler diskHandler(bus, testDbFile);

    // Füge mehrere Schlüssel in der Gruppe "groupX" ein.
    SetEventMessage setMsg;
    setMsg.id = 10;
    setMsg.key = "groupX:key1";
    setMsg.value = "val1";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    setMsg.id = 11;
    setMsg.key = "groupX:key2";
    setMsg.value = "val2";
    bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();

    // Lösche die Gruppe
    DeleteGroupEventMessage delGroupMsg;
    delGroupMsg.id = 12;
    delGroupMsg.group = "groupX";
    auto delGroupResp = bus.send<DeleteGroupResponseMessage>(HandlerID::DiskHandler, delGroupMsg).get();
    EXPECT_EQ(delGroupResp.response, 2);
}

// ---------- PERFORMANCE TEST: SET-Events ----------
// Dieser Test misst, wie schnell der DiskHandler (mit SQLite) eine Reihe von SET-Events verarbeiten kann.
TEST_F(DiskHandlerTestFixture, PerformanceTestSetEvents) {
    EventBus bus;
    DiskHandler diskHandler(bus, testDbFile);

    const int numEvents = 1000;  // Anzahl der zu sendenden SET-Events

    // Optionale Aufwärmphase, um Cache-Effekte zu minimieren
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage setMsg;
        setMsg.id = i;
        setMsg.key = "perf:key" + std::to_string(i);
        setMsg.value = "value" + std::to_string(i);
        auto setResp = bus.send<SetResponseMessage>(HandlerID::DiskHandler, setMsg).get();
        ASSERT_TRUE(setResp.response);
    }

    auto end = std::chrono::high_resolution_clock::now();
    long long totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "DiskHandler Performance Test (SET-Events):" << std::endl;
    std::cout << "  Anzahl der Events: " << numEvents << std::endl;
    std::cout << "  Gesamtdauer: " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "  Durchschnittliche Dauer pro Event: " << nsPerEvent << " ns" << std::endl;
    std::cout << "  Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
}