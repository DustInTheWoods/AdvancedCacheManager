#include <gtest/gtest.h>
#include "eventbus/EventBus.h"
#include "storage/Message.h"   // Enthält die Message-Typen (SetEventMessage, GetKeyEventMessage, etc.)
#include <string>
#include <vector>
#include "storage/RamHandler.h"
#include <thread>
#include <chrono>

// ---------- Vorhandene Tests ----------

// Test: Set and Get Key
TEST(RamHandlerTest, SetAndGetKey) {
    EventBus bus;
    // Erstelle den RamHandler (registriert sich beim Bus)
    RamHandler ramHandler(bus);

    // SET-Event
    SetEventMessage setMsg;
    setMsg.id = "1";
    setMsg.key = "group1:key1";
    setMsg.value = "ramValue1";
    // TTL und persistent sind hier für den RAM-Handler nicht entscheidend
    setMsg.ttl = 3600;
    setMsg.persistent = false;
    setMsg.group = "group1";

    auto setResp = bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();
    EXPECT_TRUE(setResp.response);

    // GET-Event
    GetKeyEventMessage getMsg;
    getMsg.id = "2";
    getMsg.key = "group1:key1";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::RamHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "ramValue1");
}

// Test: Delete Key and Group
TEST(RamHandlerTest, DeleteKeyAndGroup) {
    EventBus bus;
    RamHandler ramHandler(bus);

    // Setze einen Schlüssel
    SetEventMessage setMsg;
    setMsg.id = "3";
    setMsg.key = "group1:keyToDelete";
    setMsg.value = "toDelete";
    setMsg.ttl = 3600;
    setMsg.persistent = false;
    setMsg.group = "group1";
    bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();

    // Lösche den Schlüssel
    DeleteKeyEventMessage delMsg;
    delMsg.id = "4";
    delMsg.key = "group1:keyToDelete";
    auto delResp = bus.send<DeleteKeyResponseMessage>(HandlerID::RamHandler, delMsg).get();
    EXPECT_EQ(delResp.response, 1);

    // GET-Key: Sollte leer zurückkommen
    GetKeyEventMessage getMsg;
    getMsg.id = "5";
    getMsg.key = "group1:keyToDelete";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::RamHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "");

    // Test für GROUP-Operationen:
    // Setze zwei Schlüssel in der Gruppe "groupA"
    setMsg.id = "6";
    setMsg.key = "groupA:key1";
    setMsg.value = "val1";
    bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();

    setMsg.id = "7";
    setMsg.key = "groupA:key2";
    setMsg.value = "val2";
    bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();

    GetGroupEventMessage getGroupMsg;
    getGroupMsg.id = "8";
    getGroupMsg.group = "groupA";
    auto groupResp = bus.send<GetGroupResponseMessage>(HandlerID::RamHandler, getGroupMsg).get();
    EXPECT_EQ(groupResp.response.size(), 2);

    // Lösche die Gruppe
    DeleteGroupEventMessage delGroupMsg;
    delGroupMsg.id = "9";
    delGroupMsg.group = "groupA";
    auto delGroupResp = bus.send<DeleteGroupResponseMessage>(HandlerID::RamHandler, delGroupMsg).get();
    EXPECT_EQ(delGroupResp.response, 2);
}

// Test: TTLExpirationTest – Überprüft, ob ein Schlüssel nach Ablauf des TTL nicht mehr verfügbar ist
TEST(RamHandlerTest, TTLExpirationTest) {
    EventBus bus;
    RamHandler ramHandler(bus);

    // SET-Event mit TTL von 1 Sekunde
    SetEventMessage setMsg;
    setMsg.id = "100";
    setMsg.key = "ttlTest:key";
    setMsg.value = "tempValue";
    setMsg.ttl = 1;  // TTL = 1 Sekunde
    setMsg.persistent = false;
    setMsg.group = "ttlTest";

    auto setResp = bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();
    EXPECT_TRUE(setResp.response);

    // Sofortiger GET-Event: Der Schlüssel sollte vorhanden sein
    GetKeyEventMessage getMsg;
    getMsg.id = "101";
    getMsg.key = "ttlTest:key";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::RamHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "tempValue");

    // Warte 2 Sekunden, sodass der Schlüssel abläuft
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Erneuter GET-Event: Der Schlüssel sollte nicht mehr vorhanden sein (leerer String)
    GetKeyEventMessage getMsgExpired;
    getMsgExpired.id = "102";
    getMsgExpired.key = "ttlTest:key";
    auto getRespExpired = bus.send<GetKeyResponseMessage>(HandlerID::RamHandler, getMsgExpired).get();
    EXPECT_EQ(getRespExpired.response, "");
}

// Test: MaxSizeEvictionTest – Überprüft, ob bei Überschreiten der maximalen Größe ältere Einträge evakuiert werden
TEST(RamHandlerTest, MaxSizeEvictionTest) {
    EventBus bus;
    // Initialisiere den RamHandler mit einer kleinen maximalen Größe (z. B. 1 MB)
    size_t maxSizeMB = 1;
    RamHandler ramHandler(bus, maxSizeMB);

    // Füge viele Einträge mit relativ großen Werten ein, um den Speicherverbrauch schnell zu erhöhen
    const int numKeys = 2000;
    for (int i = 0; i < numKeys; ++i) {
        SetEventMessage setMsg;
        setMsg.id = std::to_string(1000 + i);
        setMsg.key = "evictTest:key" + std::to_string(i);
        // Erzeuge einen Wert von 100 Zeichen
        setMsg.value = std::string(1000, 'X');
        setMsg.ttl = 3600; // Langer TTL, sodass Eviction nur über maxSize erfolgt
        setMsg.persistent = false;
        setMsg.group = "evictTest";
        bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();
    }
    // Warte kurz, damit der Hintergrundthread die Eviction durchführt
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    // Überprüfe: Der erste eingefügte Schlüssel sollte evakuiert worden sein
    GetKeyEventMessage getMsg;
    getMsg.id = "3000";
    getMsg.key = "evictTest:key0";
    auto getResp = bus.send<GetKeyResponseMessage>(HandlerID::RamHandler, getMsg).get();
    EXPECT_EQ(getResp.response, "");
}

// Test: PerformanceTestSetEvents – misst die Verarbeitungsgeschwindigkeit von SET-Events
TEST(RamHandlerTest, PerformanceTestSetEvents) {
    EventBus bus;
    RamHandler ramHandler(bus);

    const int numEvents = 10000;  // Anzahl der zu sendenden SET-Events

    // Aufwärmphase
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    // Sende numEvents SET-Events nacheinander
    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage setMsg;
        setMsg.id = std::to_string(i);
        setMsg.key = "groupPerf:key" + std::to_string(i);
        setMsg.value = "value" + std::to_string(i);
        setMsg.ttl = 3600;
        setMsg.persistent = false;
        setMsg.group = "groupPerf";

        auto resp = bus.send<SetResponseMessage>(HandlerID::RamHandler, setMsg).get();
        ASSERT_TRUE(resp.response);
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Berechne die Laufzeit-Metriken
    long long totalNs = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "RamHandler Performance Test (SET-Events):" << std::endl;
    std::cout << "  Anzahl der Events: " << numEvents << std::endl;
    std::cout << "  Gesamtdauer: " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "  Durchschnitt pro Event: " << nsPerEvent << " ns" << std::endl;
    std::cout << "  Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
}

// ---------- Zusätzliche Tests für TTL und Eviction ----------
