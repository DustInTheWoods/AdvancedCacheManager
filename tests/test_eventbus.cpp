#include <gtest/gtest.h>
#include "eventbus/EventBus.h"
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <future>
#include <numeric>
#include <iostream>
#include <stdexcept>

// Hilfsfunktion zum Messen der Zeitdifferenz in Nanosekunden
inline long long nanoDiff(const std::chrono::high_resolution_clock::time_point& start,
                           const std::chrono::high_resolution_clock::time_point& end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

struct TestEvent : Message {
    int value;
    explicit TestEvent(const int value) : value(value) {}
};

struct TestReturnEvent : Message {
    int value;
    explicit TestReturnEvent(const int value) : value(value) {}
};

// ---------- TEST 1: Subscribe  ----------
TEST(EventBusTest, Subscribe) {
    EventBus bus;

    // Handler registrieren
    const bool success = bus.subscribe<TestEvent, void>(HandlerID::Broadcast, [&](const TestEvent& evt) {
        // Handler kann hier beliebige Aktionen ausführen
    });

    EXPECT_TRUE(success);
}

// ---------- TEST 2: Unsubscribe  ----------
TEST(EventBusTest, UnSubscribe) {
    EventBus bus;

    auto fn = [](const TestEvent& msg) {
        std::cout << "TestEvent empfangen, Wert: " << msg.value << std::endl;
    };

    // Handler registrieren
    const bool sub = bus.subscribe<TestEvent, void>(HandlerID::Broadcast, fn);

    // Handler wieder abmelden
    const bool unsub = bus.unsubscribe<TestEvent>(HandlerID::Broadcast);

    EXPECT_TRUE(sub);
    EXPECT_TRUE(unsub);
}

// ---------- TEST 3: FailedSubscribe  ----------
TEST(EventBusTest, FailedSubscribe) {
    EventBus bus;

    const auto fn = [](const TestEvent& msg) {
        std::cout << "TestEvent empfangen, Wert: " << msg.value << std::endl;
    };

    // Erste Registrierung sollte erfolgreich sein
    const bool success = bus.subscribe<TestEvent, void>(HandlerID::Broadcast, fn);
    EXPECT_TRUE(success);

    // Zweite Registrierung mit demselben HandlerID soll fehlschlagen und eine Exception werfen.
    // Wir packen den Code in ein Lambda, um den ganzen Block als ein einziges Argument an EXPECT_THROW zu übergeben.
    auto subscribe_call = [&]() {
        try {
            bus.subscribe<TestEvent, void>(HandlerID::Broadcast, fn);
        } catch (const std::runtime_error& e) {
            // Passe den erwarteten Text exakt an die tatsächlich geworfene Fehlermeldung an!
            EXPECT_STREQ("Event handler already exists", e.what());
            throw;
        }
    };

    EXPECT_THROW(subscribe_call(), std::runtime_error);
}

// ---------- TEST 4: Failed Unsubscribe  ----------
TEST(EventBusTest, FailedUnsubscribe) {
    EventBus bus;

    // Abmelden ohne registrierten Handler sollte fehlschlagen
    const bool unsub = bus.unsubscribe<TestEvent>(HandlerID::Broadcast);

    EXPECT_FALSE(unsub);
}

// ---------- TEST 5: Send  ----------
TEST(EventBusTest, Send) {
    EventBus bus;

    auto fn = [](const TestEvent& msg) {
        EXPECT_EQ(msg.value, 42);
    };

    bus.subscribe<TestEvent, void>(HandlerID::Broadcast, fn);

    auto event = TestEvent(42);
    bus.send<void>(HandlerID::Broadcast, event);
}

// ---------- TEST 6: FailedSend  ----------
TEST(EventBusTest, FailedSend) {
    EventBus bus;

    auto event = TestEvent(42);

    EXPECT_THROW({
        try {
            bus.send<void>(HandlerID::Broadcast, event);
        }
        catch (const std::runtime_error& e) {
            EXPECT_STREQ("Handler not found!", e.what());
            throw;
        }
    }, std::runtime_error);
}

// ---------- TEST 7: Request  ----------
TEST(EventBusTest, Request) {
    EventBus bus;

    auto fn = [](const TestEvent& msg) -> TestReturnEvent {
        return TestReturnEvent(msg.value + 1);
    };

    bus.subscribe<TestEvent, TestReturnEvent>(HandlerID::Broadcast, fn);

    auto event = TestEvent(42);
    auto response = bus.send<TestReturnEvent>(HandlerID::Broadcast, event);

    auto result = response.get(); // Ergebnis abholen
    EXPECT_EQ(result.value, 43);  // Erwarteter Wert
}

// ---------- TEST 8: PerformanceTest  ----------
TEST(EventBusTest, PerformanceTest) {
    EventBus bus;

    const int numEvents = 10000; // Anzahl der zu sendenden Events
    std::atomic<int> eventCounter{0};

    // Handler registrieren, der jedes empfangene Event zählt
    auto fn = [&eventCounter](const TestEvent& msg) {
        eventCounter++;
    };

    bus.subscribe<TestEvent, void>(HandlerID::Broadcast, fn);

    // Optional: Aufwärmphase
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    // Events senden: Erstelle jeweils ein lvalue-Objekt.
    for (int i = 0; i < numEvents; ++i) {
        TestEvent event(i);
        bus.send<void>(HandlerID::Broadcast, event);
    }

    // Warten, bis alle Events verarbeitet sind
    while (eventCounter < numEvents) {
        std::this_thread::yield();
    }

    auto end = std::chrono::high_resolution_clock::now();

    long long totalNs = nanoDiff(start, end);
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    std::cout << "PerformanceTest:" << std::endl;
    std::cout << "  Anzahl der Events: " << numEvents << std::endl;
    std::cout << "  Gesamtdauer: " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "  Durchschnittliche Dauer pro Event: " << nsPerEvent << " ns" << std::endl;
    std::cout << "  Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;

    EXPECT_EQ(eventCounter.load(), numEvents);
}