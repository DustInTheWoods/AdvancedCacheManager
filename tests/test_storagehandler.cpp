#include <gtest/gtest.h>
#include "eventbus/EventBus.h"
#include "storage/StorageHandler.h"  // Enthält den StorageHandler
#include <string>
#include <vector>
#include "storage/Message.h"

// Hilfsfunktion zum Messen der Zeitdifferenz in Nanosekunden
inline long long nanoDiff(const std::chrono::high_resolution_clock::time_point &start,
                           const std::chrono::high_resolution_clock::time_point &end) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

// ---------- TEST 1: SET-Event ----------
// Testet, ob der StorageHandler SET-Anfragen an beide Speicher (RAM und Disk) weiterleitet.
TEST(StorageHandlerTest, SetEventSuccess) {
    EventBus bus;

    // Dummy RamHandler: Liefert für SET-Events immer Erfolg.
    bus.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::RamHandler,
        [](const SetEventMessage& msg) -> SetResponseMessage {
            SetResponseMessage resp;
            resp.id = msg.id;
            resp.response = true;
            return resp;
        }
    );
    // Dummy DiskHandler: Liefert für SET-Events ebenfalls Erfolg.
    bus.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::DiskHandler,
        [](const SetEventMessage& msg) -> SetResponseMessage {
            SetResponseMessage resp;
            resp.id = msg.id;
            resp.response = true;
            return resp;
        }
    );

    // StorageHandler registriert sich und leitet weiter.
    StorageHandler storage(bus);

    SetEventMessage setMsg;
    setMsg.id = 1;
    setMsg.key = "key1";
    setMsg.value = "value1";

    auto futureResp = bus.send<SetResponseMessage>(HandlerID::StorageHandler, setMsg);
    SetResponseMessage resp = futureResp.get();

    EXPECT_EQ(resp.id, setMsg.id);
    EXPECT_TRUE(resp.response);  // Erfolg nur, wenn beide (RAM und Disk) erfolgreich waren.
}

// ---------- TEST 2: GET-Event (RAM liefert Ergebnis) ----------
// Hier liefert der Dummy‑RamHandler einen nicht-leeren Wert, sodass der StorageHandler den RAM-Wert zurückgibt.
TEST(StorageHandlerTest, GetKeyEventFromRam) {
    EventBus bus;

    // Dummy RamHandler liefert einen Wert.
    bus.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::RamHandler,
        [](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
            GetKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = "valueFromRam";
            return resp;
        }
    );
    // Dummy DiskHandler – sollte in diesem Test nicht zum Einsatz kommen.
    bus.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::DiskHandler,
        [](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
            GetKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = "valueFromDisk";
            return resp;
        }
    );

    StorageHandler storage(bus);

    GetKeyEventMessage getMsg;
    getMsg.id = 2;
    getMsg.key = "key1";

    auto futureResp = bus.send<GetKeyResponseMessage>(HandlerID::StorageHandler, getMsg);
    GetKeyResponseMessage resp = futureResp.get();

    EXPECT_EQ(resp.id, getMsg.id);
    EXPECT_EQ(resp.response, "valueFromRam");
}

// ---------- TEST 3: GET-Event (Fallback auf Disk) ----------
// Hier liefert der Dummy‑RamHandler einen leeren Wert; der StorageHandler fragt daraufhin den DiskHandler.
TEST(StorageHandlerTest, GetKeyEventFallbackToDisk) {
    EventBus bus;

    // Dummy RamHandler liefert leere Antwort.
    bus.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::RamHandler,
        [](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
            GetKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = "";
            return resp;
        }
    );
    // Dummy DiskHandler liefert einen gültigen Wert.
    bus.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::DiskHandler,
        [](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
            GetKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = "valueFromDisk";
            return resp;
        }
    );

    StorageHandler storage(bus);

    GetKeyEventMessage getMsg;
    getMsg.id = 3;
    getMsg.key = "key1";

    auto futureResp = bus.send<GetKeyResponseMessage>(HandlerID::StorageHandler, getMsg);
    GetKeyResponseMessage resp = futureResp.get();

    EXPECT_EQ(resp.id, getMsg.id);
    EXPECT_EQ(resp.response, "valueFromDisk");
}

// ---------- TEST 4: DELETE KEY ----------
// Testet, ob DELETE-Events korrekt an beide Speicher weitergeleitet werden.
TEST(StorageHandlerTest, DeleteKeyEvent) {
    EventBus bus;

    // Dummy RamHandler: Liefert für DELETE KEY einen Erfolg (1).
    bus.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(HandlerID::RamHandler,
        [](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
            DeleteKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = 1;
            return resp;
        }
    );
    // Dummy DiskHandler: Liefert ebenfalls Erfolg (1).
    bus.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(HandlerID::DiskHandler,
        [](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
            DeleteKeyResponseMessage resp;
            resp.id = msg.id;
            resp.response = 1;
            return resp;
        }
    );

    StorageHandler storage(bus);

    DeleteKeyEventMessage delMsg;
    delMsg.id = 4;
    delMsg.key = "key1";

    auto futureResp = bus.send<DeleteKeyResponseMessage>(HandlerID::StorageHandler, delMsg);
    DeleteKeyResponseMessage resp = futureResp.get();

    EXPECT_EQ(resp.id, delMsg.id);
    EXPECT_EQ(resp.response, 1);
}

// ---------- TEST 5: DELETE GROUP ----------
// Testet, ob die DELETE GROUP-Operation beide Ergebnisse (z. B. Anzahl gelöschter Einträge) korrekt kombiniert.
TEST(StorageHandlerTest, DeleteGroupEvent) {
    EventBus bus;

    // Dummy RamHandler: Löscht 2 Einträge.
    bus.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(HandlerID::RamHandler,
        [](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
            DeleteGroupResponseMessage resp;
            resp.id = msg.id;
            resp.response = 2;
            return resp;
        }
    );
    // Dummy DiskHandler: Löscht 3 Einträge.
    bus.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(HandlerID::DiskHandler,
        [](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
            DeleteGroupResponseMessage resp;
            resp.id = msg.id;
            resp.response = 3;
            return resp;
        }
    );

    StorageHandler storage(bus);

    DeleteGroupEventMessage delGroupMsg;
    delGroupMsg.id = 5;
    delGroupMsg.group = "group1";

    auto futureResp = bus.send<DeleteGroupResponseMessage>(HandlerID::StorageHandler, delGroupMsg);
    DeleteGroupResponseMessage resp = futureResp.get();

    EXPECT_EQ(resp.id, delGroupMsg.id);
    EXPECT_EQ(resp.response, 2 + 3);  // Summierung der Ergebnisse beider Speicher
}

// ---------- PERFORMANCE TEST: SET-Operation  ----------
// Dieser Test misst, wie schnell der StorageHandler eine große Anzahl von SET-Events
// an die beiden Dummy‑Speicher (RAM und Disk) weiterleiten und die Ergebnisse verarbeiten kann.
TEST(StorageHandlerTest, PerformanceTestSetEvents) {
    EventBus bus;

    // Dummy RamHandler: Liefert für SET-Events immer Erfolg.
    bus.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::RamHandler,
        [](const SetEventMessage& msg) -> SetResponseMessage {
            SetResponseMessage resp;
            resp.id = msg.id;
            resp.response = true;
            return resp;
        }
    );

    // Dummy DiskHandler: Liefert ebenfalls Erfolg für SET-Events.
    bus.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::DiskHandler,
        [](const SetEventMessage& msg) -> SetResponseMessage {
            SetResponseMessage resp;
            resp.id = msg.id;
            resp.response = true;
            return resp;
        }
    );

    // StorageHandler registriert sich beim Bus und leitet SET-Events an beide Speicher weiter.
    StorageHandler storage(bus);

    const int numEvents = 10000;  // Anzahl der zu sendenden SET-Events

    // Optional: Aufwärmphase, um z.B. JIT-/Cache-Effekte zu minimieren
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    auto start = std::chrono::high_resolution_clock::now();

    // Sende numEvents SET-Events nacheinander
    for (int i = 0; i < numEvents; ++i) {
        SetEventMessage setMsg;
        setMsg.id = i;
        setMsg.key = "key" + std::to_string(i);
        setMsg.value = "value" + std::to_string(i);

        // Senden an den StorageHandler und synchrones Warten auf die Antwort
        auto futureResp = bus.send<SetResponseMessage>(HandlerID::StorageHandler, setMsg);
        SetResponseMessage resp = futureResp.get();

        // Überprüfe, ob die Antwort korrekt ist
        ASSERT_EQ(resp.id, setMsg.id);
        ASSERT_TRUE(resp.response);
    }

    auto end = std::chrono::high_resolution_clock::now();

    // Berechne die Laufzeit-Metriken
    long long totalNs = nanoDiff(start, end);
    double totalMs = totalNs / 1e6;
    double nsPerEvent = totalNs / static_cast<double>(numEvents);
    double eventsPerSec = numEvents / (totalNs / 1e9);

    // Ausgabe der Performance-Daten
    std::cout << "StorageHandler Performance Test (SET-Events):" << std::endl;
    std::cout << "  Anzahl der Events: " << numEvents << std::endl;
    std::cout << "  Gesamtdauer: " << totalNs << " ns (" << totalMs << " ms)" << std::endl;
    std::cout << "  Durchschnittliche Dauer pro Event: " << nsPerEvent << " ns" << std::endl;
    std::cout << "  Verarbeitungsgeschwindigkeit: " << eventsPerSec << " Events/sec" << std::endl;
}