#ifndef STORAGEHANDLER_H
#define STORAGEHANDLER_H

#include "eventbus/EventBus.h"
#include <iostream>
#include "storage/Message.h"  // Enthält die Definitionen für SetEventMessage, SetResponseMessage, GetKeyEventMessage, etc.

/*
  StorageHandler empfängt über den EventBus Anfragen (SET, GET, DELETE)
  und leitet diese an den RamHandler und DiskHandler weiter.
  Damit wird erreicht, dass Änderungen sowohl im schnellen, flüchtigen Speicher (RAM)
  als auch persistent (auf der Disk) abgelegt werden.
*/
class StorageHandler {
public:
    explicit StorageHandler(EventBus& eventBus)
        : eventBus_(eventBus)
    {
        // Registrierung der Handler-Funktionen für Storage-Events beim EventBus
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
    }

    // ------------------------------
    // Handler-Implementierungen
    // ------------------------------

    // SET-Event: Leitet die Anfrage an RAM und Disk weiter und kombiniert die Ergebnisse.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        std::cout << "StorageHandler: Received SET event for key: " << msg.key << std::endl;
        // Anfrage an den RamHandler senden
        auto ramResult = eventBus_.send<SetResponseMessage>(HandlerID::RamHandler, msg);
        // Anfrage an den DiskHandler senden
        auto diskResult = eventBus_.send<SetResponseMessage>(HandlerID::DiskHandler, msg);

        // Auf Antworten warten
        SetResponseMessage ramResp = ramResult.get();
        SetResponseMessage diskResp = diskResult.get();

        SetResponseMessage resp;
        resp.id = msg.id;
        // Nur wenn beide Speicher die SET-Operation erfolgreich bestätigen, wird Erfolg zurückgegeben.
        resp.response = ramResp.response && diskResp.response;
        return resp;
    }

    // GET KEY-Event: Zuerst im RAM suchen, bei leerer Antwort dann den DiskHandler abfragen.
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::cout << "StorageHandler: Received GET KEY event for key: " << msg.key << std::endl;
        auto ramResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::RamHandler, msg);
        GetKeyResponseMessage ramResp = ramResult.get();
        if (!ramResp.response.empty()) {
            return ramResp;
        } else {
            // Fallback: Anfrage an DiskHandler
            auto diskResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::DiskHandler, msg);
            GetKeyResponseMessage diskResp = diskResult.get();
            // Optional: Bei Erfolg könnte man den RAM-Cache mit dem gefundenen Wert aktualisieren.
            return diskResp;
        }
    }

    // GET GROUP-Event: Auch hier zuerst RAM, ansonsten Disk.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::cout << "StorageHandler: Received GET GROUP event for group: " << msg.group << std::endl;
        auto ramResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::RamHandler, msg);
        GetGroupResponseMessage ramResp = ramResult.get();
        if (!ramResp.response.empty()) {
            return ramResp;
        } else {
            auto diskResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::DiskHandler, msg);
            GetGroupResponseMessage diskResp = diskResult.get();
            return diskResp;
        }
    }

    // DELETE KEY-Event: Leitet das Löschen an beide Speicher weiter.
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        std::cout << "StorageHandler: Received DELETE KEY event for key: " << msg.key << std::endl;
        auto ramResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteKeyResponseMessage ramResp = ramResult.get();
        DeleteKeyResponseMessage diskResp = diskResult.get();

        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        // Erfolg nur, wenn beide Speicher das Löschen bestätigen (1 = Erfolg)
        resp.response = (ramResp.response == 1 && diskResp.response == 1) ? 1 : 0;
        return resp;
    }

    // DELETE GROUP-Event: Leitet die Anfrage an beide Speicher weiter und fasst die Ergebnisse zusammen.
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        std::cout << "StorageHandler: Received DELETE GROUP event for group: " << msg.group << std::endl;
        auto ramResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteGroupResponseMessage ramResp = ramResult.get();
        DeleteGroupResponseMessage diskResp = diskResult.get();

        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        // Hier summieren wir die Anzahl der gelöschten Einträge (alternative Kombinationsstrategien sind denkbar)
        resp.response = ramResp.response + diskResp.response;
        return resp;
    }

private:
    EventBus& eventBus_;
};

#endif // STORAGEHANDLER_H
