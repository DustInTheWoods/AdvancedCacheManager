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


    // SET-Event: Leitet die Anfrage an RAM und Disk weiter und kombiniert die Ergebnisse.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        if (msg.persistent)
        {
            std::cout << "Forwarding it to DiskHandler" << std::endl;

            // Anfrage an den DiskHandler senden
            auto diskResult = eventBus_.send<SetResponseMessage>(HandlerID::DiskHandler, msg);

            SetResponseMessage diskResp = diskResult.get();
            diskResp.id = msg.id;
            return diskResp;
        } else
        {
            std::cout << "Forwarding it to RamHandler" << std::endl;

            // Anfrage an den RamHandler senden
            auto ramResult = eventBus_.send<SetResponseMessage>(HandlerID::RamHandler, msg);

            SetResponseMessage ramResp = ramResult.get();
            ramResp.id = msg.id;
            return ramResp;
        }
    }

    // GET KEY-Event: Zuerst im RAM suchen, bei leerer Antwort dann den DiskHandler abfragen.
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        if (msg.key.empty()){throw std::invalid_argument("Invalid key name");}

        auto ramResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::RamHandler, msg);
        GetKeyResponseMessage ramResp = ramResult.get();
        if (!ramResp.response.empty()) {
            std::cout << "Found it in RamHandler" << std::endl;
            return ramResp;
        } else {
            // Fallback: Anfrage an DiskHandler
            auto diskResult = eventBus_.send<GetKeyResponseMessage>(HandlerID::DiskHandler, msg);
            GetKeyResponseMessage diskResp = diskResult.get();
            // Optional: Bei Erfolg könnte man den RAM-Cache mit dem gefundenen Wert aktualisieren.

            if (!diskResp.response.empty()) {std::cout << "Could not find it" << std::endl;}
            std::cout << "Found it in DiskHandler" << std::endl;
            return diskResp;
        }
    }

    // GET GROUP-Event: Auch hier zuerst RAM, ansonsten Disk.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        if (msg.group.empty()){throw std::invalid_argument("Invalid group name");}

        auto ramResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<GetGroupResponseMessage>(HandlerID::DiskHandler, msg);

        GetGroupResponseMessage ramResp = ramResult.get();
        GetGroupResponseMessage diskResp = diskResult.get();

        GetGroupResponseMessage result;
        result.id = msg.id;
        diskResp.response.insert(diskResp.response.begin(), ramResp.response.begin(), ramResp.response.end());
        result.response = diskResp.response;

        return result;
    }

    // DELETE KEY-Event: Leitet das Löschen an beide Speicher weiter.
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        if (msg.key.empty()){throw std::invalid_argument("Invalid group name");}

        auto ramResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteKeyResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteKeyResponseMessage ramResp = ramResult.get();
        if (ramResp.response != 0) {std::cout << "Deleted key in DiskHandler " << msg.key << std::endl;}
        DeleteKeyResponseMessage diskResp = diskResult.get();
        if (diskResp.response != 0) {std::cout << "Deleted key in DiskHandler " << msg.key << std::endl;}

        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        // Erfolg nur, wenn beide Speicher das Löschen bestätigen (1 = Erfolg)
        resp.response = ramResp.response + diskResp.response;
        return resp;
    }

    // DELETE GROUP-Event: Leitet die Anfrage an beide Speicher weiter und fasst die Ergebnisse zusammen.
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        if (msg.group.empty()){throw std::invalid_argument("Invalid group name");}

        auto ramResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::RamHandler, msg);
        auto diskResult = eventBus_.send<DeleteGroupResponseMessage>(HandlerID::DiskHandler, msg);

        DeleteGroupResponseMessage ramResp = ramResult.get();
        if (ramResp.response != 0) {std::cout << "Deleted group in DiskHandler " << msg.group << std::endl;}
        DeleteGroupResponseMessage diskResp = diskResult.get();
        if (diskResp.response != 0) {std::cout << "Deleted group in DiskHandler " << msg.group << std::endl;}

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
