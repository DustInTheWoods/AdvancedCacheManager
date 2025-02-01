#ifndef DISKHANDLER_H
#define DISKHANDLER_H

#include "eventbus/EventBus.h"
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sqlite3.h>
#include "storage/Message.h"  // Hier sollten die spezifischen Message-Klassen (SetEventMessage, etc.) definiert sein

class DiskHandler {
public:
    // Konstruktor: Öffnet die SQLite-Datenbank (bzw. erstellt sie, falls sie noch nicht existiert)
    explicit DiskHandler(EventBus& eventBus, const std::string& dbFile = "disk_store.db")
        : eventBus_(eventBus)
    {
        int rc = sqlite3_open(dbFile.c_str(), &db_);
        if (rc) {
            std::cerr << "DiskHandler: Kann Datenbank nicht öffnen: " << sqlite3_errmsg(db_) << std::endl;
            db_ = nullptr;
            throw std::runtime_error("Fehler beim Öffnen der SQLite-Datenbank.");
        }
        // Erstelle die Tabelle, falls sie noch nicht existiert
        const char* createTableSQL = "CREATE TABLE IF NOT EXISTS store ("
                                     "key TEXT PRIMARY KEY, "
                                     "value TEXT"
                                     ");";
        char* errMsg = nullptr;
        rc = sqlite3_exec(db_, createTableSQL, nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: SQL-Fehler beim Erstellen der Tabelle: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            sqlite3_close(db_);
            db_ = nullptr;
            throw std::runtime_error("Fehler beim Erstellen der Tabelle in der SQLite-Datenbank.");
        }

        // Registrierung der EventBus-Handler
        eventBus_.subscribe<SetEventMessage, SetResponseMessage>(HandlerID::DiskHandler,
            [this](const SetEventMessage& msg) -> SetResponseMessage {
                return handleSetEvent(msg);
            }
        );

        eventBus_.subscribe<GetKeyEventMessage, GetKeyResponseMessage>(HandlerID::DiskHandler,
            [this](const GetKeyEventMessage& msg) -> GetKeyResponseMessage {
                return handleGetKeyEvent(msg);
            }
        );

        eventBus_.subscribe<GetGroupEventMessage, GetGroupResponseMessage>(HandlerID::DiskHandler,
            [this](const GetGroupEventMessage& msg) -> GetGroupResponseMessage {
                return handleGetGroupEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteKeyEventMessage, DeleteKeyResponseMessage>(HandlerID::DiskHandler,
            [this](const DeleteKeyEventMessage& msg) -> DeleteKeyResponseMessage {
                return handleDeleteKeyEvent(msg);
            }
        );

        eventBus_.subscribe<DeleteGroupEventMessage, DeleteGroupResponseMessage>(HandlerID::DiskHandler,
            [this](const DeleteGroupEventMessage& msg) -> DeleteGroupResponseMessage {
                return handleDeleteGroupEvent(msg);
            }
        );
    }

    ~DiskHandler() {
        if (db_) {
            sqlite3_close(db_);
        }
    }

private:
    sqlite3* db_ = nullptr;
    std::mutex mutex_;
    EventBus& eventBus_;

    // Handler-Implementierung für SET-Events
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "DiskHandler (SQLite): Empfange SET-Event für key: " << msg.key << std::endl;
        const char* sql = "INSERT OR REPLACE INTO store (key, value) VALUES (?, ?);";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Vorbereiten des Statements: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite prepare error in SET.");
        }
        sqlite3_bind_text(stmt, 1, msg.key.c_str(), -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, msg.value.c_str(), -1, SQLITE_STATIC);

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Ausführen des Statements: " << sqlite3_errmsg(db_) << std::endl;
            sqlite3_finalize(stmt);
            throw std::runtime_error("SQLite step error in SET.");
        }
        sqlite3_finalize(stmt);

        SetResponseMessage resp;
        resp.id = msg.id;
        resp.response = true;
        return resp;
    }

    // Handler-Implementierung für GET KEY-Events
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "DiskHandler (SQLite): Empfange GET KEY-Event für key: " << msg.key << std::endl;
        const char* sql = "SELECT value FROM store WHERE key = ?;";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Vorbereiten des Statements: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite prepare error in GET KEY.");
        }
        sqlite3_bind_text(stmt, 1, msg.key.c_str(), -1, SQLITE_STATIC);
        std::string value;
        rc = sqlite3_step(stmt);
        if (rc == SQLITE_ROW) {
            const unsigned char* text = sqlite3_column_text(stmt, 0);
            value = text ? reinterpret_cast<const char*>(text) : "";
        } else {
            value = ""; // Key nicht gefunden
        }
        sqlite3_finalize(stmt);

        GetKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = value;
        return resp;
    }

    // Handler-Implementierung für GET GROUP-Events
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "DiskHandler (SQLite): Empfange GET GROUP-Event für group: " << msg.group << std::endl;
        // Annahme: Schlüssel haben das Format "group:key", daher suchen wir nach Einträgen, die mit "group:" beginnen.
        std::string pattern = msg.group + ":%";
        const char* sql = "SELECT key, value FROM store WHERE key LIKE ?;";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Vorbereiten des Statements: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite prepare error in GET GROUP.");
        }
        sqlite3_bind_text(stmt, 1, pattern.c_str(), -1, SQLITE_STATIC);

        GetGroupResponseMessage resp;
        resp.id = msg.id;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            const unsigned char* keyText = sqlite3_column_text(stmt, 0);
            const unsigned char* valueText = sqlite3_column_text(stmt, 1);
            std::string key = keyText ? reinterpret_cast<const char*>(keyText) : "";
            std::string value = valueText ? reinterpret_cast<const char*>(valueText) : "";
            resp.response.push_back({ key, value });
        }
        sqlite3_finalize(stmt);
        return resp;
    }

    // Handler-Implementierung für DELETE KEY-Events
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "DiskHandler (SQLite): Empfange DELETE KEY-Event für key: " << msg.key << std::endl;
        const char* sql = "DELETE FROM store WHERE key = ?;";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Vorbereiten des Statements: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite prepare error in DELETE KEY.");
        }
        sqlite3_bind_text(stmt, 1, msg.key.c_str(), -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Ausführen des DELETE: " << sqlite3_errmsg(db_) << std::endl;
            sqlite3_finalize(stmt);
            throw std::runtime_error("SQLite step error in DELETE KEY.");
        }
        int changes = sqlite3_changes(db_);
        sqlite3_finalize(stmt);

        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = (changes > 0) ? 1 : 0;
        return resp;
    }

    // Handler-Implementierung für DELETE GROUP-Events
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "DiskHandler (SQLite): Empfange DELETE GROUP-Event für group: " << msg.group << std::endl;
        // Annahme: Schlüssel haben das Format "group:key"
        std::string pattern = msg.group + ":%";
        const char* sql = "DELETE FROM store WHERE key LIKE ?;";
        sqlite3_stmt* stmt = nullptr;
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Vorbereiten des Statements: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite prepare error in DELETE GROUP.");
        }
        sqlite3_bind_text(stmt, 1, pattern.c_str(), -1, SQLITE_STATIC);
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Ausführen des DELETE: " << sqlite3_errmsg(db_) << std::endl;
            sqlite3_finalize(stmt);
            throw std::runtime_error("SQLite step error in DELETE GROUP.");
        }
        int changes = sqlite3_changes(db_);
        sqlite3_finalize(stmt);

        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        resp.response = changes;
        return resp;
    }
};

#endif // DISKHANDLER_H
