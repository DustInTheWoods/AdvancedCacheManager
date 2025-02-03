#ifndef DISKHANDLER_H
#define DISKHANDLER_H

#include "eventbus/EventBus.h"
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sqlite3.h>
#include "storage/Message.h"  // Hier sollten die spezifischen Message-Klassen (SetEventMessage, etc.) definiert sein

// RAII-Hilfsklasse für SQLite-Statements
class SQLiteStmt {
public:
    SQLiteStmt(sqlite3* db, const char* sql)
        : db_(db), stmt_(nullptr)
    {
        int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt_, nullptr);
        if (rc != SQLITE_OK) {
            throw std::runtime_error(std::string("SQLite prepare error: ") + sqlite3_errmsg(db_));
        }
    }

    ~SQLiteStmt() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() const { return stmt_; }

    // Optionale Methode zum Zurücksetzen des Statements
    void reset() {
        if (stmt_) {
            sqlite3_reset(stmt_);
        }
    }

private:
    sqlite3* db_;
    sqlite3_stmt* stmt_;
};

class DiskHandler {
public:
    // Konstruktor: Öffnet die SQLite-Datenbank (bzw. erstellt sie, falls sie noch nicht existiert)
    explicit DiskHandler(EventBus& eventBus, const std::string& dbFile = "disk_store.db")
        : eventBus_(eventBus)
    {
        int rc = sqlite3_open(dbFile.c_str(), &db_);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Kann Datenbank nicht öffnen: " << sqlite3_errmsg(db_) << std::endl;
            db_ = nullptr;
            throw std::runtime_error("Fehler beim Öffnen der SQLite-Datenbank.");
        }
        // Erstelle die Tabelle, falls sie noch nicht existiert
        const char* createTableSQL = "CREATE TABLE IF NOT EXISTS store ("
                                     "key TEXT PRIMARY KEY, "
                                     "value TEXT, "
                                     "group_name TEXT"
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
        // Beispiel für Transaktions-Management: In Batch-Situationen können mehrere SETs
        // innerhalb einer Transaktion zusammengefasst werden.
        char* errMsg = nullptr;
        int rc = sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "DiskHandler: Fehler beim Starten der Transaktion: " << errMsg << std::endl;
            sqlite3_free(errMsg);
            throw std::runtime_error("SQLite transaction BEGIN error in SET.");
        }

        try {
            SQLiteStmt stmt(db_, "INSERT OR REPLACE INTO store (key, value, group_name) VALUES (?, ?, ?);");
            sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt.get(), 2, msg.value.c_str(), -1, SQLITE_TRANSIENT);;
            sqlite3_bind_text(stmt.get(), 3, msg.group.c_str(), -1, SQLITE_TRANSIENT);

            rc = sqlite3_step(stmt.get());
            if (rc != SQLITE_DONE) {
                std::cerr << "DiskHandler: Fehler beim Ausführen des Statements: " << sqlite3_errmsg(db_) << std::endl;
                throw std::runtime_error("SQLite step error in SET.");
            }
            // Commit der Transaktion
            rc = sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                std::cerr << "DiskHandler: Fehler beim Commit der Transaktion: " << errMsg << std::endl;
                sqlite3_free(errMsg);
                throw std::runtime_error("SQLite transaction COMMIT error in SET.");
            }
        }
        catch (const std::exception& e) {
            // Bei einem Fehler wird die Transaktion zurückgerollt
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            throw; // Exception weiterwerfen
        }

        SetResponseMessage resp;
        resp.id = msg.id;
        resp.response = true;
        return resp;
    }

    // Handler-Implementierung für GET KEY-Events
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        SQLiteStmt stmt(db_, "SELECT value FROM store WHERE key = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
        std::string value;
        int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_ROW) {
            const unsigned char* text = sqlite3_column_text(stmt.get(), 0);
            value = text ? reinterpret_cast<const char*>(text) : "";
        } else if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Abrufen des Wertes: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite step error in GET KEY.");
        }
        GetKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = value;
        return resp;
    }

    // Handler-Implementierung für GET GROUP-Events
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        // Annahme: Schlüssel haben das Format "group:key"
        SQLiteStmt stmt(db_, "SELECT key, value FROM store WHERE group_name = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.group.c_str(), -1, SQLITE_TRANSIENT);

        GetGroupResponseMessage resp;
        resp.id = msg.id;
        while (true) {
            int rc = sqlite3_step(stmt.get());
            if (rc == SQLITE_ROW) {
                const unsigned char* keyText = sqlite3_column_text(stmt.get(), 0);
                const unsigned char* valueText = sqlite3_column_text(stmt.get(), 1);
                std::string key = keyText ? reinterpret_cast<const char*>(keyText) : "";
                std::string value = valueText ? reinterpret_cast<const char*>(valueText) : "";
                resp.response.push_back({ key, value });
            } else if (rc == SQLITE_DONE) {
                break;
            } else {
                std::cerr << "DiskHandler: Fehler beim Abrufen der Gruppe: " << sqlite3_errmsg(db_) << std::endl;
                throw std::runtime_error("SQLite step error in GET GROUP.");
            }
        }
        return resp;
    }

    // Handler-Implementierung für DELETE KEY-Events
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        SQLiteStmt stmt(db_, "DELETE FROM store WHERE key = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Ausführen des DELETE: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite step error in DELETE KEY.");
        }
        int changes = sqlite3_changes(db_);
        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = (changes > 0) ? 1 : 0;
        return resp;
    }

    // Handler-Implementierung für DELETE GROUP-Events
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        // Annahme: Schlüssel haben das Format "group:key"
        SQLiteStmt stmt(db_, "DELETE FROM store WHERE group_name = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.group.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            std::cerr << "DiskHandler: Fehler beim Ausführen des DELETE GROUP: " << sqlite3_errmsg(db_) << std::endl;
            throw std::runtime_error("SQLite step error in DELETE GROUP.");
        }
        int changes = sqlite3_changes(db_);
        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        resp.response = changes;
        return resp;
    }
};

#endif // DISKHANDLER_H
