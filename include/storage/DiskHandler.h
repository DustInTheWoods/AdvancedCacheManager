#ifndef DISKHANDLER_H
#define DISKHANDLER_H

#include "eventbus/EventBus.h"
#include "storage/Message.h"  // The specific Message classes (SetEventMessage, etc.) should be defined here.
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sqlite3.h>

// ------------------------------
// Logging Helpers and Macros
// ------------------------------

// Logging macros with a consistent layout.
#define LOG_INFO(component, message) \
std::cout <<"[INFO]" << " [" << component << "] " << message << std::endl;

#define LOG_ERROR(component, message) \
std::cout <<"[ERROR]" << " [" << component << "] " << message << std::endl;

// ------------------------------
// RAII Helper Class for SQLite Statements
// ------------------------------
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

    // Optional method to reset the statement.
    void reset() {
        if (stmt_) {
            sqlite3_reset(stmt_);
        }
    }

private:
    sqlite3* db_;
    sqlite3_stmt* stmt_;
};

// ------------------------------
// DiskHandler Class
// ------------------------------
class DiskHandler {
public:
    // Constructor: Opens (or creates, if it does not exist) the SQLite database.
    explicit DiskHandler(EventBus& eventBus, const std::string& dbFile = "disk_store.db")
        : eventBus_(eventBus)
    {
        int rc = sqlite3_open(dbFile.c_str(), &db_);
        if (rc != SQLITE_OK) {
            LOG_ERROR("DiskHandler", "Unable to open database: " << sqlite3_errmsg(db_));
            db_ = nullptr;
            throw std::runtime_error("Error opening SQLite database.");
        }

        // Create the table if it does not exist.
        const char* createTableSQL = "CREATE TABLE IF NOT EXISTS store ("
                                     "key TEXT PRIMARY KEY, "
                                     "value TEXT, "
                                     "group_name TEXT"
                                     ");";
        char* errMsg = nullptr;
        rc = sqlite3_exec(db_, createTableSQL, nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            LOG_ERROR("DiskHandler", "SQL error while creating table: " << errMsg);
            sqlite3_free(errMsg);
            sqlite3_close(db_);
            db_ = nullptr;
            throw std::runtime_error("Error creating table in SQLite database.");
        }

        // Register EventBus handlers.
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

        LOG_INFO("DiskHandler", "Initialized and database '" << dbFile << "' opened successfully.");
    }

    ~DiskHandler() {
        if (db_) {
            sqlite3_close(db_);
            LOG_INFO("DiskHandler", "Database connection closed.");
        }
    }

private:
    sqlite3* db_ = nullptr;
    std::mutex mutex_;
    EventBus& eventBus_;

    // Handler implementation for SET events.
    SetResponseMessage handleSetEvent(const SetEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        char* errMsg = nullptr;
        int rc = sqlite3_exec(db_, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            LOG_ERROR("DiskHandler", "Error starting transaction: " << errMsg);
            sqlite3_free(errMsg);
            throw std::runtime_error("SQLite transaction BEGIN error in SET.");
        }

        try {
            SQLiteStmt stmt(db_, "INSERT OR REPLACE INTO store (key, value, group_name) VALUES (?, ?, ?);");
            sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt.get(), 2, msg.value.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt.get(), 3, msg.group.c_str(), -1, SQLITE_TRANSIENT);

            rc = sqlite3_step(stmt.get());
            if (rc != SQLITE_DONE) {
                LOG_ERROR("DiskHandler", "Error executing statement: " << sqlite3_errmsg(db_));
                throw std::runtime_error("SQLite step error in SET.");
            }
            // Commit the transaction.
            rc = sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &errMsg);
            if (rc != SQLITE_OK) {
                LOG_ERROR("DiskHandler", "Error committing transaction: " << errMsg);
                sqlite3_free(errMsg);
                throw std::runtime_error("SQLite transaction COMMIT error in SET.");
            }
        }
        catch (const std::exception& e) {
            // Roll back the transaction upon error.
            sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
            LOG_ERROR("DiskHandler", "Transaction rolled back due to error: " << e.what());
            throw; // Rethrow the exception.
        }

        SetResponseMessage resp;
        resp.id = msg.id;
        resp.response = true;
        LOG_INFO("DiskHandler", "SET event successful for key: " << msg.key);
        return resp;
    }

    // Handler implementation for GET KEY events.
    GetKeyResponseMessage handleGetKeyEvent(const GetKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        SQLiteStmt stmt(db_, "SELECT value FROM store WHERE key = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
        std::string value;
        int rc = sqlite3_step(stmt.get());
        if (rc == SQLITE_ROW) {
            const unsigned char* text = sqlite3_column_text(stmt.get(), 0);
            value = text ? reinterpret_cast<const char*>(text) : "";
            LOG_INFO("DiskHandler", "GET KEY event: Key '" << msg.key << "' found.");
        } else if (rc == SQLITE_DONE) {
            LOG_INFO("DiskHandler", "GET KEY event: Key '" << msg.key << "' not found.");
        } else {
            LOG_ERROR("DiskHandler", "Error retrieving value: " << sqlite3_errmsg(db_));
            throw std::runtime_error("SQLite step error in GET KEY.");
        }
        GetKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = value;
        return resp;
    }

    // Handler implementation for GET GROUP events.
    GetGroupResponseMessage handleGetGroupEvent(const GetGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
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
                LOG_ERROR("DiskHandler", "Error retrieving group: " << sqlite3_errmsg(db_));
                throw std::runtime_error("SQLite step error in GET GROUP.");
            }
        }
        LOG_INFO("DiskHandler", "GET GROUP event: Returned " << resp.response.size() << " entries for group '" << msg.group << "'.");
        return resp;
    }

    // Handler implementation for DELETE KEY events.
    DeleteKeyResponseMessage handleDeleteKeyEvent(const DeleteKeyEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        SQLiteStmt stmt(db_, "DELETE FROM store WHERE key = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.key.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            LOG_ERROR("DiskHandler", "Error executing DELETE: " << sqlite3_errmsg(db_));
            throw std::runtime_error("SQLite step error in DELETE KEY.");
        }
        int changes = sqlite3_changes(db_);
        DeleteKeyResponseMessage resp;
        resp.id = msg.id;
        resp.response = (changes > 0) ? 1 : 0;
        LOG_INFO("DiskHandler", "DELETE KEY event: Key '" << msg.key << "' deletion " << ((changes > 0) ? "succeeded." : "failed."));
        return resp;
    }

    // Handler implementation for DELETE GROUP events.
    DeleteGroupResponseMessage handleDeleteGroupEvent(const DeleteGroupEventMessage& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        SQLiteStmt stmt(db_, "DELETE FROM store WHERE group_name = ?;");
        sqlite3_bind_text(stmt.get(), 1, msg.group.c_str(), -1, SQLITE_TRANSIENT);
        int rc = sqlite3_step(stmt.get());
        if (rc != SQLITE_DONE) {
            LOG_ERROR("DiskHandler", "Error executing DELETE GROUP: " << sqlite3_errmsg(db_));
            throw std::runtime_error("SQLite step error in DELETE GROUP.");
        }
        int changes = sqlite3_changes(db_);
        DeleteGroupResponseMessage resp;
        resp.id = msg.id;
        resp.response = changes;
        LOG_INFO("DiskHandler", "DELETE GROUP event: Removed " << changes << " entries for group '" << msg.group << "'.");
        return resp;
    }
};

#endif // DISKHANDLER_H
