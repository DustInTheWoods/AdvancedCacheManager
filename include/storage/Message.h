#ifndef SOCKETCONNECTION_H
#define SOCKETCONNECTION_H

#include <eventbus/Message.h>


// SET EVENT
struct SetEventMessage : public Message {
    std::string id;
    bool persistent;
    int ttl;
    std::string key;
    std::string value;
    std::string group;
};

// GET KEY EVENT
struct GetKeyEventMessage : public Message {
    std::string id;
    std::string key;
};

// GET GROUP EVENT
struct GetGroupEventMessage : public Message {
    std::string id;
    std::string group;
};

// DELETE KEY EVENT
struct DeleteKeyEventMessage : public Message {
    std::string id;
    std::string key;
};

// DELETE GROUP EVENT
struct DeleteGroupEventMessage : public Message {
    std::string id;
    std::string group;
};

struct ListEventMessage : public Message {
    std::string id;
};

// ======================
// Response–Nachrichten
// ======================

struct SetResponseMessage : public Message {
    std::string id;
    bool response;
};

struct GetKeyResponseMessage : public Message {
    std::string id;
    std::string response;
};

struct KeyValue {
    std::string key;
    std::string value;
};

struct GetGroupResponseMessage : public Message {
    std::string id;
    std::vector<KeyValue> response;
};

struct DeleteKeyResponseMessage : public Message {
    std::string id;
    int response;  // z. B. 1 oder 0
};

struct DeleteGroupResponseMessage : public Message {
    std::string id;
    int response;  // z. B. 1 oder 0
};

struct StorageEntry {
    std::string key;
    std::string value;
    std::string group;
};

struct ListEventReponseMessage : public Message {
    std::string id;
    std::vector<StorageEntry> response;
};

#endif //SOCKETCONNECTION_H
