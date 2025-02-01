#pragma once
#include <atomic>
#include <string>
#include <chrono>

// Basisklasse Message mit automatischer CID-Erstellung
struct Message {
    virtual ~Message() = default;

    const uint16_t cid;

protected:
    // Automatisch generierte CID
    explicit Message() : cid(generateCID()) {}

private:
    static uint16_t generateCID() {
        static std::atomic<uint16_t> counter{0};  // Atomarer Zähler für eindeutige CIDs
        return ++counter;
    }
};
