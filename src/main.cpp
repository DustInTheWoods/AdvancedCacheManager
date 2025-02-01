#include "eventbus/EventBus.h"
#include <iostream>

// Beispiel-Anfragetyp
struct AddRequest : Message {
    int x, y;

    AddRequest(int xx, int yy) : x(xx), y(yy) {}
};

// Beispiel-Antworttyp
struct AddResponse : Message {
    int sum;

    // Standard oder individuell
    AddResponse(int s) : sum(s) {}
};

// Beispiel-Event (ohne Antwort)
struct LogEvent : Message {
    std::string message;

    // Konstruktor, der einen std::string oder C-String annimmt
    LogEvent(const std::string& msg) : message(msg) {}
};

int main() {
    EventBus bus;


    return 0;
}
