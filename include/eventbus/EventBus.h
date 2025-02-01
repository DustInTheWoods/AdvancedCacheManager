#ifndef EVENTBUS_H
#define EVENTBUS_H

#include <functional>
#include <future>
#include <mutex>
#include <typeindex>
#include <unordered_map>
#include <eventbus/Message.h>

enum class HandlerID {
    Broadcast = 0,
    EventBus = 1,
    SocketHandler = 2,
    StorageHandler = 3,
    RamHandler = 4,
    DiskHandler = 5,
};

template <typename RetMsg>
class EventBusResult {
public:
    explicit EventBusResult(std::future<std::unique_ptr<RetMsg>> future) : future(std::move(future)) {}

    RetMsg get() {
        auto val = future.get();
        return dynamic_cast<RetMsg&>(*val);
    }

private:
    std::future<std::unique_ptr<RetMsg>> future; // Korrektur: RetMsg statt Message
};

template <>
class EventBusResult<void> {
public:
    explicit EventBusResult(std::future<void> future) : future(std::move(future)) {}

    void wait() {
        future.wait();
    }

private:
    std::future<void> future;
};

class EventBus {
public:
    template <typename TMsg, typename RetMsg>
    bool subscribe(const HandlerID id, std::function<RetMsg(const TMsg&)> callback) {
        static_assert(std::is_base_of_v<Message, TMsg>, "TMsg must inherit from Message!");

        if constexpr (!std::is_void_v<RetMsg>) {
            static_assert(std::is_base_of_v<Message, RetMsg>, "RetMsg must inherit from Message!");
        }

        std::lock_guard<std::mutex> lock(handlers_.mutex);
        auto& handler = handlers_.map[id];

        // Check if it is already subscribed
        const auto it = handler.find(typeid(TMsg));
        if (it != handler.end()) {
            throw std::runtime_error("Event handler already exists");
        }

        auto fn = [callback = std::move(callback)](const Message& msg) -> std::unique_ptr<Message> {
            const TMsg* specificMsg = dynamic_cast<const TMsg*>(&msg);
            if (!specificMsg) {
                throw std::runtime_error("Message type mismatch in subscribe!");
            }

            if constexpr (std::is_void_v<RetMsg>) {
                callback(*specificMsg);
                return nullptr;
            } else {
                return std::make_unique<RetMsg>(std::move(callback(*specificMsg)));
            }
        };

        // Get type index
        const std::type_index typeIdx(typeid(TMsg));
        handler[typeIdx] = std::move(fn);

        return true;
    }

    template <typename RetMsg>
    EventBusResult<RetMsg> send(const HandlerID id, Message& msg) {
        std::lock_guard lock(handlers_.mutex);

        const auto itHandler = handlers_.map.find(id);
        if (itHandler == handlers_.map.end()) {
            throw std::runtime_error("Handler not found!");
        }

        const std::type_index typeIdx(typeid(msg));
        const auto itFunc = itHandler->second.find(typeIdx);
        if (itFunc == itHandler->second.end()) {
            throw std::runtime_error("Event not found!");
        }

        if constexpr (std::is_void_v<RetMsg>) {
            // Spezialfall für void: Kein Return-Wert, ignoriere das Ergebnis
            auto future = std::async(std::launch::async, [itFunc, &msg]() {
                itFunc->second(msg); // Handler aufrufen (Rückgabewert wird ignoriert)
            });
            return EventBusResult<void>(std::move(future)); // Future<void> zurückgeben
        } else {
            // Normales Future für nicht-void Typen
            auto future = std::async(std::launch::async, [itFunc, &msg]() -> std::unique_ptr<RetMsg> {
                auto result = itFunc->second(msg); // Ergebnis des Handlers holen
                return std::unique_ptr<RetMsg>(dynamic_cast<RetMsg*>(result.release())); // Casten
            });
            return EventBusResult<RetMsg>(std::move(future));
        }
    }

    template <typename TMsg>
    bool unsubscribe(const HandlerID id) {
        static_assert(std::is_base_of_v<Message, TMsg>, "TMsg must inherit from Message!");

        std::lock_guard<std::mutex> lock(handlers_.mutex);

        auto it = handlers_.map.find(id);
        if (it == handlers_.map.end()) {
            return false;  // Kein Eintrag vorhanden
        }

        auto& handlersMap = it->second;
        const std::type_index typeIdx(typeid(TMsg));
        size_t oldSize = handlersMap.size();

        handlersMap.erase(typeIdx);

        // Falls der Eintrag nun leer ist, entfernen wir die ID aus der Map
        if (handlersMap.empty()) {
            handlers_.map.erase(it);
        }

        return oldSize != handlersMap.size();
    }

private:
    using HandlerFunction = std::function<std::unique_ptr<Message>(const Message&)>;

    struct Handlers {
        std::mutex mutex;
        std::unordered_map<HandlerID, std::unordered_map<std::type_index, HandlerFunction>> map;
    };

    Handlers handlers_;
};

#endif // EVENTBUS_H