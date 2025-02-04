#ifndef EVENTBUS_H
#define EVENTBUS_H

#include <functional>
#include <future>
#include <shared_mutex>   // Für std::shared_mutex, std::shared_lock
#include <typeindex>
#include <unordered_map>
#include <stdexcept>
#include <iostream>
#include <chrono>
#include <ctime>
#include <queue>
#include <thread>
#include <condition_variable>
#include <vector>
#include <memory>
#include <sstream>

#include <eventbus/Message.h>

// Logging macros with a consistent layout.
#define LOG_INFO(component, message) \
    std::cout << "[INFO] [" << component << "] " << message << std::endl;

#define LOG_ERROR(component, message) \
    std::cout << "[ERROR] [" << component << "] " << message << std::endl;

// =========================
// Simple ThreadPool class
// =========================
class ThreadPool {
public:
    explicit ThreadPool(size_t numThreads) : stop(false) {
        // Create worker threads.
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                for (;;) {
                    std::function<void()> task;

                    {
                        std::unique_lock<std::mutex> lock(queueMutex);
                        condition.wait(lock, [this] { return stop || !tasks.empty(); });
                        if (stop && tasks.empty())
                            return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    // Enqueue a new task.
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result_t<F, Args...>> {
        using return_type = typename std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            if (stop)
                throw std::runtime_error("enqueue on stopped ThreadPool");
            tasks.emplace([task]() { (*task)(); });
        }
        condition.notify_one();
        return res;
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread &worker : workers)
            worker.join();
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;
};

// =========================
// Handler ID and EventBusResult
// =========================
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
    explicit EventBusResult(std::future<std::unique_ptr<RetMsg>> future)
        : future(std::move(future)) {}

    RetMsg get() {
        auto val = future.get();
        return dynamic_cast<RetMsg&>(*val);
    }

private:
    std::future<std::unique_ptr<RetMsg>> future;
};

template <>
class EventBusResult<void> {
public:
    explicit EventBusResult(std::future<void> future)
        : future(std::move(future)) {}

    void wait() {
        future.wait();
    }

private:
    std::future<void> future;
};

// =========================
// EventBus class
// =========================
class EventBus {
public:
    // Constructor: initialize ThreadPool with a fixed number of threads (e.g., 20)
    EventBus() : threadPool(20) { }

    template <typename TMsg, typename RetMsg>
    bool subscribe(const HandlerID id, std::function<RetMsg(const TMsg&)> callback) {
        static_assert(std::is_base_of_v<Message, TMsg>, "TMsg must inherit from Message!");
        if constexpr (!std::is_void_v<RetMsg>) {
            static_assert(std::is_base_of_v<Message, RetMsg>, "RetMsg must inherit from Message!");
        }

        // Exklusiver Zugriff für Schreibzugriffe.
        std::unique_lock<std::shared_mutex> lock(handlers_.mutex);
        auto& handler = handlers_.map[id];

        // Check if already subscribed.
        const auto it = handler.find(typeid(TMsg));
        if (it != handler.end()) {
            LOG_ERROR("EventBus", "Event handler already exists for message type: " << typeid(TMsg).name());
            throw std::runtime_error("Event handler already exists");
        }

        auto fn = [callback = std::move(callback)](const Message& msg) -> std::unique_ptr<Message> {
            const TMsg* specificMsg = dynamic_cast<const TMsg*>(&msg);
            if (!specificMsg) {
                LOG_ERROR("EventBus", "Message type mismatch in subscribe for message type: " << typeid(TMsg).name());
                throw std::runtime_error("Message type mismatch in subscribe!");
            }

            if constexpr (std::is_void_v<RetMsg>) {
                callback(*specificMsg);
                return nullptr;
            } else {
                return std::make_unique<RetMsg>(std::move(callback(*specificMsg)));
            }
        };

        const std::type_index typeIdx(typeid(TMsg));
        handler[typeIdx] = std::move(fn);

        LOG_INFO("EventBus", "Subscribed handler for message type: " << typeid(TMsg).name()
                 << " on handler ID: " << static_cast<int>(id));
        return true;
    }

    template <typename RetMsg>
    EventBusResult<RetMsg> send(const HandlerID id, const Message& msg) {
        // Lesezugriffe werden mit shared_lock abgesichert.
        std::shared_lock<std::shared_mutex> lock(handlers_.mutex);

        const auto itHandler = handlers_.map.find(id);
        if (itHandler == handlers_.map.end()) {
            LOG_ERROR("EventBus", "Handler not found for ID: " << static_cast<int>(id));
            throw std::runtime_error("Handler not found!");
        }

        const std::type_index typeIdx(typeid(msg));
        const auto itFunc = itHandler->second.find(typeIdx);
        if (itFunc == itHandler->second.end()) {
            LOG_ERROR("EventBus", "Event not found for message type: " << typeIdx.name());
            throw std::runtime_error("Event not found!");
        }

        LOG_INFO("EventBus", "Sending message of type: " << typeIdx.name()
                 << " to handler ID: " << static_cast<int>(id));

        if constexpr (std::is_void_v<RetMsg>) {
            auto future = threadPool.enqueue([itFunc, &msg]() {
                itFunc->second(msg); // Call handler (result ignored)
            });
            return EventBusResult<void>(std::move(future));
        } else {
            auto future = threadPool.enqueue([itFunc, &msg]() -> std::unique_ptr<RetMsg> {
                auto result = itFunc->second(msg); // Get the result from the handler
                return std::unique_ptr<RetMsg>(dynamic_cast<RetMsg*>(result.release()));
            });
            return EventBusResult<RetMsg>(std::move(future));
        }
    }

    template <typename TMsg>
    bool unsubscribe(const HandlerID id) {
        static_assert(std::is_base_of_v<Message, TMsg>, "TMsg must inherit from Message!");

        std::unique_lock<std::shared_mutex> lock(handlers_.mutex);

        auto it = handlers_.map.find(id);
        if (it == handlers_.map.end()) {
            LOG_INFO("EventBus", "No handlers registered for handler ID: " << static_cast<int>(id));
            return false;  // No entry exists.
        }

        auto& handlersMap = it->second;
        const std::type_index typeIdx(typeid(TMsg));
        size_t oldSize = handlersMap.size();

        handlersMap.erase(typeIdx);

        // If the handler map is now empty, remove the ID from the main map.
        if (handlersMap.empty()) {
            handlers_.map.erase(it);
        }

        bool removed = (oldSize != handlersMap.size());
        if (removed) {
            LOG_INFO("EventBus", "Unsubscribed handler for message type: " << typeid(TMsg).name()
                     << " from handler ID: " << static_cast<int>(id));
        } else {
            LOG_INFO("EventBus", "Handler for message type: " << typeid(TMsg).name()
                     << " was not found under handler ID: " << static_cast<int>(id));
        }
        return removed;
    }

private:
    using HandlerFunction = std::function<std::unique_ptr<Message>(const Message&)>;

    // Verwende einen shared_mutex, um zwischen Lese- und Schreibzugriffen zu unterscheiden.
    struct Handlers {
        std::shared_mutex mutex;
        std::unordered_map<HandlerID, std::unordered_map<std::type_index, HandlerFunction>> map;
    };

    Handlers handlers_;
    ThreadPool threadPool;
};

#endif // EVENTBUS_H
