#include <iostream>
#include "config/ConfigHandler.h"
#include "eventbus/EventBus.h"
#include "storage/RamHandler.h"
#include "storage/DiskHandler.h"
#include "storage/StorageHandler.h"
#include "network/UnixSocket.h"

int main(int argc, char** argv) {
    // Standard-Konfigurationsdatei ist "config.json"
    std::string configFile = "config.json";
    if (argc > 1) {
        configFile = argv[1];  // Optional: Pfad zur Konfigurationsdatei über die Kommandozeile angeben
    }

    try {
        ConfigHandler configHandler(configFile);
        const Config& config = configHandler.getConfig();

        std::cout << "Konfiguration geladen:" << std::endl;
        std::cout << "  RAM max size (MB): " << config.maxSizeMB << std::endl;
        std::cout << "  Disk DB file:      " << config.dbFile << std::endl;
        std::cout << "  Socket path:       " << config.socketPath << std::endl;

        // Hier startest du den Rest deiner Anwendung.
        // Beispiel:
        EventBus eventBus;
        RamHandler ramHandler(eventBus, config.maxSizeMB);
        DiskHandler diskHandler(eventBus, config.dbFile);
        SocketHandler socketHandler(config.socketPath, eventBus);
        StorageHandler storageHandler(eventBus);
        socketHandler.run();  // Blockierende Methode, die auf Verbindungen wartet

        std::cout << "AdvancedCacheManager startet..." << std::endl;

        // Für dieses Beispiel beenden wir das Programm hier.
    } catch (const std::exception& ex) {
        std::cerr << "Fehler beim Laden der Konfiguration: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
