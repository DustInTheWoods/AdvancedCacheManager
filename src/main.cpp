#include <iostream>
#include <filesystem>
#include "config/ConfigHandler.h"
#include "eventbus/EventBus.h"
#include "storage/RamHandler.h"
#include "storage/DiskHandler.h"
#include "storage/StorageHandler.h"
#include "network/UnixSocket.h"

namespace fs = std::filesystem;

int main(int argc, char** argv) {
    // Standard-Konfigurationsdatei ist "config.json"
    fs::path configFile = fs::absolute("etc/AdvancedCacheManager/config.json");

    // Falls ein alternativer Pfad über die Kommandozeile übergeben wurde, nutze diesen
    if (argc > 1) {
        configFile = fs::absolute(argv[1]);
    }

    try {
        std::cout << "Lade Konfiguration von: " << configFile << std::endl;
        ConfigHandler configHandler(configFile.string());
        const Config& config = configHandler.getConfig();

        std::cout << "Konfiguration geladen:" << std::endl;
        std::cout << "  RAM max size (MB): " << config.maxSizeMB << std::endl;
        std::cout << "  Disk DB file:      " << config.dbFile << std::endl;
        std::cout << "  Socket path:       " << config.socketPath << std::endl;

        // Hier startet die Anwendung
        EventBus eventBus;
        RamHandler ramHandler(eventBus, config.maxSizeMB);
        DiskHandler diskHandler(eventBus, config.dbFile);
        SocketHandler socketHandler(config.socketPath, eventBus);
        StorageHandler storageHandler(eventBus);
        socketHandler.run();  // Blockierende Methode, die auf Verbindungen wartet

        std::cout << "AdvancedCacheManager startet..." << std::endl;

    } catch (const std::exception& ex) {
        std::cerr << "Fehler beim Laden der Konfiguration: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
