#ifndef CONFIG_HANDLER_H
#define CONFIG_HANDLER_H

#include <string>
#include <stdexcept>
#include <fstream>
#include <nlohmann/json.hpp>

// Struktur, die die Konfiguration speichert.
struct Config {
    int maxSizeMB;         // Maximale RAM-Größe in MB
    std::string dbFile;    // Pfad zur SQLite-Datenbankdatei
    std::string socketPath; // Pfad zum Unix-Socket
};

class ConfigHandler {
public:
    // Konstruktor, der die Konfiguration aus der angegebenen Datei lädt.
    explicit ConfigHandler(const std::string& configFile) {
        std::ifstream ifs(configFile);
        if (!ifs.is_open()) {
            throw std::runtime_error("Could not open configuration file: " + configFile);
        }

        nlohmann::json j;
        try {
            ifs >> j;
        } catch (const std::exception& ex) {
            throw std::runtime_error("Error parsing JSON from config file: " + std::string(ex.what()));
        }

        // Setze die Konfiguration; hier wird vorausgesetzt, dass die Keys existieren.
        try {
            config_.maxSizeMB = j.at("ram").at("maxSizeMB").get<int>();
            config_.dbFile = j.at("disk").at("dbFile").get<std::string>();
            config_.socketPath = j.at("socket").at("socketPath").get<std::string>();
        } catch (const std::exception& ex) {
            throw std::runtime_error("Error reading config values: " + std::string(ex.what()));
        }
    }

    // Gibt die geladene Konfiguration zurück.
    const Config& getConfig() const {
        return config_;
    }

private:
    Config config_;
};

#endif // CONFIG_HANDLER_H
