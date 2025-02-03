#ifndef CONFIG_HANDLER_H
#define CONFIG_HANDLER_H

#include <string>
#include <stdexcept>
#include <fstream>
#include <nlohmann/json.hpp>
#include <filesystem>

namespace fs = std::filesystem;

struct Config {
    int maxSizeMB;
    std::string dbFile;
    std::string socketPath;
};

class ConfigHandler {
public:
    explicit ConfigHandler(const std::string& configFile) {
        fs::path absPath = fs::absolute(configFile);

        std::ifstream ifs(absPath);
        if (!ifs.is_open()) {
            throw std::runtime_error("Could not open configuration file: " + absPath.string());
        }

        nlohmann::json j;
        ifs >> j;

        config_.maxSizeMB = j.at("ram").at("maxSizeMB").get<int>();
        config_.dbFile = fs::absolute(j.at("disk").at("dbFile").get<std::string>()).string();
        config_.socketPath = fs::absolute(j.at("socket").at("socketPath").get<std::string>()).string();
    }

    const Config& getConfig() const {
        return config_;
    }

private:
    Config config_;
};

#endif // CONFIG_HANDLER_H
