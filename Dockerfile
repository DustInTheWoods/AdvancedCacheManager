# Basis-Image wählen
FROM ubuntu:22.04
LABEL maintainer="devcontainer"
LABEL description="Mein C++ Entwicklungsimage"

# Umgebungsvariablen setzen (verhindert Interaktivität)
ENV DEBIAN_FRONTEND=noninteractive

# Systempakete aktualisieren und nötige Tools installieren
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    gdb \
    ninja-build \
    libsqlite3-dev \
    libgtest-dev \
    nlohmann-json3-dev \
    libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*

# Optional: Benutzer anlegen, um nicht als root zu arbeiten
RUN useradd -ms /bin/bash developer

# Arbeitsverzeichnis setzen (statt /tmp, um Schreibprobleme zu vermeiden)
WORKDIR /home/developer/project

# Berechtigungen setzen (wichtig für CLion & CMake)
RUN chown -R developer:developer /home/developer

# Benutzer wechseln, damit wir nicht als root arbeiten
USER root

# Standardport für Debugging (optional)
EXPOSE 2222
