# Basis-Image wählen
FROM ubuntu:22.04

# Umgebungsvariablen setzen (verhindert Interaktivität)
ENV DEBIAN_FRONTEND=noninteractive

# Systempakete aktualisieren und nötige Tools installieren
RUN apt-get update && apt-get install -y \
    build-essential \
    gdb \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

# Optional: Benutzer anlegen, um nicht als root zu arbeiten
RUN useradd -ms /bin/bash developer
USER developer
WORKDIR /home/developer

# Standardport freigeben, falls Debugger remote verwendet wird (optional)
EXPOSE 2222
