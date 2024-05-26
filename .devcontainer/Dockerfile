FROM python:3.9.16

USER root

# Install base packages for Duckdb
RUN dpkg --add-architecture amd64
RUN apt update
RUN apt-get install -y wget binutils:amd64 libwebkit2gtk-4.0-37:amd64

# install duckdb command-line tool
RUN wget https://github.com/duckdb/duckdb/releases/download/v0.10.3/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin \
    && chmod +x /usr/local/bin/duckdb \
    && rm duckdb_cli-linux-amd64.zip \
    && rm -rf /var/lib/apt/lists/*

# create non-root user and set the default user
ARG USERNAME=devuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    # Add sudo support
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && rm -rf /var/lib/apt/lists/*
USER $USERNAME

# make all python tools installed by pip accesible
ENV PATH=$PATH:/home/$USERNAME/.local/bin
RUN pip install pip --upgrade

# install python depencencies
COPY .devcontainer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
