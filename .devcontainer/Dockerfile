FROM ubuntu:20.04

ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN apt-get update \
    && apt-get install -y sudo git \
    && addgroup --gid "${USER_GID}" impdev \
    && adduser --uid "${USER_UID}" --gid "${USER_GID}" --disabled-password --gecos '' impdev \
    && echo 'impdev ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers