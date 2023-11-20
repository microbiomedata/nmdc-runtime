FROM gitpod/workspace-full

RUN sudo apt-get update \
 && sudo apt-get install -y \
    mongodb-org-tools-6.0 \
 && sudo rm -rf /var/lib/apt/lists/*
