# syntax=docker/dockerfile:1

# Lightweight base with bash, curl
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies: git, curl, build tools for node-gyp
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       bash ca-certificates curl git build-essential python3 \
    && rm -rf /var/lib/apt/lists/*

# Install NVM and Node 23, then Yarn
ENV NVM_DIR=/root/.nvm
RUN mkdir -p "$NVM_DIR" \
    && curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash \
    && bash -lc "source $NVM_DIR/nvm.sh && nvm install 23 && nvm alias default 23 && node -v && npm -v" \
    && bash -lc "source $NVM_DIR/nvm.sh && npm i -g yarn@latest && yarn -v"

# Ensure nvm is available for interactive shells and CMD
RUN echo '\nexport NVM_DIR="$HOME/.nvm"\n[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"\n[ -s "$NVM_DIR/bash_completion" ] && . "$NVM_DIR/bash_completion"\n' >> /root/.bashrc

WORKDIR /opt

# 1) Checkout the repo
RUN git clone https://github.com/google-deepmind/actionengine /opt/actionengine

# 2) Build and link the JS package
RUN bash -lc "source $NVM_DIR/nvm.sh && cd /opt/actionengine/js && npm install && npm run build:bundle && yarn link"

# 3) Install and link in the web app, and build it
RUN bash -lc "source $NVM_DIR/nvm.sh && cd /opt/actionengine/web && yarn install && yarn link @helenapankov/actionengine && yarn build"

WORKDIR /opt/actionengine/web

# Expose the typical web dev/serve port (adjust if your app uses a different one)
EXPOSE 3000

# 4) Start the app using yarn
CMD ["bash", "-lc", "source $NVM_DIR/nvm.sh && yarn start"]
