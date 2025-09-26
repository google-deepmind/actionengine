#!/usr/bin/env bash

set -e

repo_root=$(dirname "$(realpath "$0")")/..
cd "$repo_root" || exit 1

cd js
npm install
npm run build:bundle
npm link

cd ../web
yarn install
yarn link actionengine
yarn dev
