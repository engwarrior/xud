#!/bin/bash
set -xe
declare -xp
# export PATH="$PWD/go/bin:$PATH"
export PATH="$PWD/go/bin:$PATH"
export GOPATH="$PWD/go"
GETH_PATH="$PWD/go/src/github.com/ethereum/go-ethereum"
git clone --verbose https://github.com/ethereum/go-ethereum "$GETH_PATH"
cd "$GETH_PATH" || exit 1
echo "go version $(go version)"
make geth
