#!/bin/bash
set -ex

API_PORT=$1
RESOLVER_PORT=$2
GETH_PORT=$3
$SCRIPTS_PATH/start-raiden.sh "$RAIDEN_DATA_DIR_BOB" "$API_PORT" "$RESOLVER_PORT" "$GETH_PORT"
