#!/bin/sh
set -e
if [ "$1" != "migrate" ]; then
  polymarket-pipeline migrate
fi
exec polymarket-pipeline "$@"
