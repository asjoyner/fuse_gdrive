#!/bin/bash

ROOT="$1"

if [ -z "$ROOT" ]; then
  echo "usage: $0 </path/to/mounted/fs>"
  exit 1
fi

for i in `seq 1 20`; do
  ls -lRa $ROOT >/dev/null &
  ls -lRa $ROOT >/dev/null
done
