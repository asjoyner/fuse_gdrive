#!/bin/bash

ROOT="$1"

if [ -z "$ROOT" ]; then
  echo "usage: $0 </path/to/mounted/fs>"
  exit 1
fi

echo "========================================"
echo "List directories: "
/usr/bin/time ./list-dirs.sh $ROOT
echo "========================================"
echo "Create and Remove directories: "
/usr/bin/time ./mkdirs.sh $ROOT
echo "========================================"
