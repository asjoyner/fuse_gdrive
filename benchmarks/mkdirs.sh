
#!/bin/bash

ROOT="$1"

if [ -z "$ROOT" ]; then
  echo "usage: $0 </path/to/mounted/fs>"
  exit 1
fi

BASEDIR=$ROOT/benchmarkdir
DIR=$BASEDIR
for i in `seq 1 20`; do
  DIR=$DIR/benchmark.dir$i
done
mkdir -p $DIR
rm -rf $BASEDIR
