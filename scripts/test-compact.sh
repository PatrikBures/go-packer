#!/usr/bin/env bash

go build -o ./bin/pack ./internal/cmd/pack

testfile="$1"
if [[ -z "$testfile" ]]; then
    echo "no testfile provided"
    exit 1
fi

tmpdir="/tmp/testing-packer"

rm -rf "$tmpdir"

count=5000

./bin/pack put --count "$count" test "$testfile" || exit 1

# shellcheck disable=SC2046
./bin/pack delete $(seq 1 2 $count | sed 's/^/test/') || exit 1


echo
echo "before packs:"
find "$tmpdir" -type f -name '*pack*' | sort -V
echo

./bin/pack compact

echo
echo "after packs:"
find "$tmpdir" -type f -name '*pack*' | sort -V

for i in $(seq 2 2 $count); do 
    printf "checking chunk: %s" "$i -- "
    ./bin/pack get "test$i"
done
