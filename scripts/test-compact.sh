#!/usr/bin/env bash

testfile="$1"
if [[ -z "$testfile" ]]; then
    echo "no testfile provided"
    exit 1
fi

tmpdir="/tmp/testing-packer"

rm -rf "$tmpdir"

for i in {1..50}; do 
    go run ./internal/cmd/pack put "test$i" "$1"
done

for i in $(seq 1 2 50); do 
    go run ./internal/cmd/pack delete "test$i"
done


echo
echo "before packs:"
find "$tmpdir" -type f -name '*pack*' | sort -V
echo

go run ./internal/cmd/pack compact

echo
echo "after packs:"
find "$tmpdir" -type f -name '*pack*' | sort -V

for i in $(seq 2 2 50); do 
    printf "checking chunk: %s" "$i -- "
    go run ./internal/cmd/pack get "test$i"
done
