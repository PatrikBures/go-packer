#!/usr/bin/env bash

testdir=""

cleanup() {
    echo -e "\n\nScript interrupted. Cleaning up and exiting..."
    if [[ -d "$testdir" ]]; then
        echo "removing testdir: '$testdir'"
        rm -rf "$testdir"
    fi
    exit 1
}

trap cleanup INT

find ./logs -type f -name 'packer-*' | sed 's/packer-//'
last_num=$(find ./logs -maxdepth 1 -type d -name 'packer-*' -printf '%f\n' | sed 's/packer-//' | sort --numeric-sort --reverse | head -1)

if [[ -z "$last_num" ]]; then
    last_num=0
fi

number_of_tests=$1
if [[ -z $number_of_tests ]]; then
    number_of_tests=10
    echo "no number of tests provided, using default $number_of_tests"
fi

if ! [[ "$number_of_tests" =~ ^[0-9]+$ ]]; then
    echo "number_of_tests is not a valid amount, needs to be a non-negative number"
    exit 2
fi


range_start=$(( last_num + 1 ))
range_end=$(( range_start + number_of_tests - 1 ))

echo "running $number_of_tests tests in range $range_start to $range_end"

for i in $(seq "$range_start" "$range_end"); do 
    testdir=$(mktemp -d --suffix "-packertest${i}")
    logfile="${testdir}/output.log"

    echo -n "running test ${i}... "

    go test ./internal/imp_test -v \
        -run TestLong_Bolt_Local -timeout 10h \
        -args \
        -duration 1m \
        -routines 12 \
        -compact-interval 2s \
        -temp-dir "$testdir" \
        > "$logfile"

    err=$?
    if [[ $err -eq 0 ]]; then
        rm -rf "$testdir"
        echo "SUCCESS"
        continue
    fi

    preserve_dir="logs/packer-${i}"
    mv "$testdir" "$preserve_dir"

    echo "FAILED, preserved log and dir at '${preserve_dir}'"
done
