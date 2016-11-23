#!/usr/bin/env bash

script=$(readlink -f "$0")
docker_script_path=$(dirname "$script")

cd $docker_script_path
for test in t/*.t
do
    tarantool $test
done