#!/usr/bin/env bash

cleanup ()
{
    exit 0
}

trap cleanup SIGINT SIGTERM

docker_script_path="/usr/local/share/tarantool"
tarantool_versions=("1.6" "1.7")
for version in "${tarantool_versions[@]}"
do
    for test in t/*.t
    do
        docker run --rm -v $PWD:$docker_script_path tarantool/tarantool:$version tarantool $docker_script_path/$test | grep 'not ok'
    done
done