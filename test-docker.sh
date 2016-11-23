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
    echo RUN IN DOCKER v$version
    docker run --rm -v $PWD:$docker_script_path tarantool/tarantool:$version sh $docker_script_path/test-local.sh
done