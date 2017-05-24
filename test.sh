#!/bin/bash

VERSION=${VERSION:-1_6}

curl https://packagecloud.io/tarantool/${VERSION}/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

sudo apt-get install -y apt-transport-https

sudo tee /etc/apt/sources.list.d/tarantool_${VERSION}.list <<- EOF
deb https://packagecloud.io/tarantool/${VERSION}/ubuntu/ $release main
deb-src https://packagecloud.io/tarantool/${VERSION}/ubuntu/ $release main
EOF

sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev --force-yes
cmake .
make check
