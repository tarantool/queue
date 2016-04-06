curl http://download.tarantool.org/tarantool/1.6/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`

sudo tee /etc/apt/sources.list.d/tarantool_1_6.list <<- EOF
deb http://download.tarantool.org/tarantool/1.6/ubuntu/ $release main
deb-src http://download.tarantool.org/tarantool/1.6/ubuntu/ $release main
EOF

sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev
cmake .
make check
