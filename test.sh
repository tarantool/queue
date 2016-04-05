curl http://tarantool.org/dist/public.key | sudo apt-key add -
echo "deb http://tarantool.org/dist/1.6/ubuntu/ `lsb_release -c -s` main" | sudo tee -a /etc/apt/sources.list.d/tarantool.list
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev
cmake .
make check
