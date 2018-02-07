#!/bin/sh

set -ex

sudo apt-get --no-install-recommends -y install \
    build-essential pkg-config erlang \
    libicu-dev libmozjs185-dev libcurl4-openssl-dev

mkdir temp
cd temp

wget http://www.trieuvan.com/apache/couchdb/source/2.1.1/apache-couchdb-2.1.1.tar.gz

tar -xzf apache-couchdb-2.1.1.tar.gz
cd apache-couchdb-2.1.1
./configure
make release
nohup ./rel/couchdb/bin/couchdb > /dev/null &

#Give couch a chance to start
sleep 15

curl -X PUT http://127.0.0.1:5984/_users

curl -X PUT http://127.0.0.1:5984/_replicator

cd ..
