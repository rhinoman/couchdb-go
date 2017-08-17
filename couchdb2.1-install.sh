#!/bin/sh

echo "deb https://apache.bintray.com/couchdb-deb trusty main" \
| sudo tee -a /etc/apt/sources.list

curl -L https://couchdb.apache.org/repo/bintray-pubkey.asc \
    | sudo apt-key add -

sudo apt-get update || true
sudo apt-get --no-install-recommends -y install couchdb