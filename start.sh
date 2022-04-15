#!/bin/bash

set -e

sleep 0.1

mkdir /tmp/chukchahc1
mkdir /tmp/chukchahc2

go install -v ./...

 ~/go/bin/chukcha -instance 'hc1' -dirname '/tmp/chukchahc1' -listen 'localhost:8061' &
 ~/go/bin/chukcha -instance 'hc2' -dirname '/tmp/chukchahc2' -listen 'localhost:8062' &

 wait
